package edudfs.tools;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import edudfs.client.DFSClient;
import edudfs.client.DFSInputStream;
import edudfs.client.DFSOutputStream;
import edudfs.common.HdfsConfig;
import edudfs.common.Log;
import edudfs.protocol.DatanodeInfo;
import edudfs.protocol.HdfsFileStatus;
import edudfs.protocol.LocatedBlock;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Corresponds (in excerpts) to org.apache.hadoop.fs.http.server.HttpFSServer: a REST gateway that speaks
 * the WebHDFS API ({@code /webhdfs/v1/<path>?op=...}) and forwards every call to a {@link DFSClient}.
 *
 * Why it is here: programs that do not speak the EduDFS protocol (Python with pyarrow, curl, a browser)
 * can read and write files - and above all read <b>byte ranges</b> ({@code op=OPEN&offset=..&length=..}).
 * That is exactly what a Parquet reader does: footer first, then only the column chunks it needs.
 * Every OPEN is logged with the HDFS blocks it touches, so you can watch projection and predicate
 * pushdown from the file system's point of view.
 *
 * Supported operations (a subset of WebHDFS):
 * <pre>
 *   GET    op=OPEN [offset, length]           file contents (a byte range)
 *   GET    op=GETFILESTATUS                   {"FileStatus": {...}}
 *   GET    op=LISTSTATUS                      {"FileStatuses": {"FileStatus": [...]}}
 *   GET    op=GETFILEBLOCKLOCATIONS [offset, length]   blocks with offset, length, block name, hosts
 *   PUT    op=CREATE [overwrite, blocksize, replication]   request body = file contents
 *   PUT    op=MKDIRS                          {"boolean": true}
 *   DELETE op=DELETE [recursive]              {"boolean": true|false}
 * </pre>
 * Simplification: the original CREATE is two steps (a 307 redirect to a DataNode, then the data). HttpFS
 * accepts the data directly with {@code data=true}; here the data is always taken directly.
 * Port 14000 as in the original (httpfs.http.port), bound to localhost only.
 */
public class HttpFSServer {

    public static final int DEFAULT_PORT = 14000;
    private static final String PREFIX = "/webhdfs/v1";

    private final DFSClient client;
    private final HttpServer server;
    private final ExecutorService executor = Executors.newFixedThreadPool(4);

    public HttpFSServer(DFSClient client, int port) throws IOException {
        this.client = client;
        this.server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
        server.createContext(PREFIX, this::handle);
        server.setExecutor(executor);
    }

    public void start() {
        server.start();
        Log.info("HttpFS", "listening on http://localhost:" + getPort() + PREFIX + "/");
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    public void stop() {
        server.stop(0);
        executor.shutdownNow();
    }

    // ------------------------------------------------------------ Dispatch
    private void handle(HttpExchange ex) throws IOException {
        try (ex) {
            String path = ex.getRequestURI().getPath().substring(PREFIX.length());
            if (path.isEmpty()) {
                path = "/";
            }
            Map<String, String> q = query(ex.getRequestURI().getRawQuery());
            String op = q.getOrDefault("op", "").toUpperCase();
            String method = ex.getRequestMethod();
            try {
                switch (method + " " + op) {
                    case "GET OPEN" -> open(ex, path, q);
                    case "GET GETFILESTATUS" -> json(ex, 200, "{\"FileStatus\":" + status(fileInfo(path), true) + "}");
                    case "GET LISTSTATUS" -> listStatus(ex, path);
                    case "GET GETFILEBLOCKLOCATIONS" -> blockLocations(ex, path, q);
                    case "PUT CREATE" -> create(ex, path, q);
                    case "PUT MKDIRS" -> json(ex, 200, "{\"boolean\":" + client.mkdirs(path) + "}");
                    case "DELETE DELETE" -> json(ex, 200, "{\"boolean\":"
                            + client.delete(path, Boolean.parseBoolean(q.get("recursive"))) + "}");
                    default -> error(ex, 400, "IllegalArgumentException", "Unsupported: " + method + " op=" + op);
                }
            } catch (FileNotFoundException e) {
                error(ex, 404, "FileNotFoundException", e.getMessage());
            } catch (IOException | RuntimeException e) {
                Log.warn("HttpFS", method + " op=" + op + " " + path + ": " + e);
                error(ex, 500, e.getClass().getSimpleName(), String.valueOf(e.getMessage()));
            }
        }
    }

    private HdfsFileStatus fileInfo(String path) throws IOException {
        HdfsFileStatus st = client.getFileInfo(path);
        if (st == null) {
            throw new FileNotFoundException("File does not exist: " + path);
        }
        return st;
    }

    // ---------------------------------------------------------- Operations
    /** op=OPEN: reads [offset, offset+length) with a positional read and logs the blocks touched. */
    private void open(HttpExchange ex, String path, Map<String, String> q) throws IOException {
        try (DFSInputStream in = client.open(path)) {
            long fileLen = in.getFileLength();
            long offset = Long.parseLong(q.getOrDefault("offset", "0"));
            long length = q.containsKey("length") ? Long.parseLong(q.get("length")) : fileLen - offset;
            length = Math.max(0, Math.min(length, fileLen - offset));
            Log.info("HttpFS", String.format(Locale.ROOT, "OPEN %s bytes [%,d .. %,d) = %,d B  <- %s", path, offset,
                    offset + length, length, blocksTouched(path, offset, length)));
            ex.getResponseHeaders().set("Content-Type", "application/octet-stream");
            ex.sendResponseHeaders(200, length == 0 ? -1 : length);
            OutputStream out = ex.getResponseBody();
            byte[] buf = new byte[64 * 1024];
            long pos = offset;
            while (pos < offset + length) {
                int n = (int) Math.min(buf.length, offset + length - pos);
                in.readFully(pos, buf, 0, n);
                out.write(buf, 0, n);
                pos += n;
            }
        }
    }

    /** "blk_1073741825 (12,345 B), blk_1073741826 (1,048,576 B)" - which blocks a byte range lies in. */
    private String blocksTouched(String path, long offset, long length) throws IOException {
        List<String> res = new ArrayList<>();
        for (LocatedBlock lb : client.getNamenode().getBlockLocations(path, offset, length).getLocatedBlocks()) {
            long start = Math.max(offset, lb.getStartOffset());
            long end = Math.min(offset + length, lb.getStartOffset() + lb.getBlock().getNumBytes());
            if (end > start) {
                res.add(String.format(Locale.ROOT, "%s (%,d B)", lb.getBlock().getBlockName(), end - start));
            }
        }
        return res.isEmpty() ? "no block" : String.join(", ", res);
    }

    private void listStatus(HttpExchange ex, String path) throws IOException {
        HdfsFileStatus st = fileInfo(path);
        HdfsFileStatus[] list = st.isDir() ? client.listPaths(path) : new HdfsFileStatus[]{st};
        List<String> items = new ArrayList<>();
        for (HdfsFileStatus s : list) {
            items.add(status(s, !st.isDir()));
        }
        json(ex, 200, "{\"FileStatuses\":{\"FileStatus\":[" + String.join(",", items) + "]}}");
    }

    private void blockLocations(HttpExchange ex, String path, Map<String, String> q) throws IOException {
        long offset = Long.parseLong(q.getOrDefault("offset", "0"));
        long length = Long.parseLong(q.getOrDefault("length", String.valueOf(Long.MAX_VALUE)));
        List<String> items = new ArrayList<>();
        for (LocatedBlock lb : client.getNamenode().getBlockLocations(path, offset, length).getLocatedBlocks()) {
            List<String> hosts = new ArrayList<>();
            for (DatanodeInfo dn : lb.getLocations()) {
                hosts.add("\"" + dn.getXferAddr() + "\"");
            }
            items.add("{\"offset\":" + lb.getStartOffset() + ",\"length\":" + lb.getBlock().getNumBytes()
                    + ",\"block\":\"" + lb.getBlock().getBlockName() + "\",\"names\":[" + String.join(",", hosts) + "]}");
        }
        json(ex, 200, "{\"BlockLocations\":{\"BlockLocation\":[" + String.join(",", items) + "]}}");
    }

    private void create(HttpExchange ex, String path, Map<String, String> q) throws IOException {
        boolean overwrite = Boolean.parseBoolean(q.getOrDefault("overwrite", "false"));
        short repl = q.containsKey("replication") ? Short.parseShort(q.get("replication"))
                : HdfsConfig.DEFAULT_REPLICATION;
        long bs = q.containsKey("blocksize") ? Long.parseLong(q.get("blocksize")) : HdfsConfig.DEFAULT_BLOCK_SIZE;
        long n;
        try (InputStream in = ex.getRequestBody(); DFSOutputStream out = client.create(path, overwrite, repl, bs)) {
            n = in.transferTo(out);
        }
        Log.info("HttpFS", String.format(Locale.ROOT, "CREATE %s (%,d B)", path, n));
        ex.getResponseHeaders().set("Location", "hdfs://" + HdfsConfig.NAMENODE_HOST + ":"
                + HdfsConfig.NAMENODE_RPC_PORT + path);
        json(ex, 201, "");
    }

    // -------------------------------------------------------------- Helpers
    /** A FileStatus object as WebHDFS writes it; pathSuffix is empty for GETFILESTATUS (the path itself). */
    private static String status(HdfsFileStatus s, boolean self) {
        String name = s.getPath().substring(s.getPath().lastIndexOf('/') + 1);
        return "{\"pathSuffix\":\"" + (self ? "" : esc(name)) + "\",\"type\":\"" + (s.isDir() ? "DIRECTORY" : "FILE")
                + "\",\"length\":" + s.getLen() + ",\"blockSize\":" + s.getBlockSize() + ",\"replication\":"
                + s.getReplication() + ",\"modificationTime\":" + s.getModificationTime() + ",\"fileId\":"
                + s.getFileId() + "}";
    }

    private static void json(HttpExchange ex, int code, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(code, b.length == 0 ? -1 : b.length);
        ex.getResponseBody().write(b);
    }

    private static void error(HttpExchange ex, int code, String exception, String msg) throws IOException {
        json(ex, code, "{\"RemoteException\":{\"exception\":\"" + exception + "\",\"message\":\"" + esc(msg) + "\"}}");
    }

    private static String esc(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static Map<String, String> query(String raw) {
        Map<String, String> q = new HashMap<>();
        if (raw != null) {
            for (String kv : raw.split("&")) {
                int i = kv.indexOf('=');
                String k = URLDecoder.decode(i < 0 ? kv : kv.substring(0, i), StandardCharsets.UTF_8);
                q.put(k.toLowerCase(), i < 0 ? "" : URLDecoder.decode(kv.substring(i + 1), StandardCharsets.UTF_8));
            }
        }
        return q;
    }

    /** ./hdfs httpfs [port] - runs until Ctrl+C. EduDFS (./hdfs minicluster) must be running. */
    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : Integer.getInteger("httpfs.http.port", DEFAULT_PORT);
        DFSClient client = new DFSClient();
        HttpFSServer server = new HttpFSServer(client, port);
        server.start();
        System.out.println("\nHttpFS (WebHDFS REST API) for EduDFS: http://localhost:" + port + PREFIX + "/"
                + "\nTry: curl 'http://localhost:" + port + PREFIX + "/?op=LISTSTATUS'"
                + "\nEvery OPEN is logged with the HDFS blocks it reads. Stop with Ctrl+C\n");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop();
            try {
                client.close();
            } catch (IOException ignored) {
                // shutting down anyway
            }
        }));
        Thread.currentThread().join();
    }
}
