package edudfsbase.fs;

import edudfsbase.io.PositionedReadable;
import edudfs.client.DFSClient;
import edudfs.client.DFSInputStream;
import edudfs.client.DFSOutputStream;
import edudfs.common.HdfsConfig;
import edudfs.common.Log;
import edudfs.protocol.HdfsFileStatus;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;

/**
 * Corresponds to org.apache.hadoop.hbase.fs.HFileSystem: the HDFS client of a master or
 * RegionServer. Everything HBase stores lies in HDFS - HBase itself keeps nothing on the local disk.
 *
 * Every process has its OWN DFSClient and therefore its own lease holder name. If a RegionServer
 * crashes, its WAL stays "under construction" in HDFS until someone else takes over the lease
 * ({@link #recoverFileLease}).
 */
public class HFileSystem implements Closeable {

    private final DFSClient dfs;

    public HFileSystem() throws IOException {
        this.dfs = new DFSClient();
    }

    public DFSClient getClient() {
        return dfs;
    }

    public DFSOutputStream create(String path, boolean overwrite) throws IOException {
        return dfs.create(path, overwrite);
    }

    public DFSOutputStream create(String path, boolean overwrite, long blockSize) throws IOException {
        return dfs.create(path, overwrite, HdfsConfig.DEFAULT_REPLICATION, blockSize);
    }

    /** For HFiles: random access via pread. */
    public PositionedReadable open(String path) throws IOException {
        return PositionedReadable.of(dfs.open(path));
    }

    /** For WALs and small files: sequential reading. */
    public DFSInputStream openStream(String path) throws IOException {
        return dfs.open(path);
    }

    public String readFile(String path) throws IOException {
        try (DFSInputStream in = dfs.open(path)) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            in.transferTo(bos);
            return bos.toString(StandardCharsets.UTF_8);
        }
    }

    public void writeFile(String path, String content) throws IOException {
        try (DFSOutputStream out = dfs.create(path, true)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }
    }

    public boolean exists(String path) throws IOException {
        return dfs.getFileInfo(path) != null;
    }

    public HdfsFileStatus getFileStatus(String path) throws IOException {
        HdfsFileStatus st = dfs.getFileInfo(path);
        if (st == null) {
            throw new FileNotFoundException(path);
        }
        return st;
    }

    /** Lists a directory; an empty array if it does not exist. */
    public HdfsFileStatus[] listStatus(String path) throws IOException {
        return exists(path) ? dfs.listPaths(path) : new HdfsFileStatus[0];
    }

    public boolean mkdirs(String path) throws IOException {
        return dfs.mkdirs(path);
    }

    public boolean rename(String src, String dst) throws IOException {
        return dfs.rename(src, dst);
    }

    public boolean delete(String path, boolean recursive) throws IOException {
        return exists(path) && dfs.delete(path, recursive);
    }

    /**
     * Corresponds to RecoverLeaseFSUtils.recoverFileLease: closes a file that a (crashed) writer left
     * open. Repeats recoverLease until the NameNode reports the file as closed - in between, the
     * DataNodes run the block recovery (minimum length of the replicas = everything hflushed).
     */
    public void recoverFileLease(String path, long timeoutMs) throws IOException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        for (int attempt = 1; ; attempt++) {
            if (dfs.recoverLease(path)) {
                Log.info("RecoverLeaseFSUtils", "Recovered lease, attempt=" + attempt + " on file=" + path);
                return;
            }
            if (System.currentTimeMillis() > deadline) {
                throw new IOException("Timeout recovering lease of " + path);
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }
    }

    /** Simulated crash: open files stay open, the lease is no longer renewed. */
    public void abort() {
        dfs.abort();
    }

    @Override
    public void close() throws IOException {
        dfs.close();
    }
}
