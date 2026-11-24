package edudfs.tools;

import edudfs.client.DFSClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The WebHDFS gateway: create, range reads across block boundaries, listing, block locations, errors. */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class HttpFSServerTest {

    @TempDir
    File baseDir;

    private MiniDFSCluster cluster;
    private DFSClient client;
    private HttpFSServer server;
    private final HttpClient http = HttpClient.newHttpClient();
    private String base;

    @BeforeEach
    void start() throws Exception {
        cluster = new MiniDFSCluster(3, baseDir);
        cluster.waitActive();
        client = new DFSClient();
        server = new HttpFSServer(client, 0);
        server.start();
        base = "http://localhost:" + server.getPort() + "/webhdfs/v1";
    }

    @AfterEach
    void stop() throws Exception {
        server.stop();
        client.close();
        cluster.close();
    }

    private HttpResponse<byte[]> send(String method, String pathAndQuery, byte[] body) throws Exception {
        HttpRequest.BodyPublisher pub = body == null ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofByteArray(body);
        return http.send(HttpRequest.newBuilder(URI.create(base + pathAndQuery)).method(method, pub).build(),
                HttpResponse.BodyHandlers.ofByteArray());
    }

    @Test
    void createThenReadRangesAcrossBlocks() throws Exception {
        byte[] data = new byte[300_000];
        new Random(7).nextBytes(data);
        assertEquals(201, send("PUT", "/x/data.bin?op=CREATE&overwrite=true&blocksize=131072", data).statusCode());

        // a range that spans the boundary between block 1 and block 2
        HttpResponse<byte[]> r = send("GET", "/x/data.bin?op=OPEN&offset=130000&length=5000", null);
        assertEquals(200, r.statusCode());
        assertArrayEquals(Arrays.copyOfRange(data, 130_000, 135_000), r.body());

        // length beyond the end is cut off; no length = up to the end
        assertEquals(1000, send("GET", "/x/data.bin?op=OPEN&offset=299000&length=99999", null).body().length);
        assertArrayEquals(data, send("GET", "/x/data.bin?op=OPEN", null).body());

        String blocks = new String(send("GET", "/x/data.bin?op=GETFILEBLOCKLOCATIONS", null).body());
        assertEquals(3, blocks.split("\"block\"").length - 1, blocks);
    }

    @Test
    void statusListingMkdirsDeleteAndErrors() throws Exception {
        assertEquals("{\"boolean\":true}", new String(send("PUT", "/a/b?op=MKDIRS", null).body()));
        send("PUT", "/a/b/f.txt?op=CREATE", "hello".getBytes());
        String list = new String(send("GET", "/a/b?op=LISTSTATUS", null).body());
        assertTrue(list.contains("\"pathSuffix\":\"f.txt\"") && list.contains("\"length\":5"), list);
        String st = new String(send("GET", "/a/b?op=GETFILESTATUS", null).body());
        assertTrue(st.contains("\"type\":\"DIRECTORY\""), st);

        HttpResponse<byte[]> missing = send("GET", "/nope?op=GETFILESTATUS", null);
        assertEquals(404, missing.statusCode());
        assertTrue(new String(missing.body()).contains("FileNotFoundException"));
        assertEquals(400, send("GET", "/a?op=NOSUCHOP", null).statusCode());

        assertEquals("{\"boolean\":true}", new String(send("DELETE", "/a?op=DELETE&recursive=true", null).body()));
        assertEquals(404, send("GET", "/a/b/f.txt?op=GETFILESTATUS", null).statusCode());
    }
}
