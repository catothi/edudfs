package edudfs.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Configuration in Hadoop format (core-site.xml / hdfs-site.xml). */
class HdfsConfigTest {

    @TempDir
    File tmp;

    @Test
    void parsesHadoopXmlFormat() throws IOException {
        File f = new File(tmp, "hdfs-site.xml");
        Files.writeString(f.toPath(), """
                <?xml version="1.0"?>
                <configuration>
                  <!-- comment -->
                  <property>
                    <name>dfs.datanode.data.dir</name>
                    <value>
                      [DISK]/data/1/dfs/dn,[SSD]/data/2/dfs/dn
                    </value>
                    <description>is ignored</description>
                  </property>
                  <property><name>dfs.replication</name><value>2</value></property>
                </configuration>
                """);
        Map<String, String> conf = HdfsConfig.parseXml(f);
        assertEquals("[DISK]/data/1/dfs/dn,[SSD]/data/2/dfs/dn", conf.get("dfs.datanode.data.dir"));
        assertEquals("2", conf.get("dfs.replication"));
        assertEquals(2, conf.size());
    }

    @Test
    void sizeAndDurationSuffixes() {
        assertEquals(128L << 20, HdfsConfig.parseBytes("128m"));
        assertEquals(1L << 30, HdfsConfig.parseBytes("1G"));
        assertEquals(4096, HdfsConfig.parseBytes("4096"));
        assertEquals(3000, HdfsConfig.parseDurationMs("3s", 1));
        assertEquals(500, HdfsConfig.parseDurationMs("500ms", 1000));
        assertEquals(120000, HdfsConfig.parseDurationMs("2m", 1));
        assertEquals(3000, HdfsConfig.parseDurationMs("3", 1000));  // no suffix: unit of the key
    }

    @Test
    void projectConfigurationIsLoaded() {
        // the project's conf/hdfs-site.xml: two volumes per DataNode, one failure tolerated
        assertTrue(HdfsConfig.LOADED_RESOURCES.stream().anyMatch(f -> f.getName().equals("hdfs-site.xml")));
        assertEquals(2, HdfsConfig.DATANODE_DATA_DIRS.size());
        assertEquals(1, HdfsConfig.FAILED_VOLUMES_TOLERATED);
        assertEquals("localhost", HdfsConfig.NAMENODE_HOST);
        assertEquals(9000, HdfsConfig.NAMENODE_RPC_PORT);
        // -D short keys from pom.xml override the XML file (1s there)
        assertFalse(HdfsConfig.HEARTBEAT_INTERVAL_MS == 1000);
    }
}
