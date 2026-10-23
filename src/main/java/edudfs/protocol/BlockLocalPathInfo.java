package edudfs.protocol;

import java.io.Serializable;

/**
 * Corresponds to org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo: where a DataNode keeps a replica
 * on its local disk - block file and checksum (.meta) file. In the original used by legacy short-circuit
 * reads; here to show students the files behind an HDFS path (e.g. in the EduDFSBase metasave).
 */
public class BlockLocalPathInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Block block;
    private final String localBlockPath;
    private final String localMetaPath;

    public BlockLocalPathInfo(Block block, String localBlockPath, String localMetaPath) {
        this.block = block;
        this.localBlockPath = localBlockPath;
        this.localMetaPath = localMetaPath;
    }

    public Block getBlock() {
        return block;
    }

    public String getBlockPath() {
        return localBlockPath;
    }

    public String getMetaPath() {
        return localMetaPath;
    }

    @Override
    public String toString() {
        return localBlockPath;
    }
}
