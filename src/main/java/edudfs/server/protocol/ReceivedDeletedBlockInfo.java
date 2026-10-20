package edudfs.server.protocol;

import edudfs.protocol.Block;

import java.io.Serializable;

/** Corresponds to org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo (entry in the IBR). */
public class ReceivedDeletedBlockInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum BlockStatus {
        RECEIVED_BLOCK,
        DELETED_BLOCK
    }

    private final Block block;
    private final BlockStatus status;

    public ReceivedDeletedBlockInfo(Block block, BlockStatus status) {
        this.block = block;
        this.status = status;
    }

    public Block getBlock() {
        return block;
    }

    public BlockStatus getStatus() {
        return status;
    }

    @Override
    public String toString() {
        return block + "(" + status + ")";
    }
}
