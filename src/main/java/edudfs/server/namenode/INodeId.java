package edudfs.server.namenode;

import edudfs.common.SequentialNumber;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.namenode.INodeId.
 * IDs up to 16384 are reserved; the root directory always has ID 16385.
 */
public class INodeId extends SequentialNumber {

    /** Operator precedence as in the original: 2 << (14 - 1) = 16384. */
    public static final long LAST_RESERVED_ID = 2 << 14 - 1;
    public static final long ROOT_INODE_ID = LAST_RESERVED_ID + 1;
    /** "No ID known" - then resolution goes by path. */
    public static final long GRANDFATHER_INODE_ID = 0;

    INodeId() {
        super(ROOT_INODE_ID);
    }
}
