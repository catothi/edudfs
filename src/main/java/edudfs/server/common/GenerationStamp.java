package edudfs.server.common;

import edudfs.common.SequentialNumber;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.common.GenerationStamp.
 * Every new block and every block recovery gets a new, globally
 * monotonically increasing generation stamp.
 */
public class GenerationStamp extends SequentialNumber {

    /** The first 1000 values are reserved (as in the original). */
    public static final long LAST_RESERVED_STAMP = 1000L;
    public static final long GRANDFATHER_GENERATION_STAMP = 0;

    public GenerationStamp() {
        super(LAST_RESERVED_STAMP);
    }
}
