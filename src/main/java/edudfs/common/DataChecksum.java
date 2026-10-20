package edudfs.common;

import java.util.zip.CRC32;

/**
 * Corresponds to org.apache.hadoop.util.DataChecksum (CRC32 only).
 *
 * HDFS computes one CRC over every {@link HdfsConfig#BYTES_PER_CHECKSUM} bytes ("chunk").
 * The CRCs are stored in the .meta file next to the block file and sent over the network
 * with every packet; the client verifies them when reading.
 */
public final class DataChecksum {

    public static final int CHECKSUM_SIZE = 4;
    public static final byte CHECKSUM_CRC32 = 1;

    private DataChecksum() {
    }

    public static int numChunks(int dataLen) {
        return (dataLen + HdfsConfig.BYTES_PER_CHECKSUM - 1) / HdfsConfig.BYTES_PER_CHECKSUM;
    }

    public static int crc(byte[] data, int off, int len) {
        CRC32 crc = new CRC32();
        crc.update(data, off, len);
        return (int) crc.getValue();
    }

    public static int[] computeChunkedSums(byte[] data, int off, int len) {
        int bpc = HdfsConfig.BYTES_PER_CHECKSUM;
        int[] sums = new int[numChunks(len)];
        for (int i = 0; i < sums.length; i++) {
            int chunkOff = off + i * bpc;
            sums[i] = crc(data, chunkOff, Math.min(bpc, off + len - chunkOff));
        }
        return sums;
    }

    public static void verifyChunkedSums(byte[] data, int off, int len, int[] sums, String context)
            throws ChecksumException {
        int bpc = HdfsConfig.BYTES_PER_CHECKSUM;
        if (sums.length != numChunks(len)) {
            throw new ChecksumException("Wrong number of checksums (" + sums.length + ") for " + context);
        }
        for (int i = 0; i < sums.length; i++) {
            int chunkOff = off + i * bpc;
            int actual = crc(data, chunkOff, Math.min(bpc, off + len - chunkOff));
            if (actual != sums[i]) {
                throw new ChecksumException("Checksum error: " + context + " (chunk " + i + ")");
            }
        }
    }
}
