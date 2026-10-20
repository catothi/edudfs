package edudfs.common;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DataChecksumTest {

    private static byte[] data(int n) {
        byte[] b = new byte[n];
        new Random(42).nextBytes(b);
        return b;
    }

    @Test
    void numChunksRoundsUpTo512ByteChunks() {
        assertEquals(0, DataChecksum.numChunks(0));
        assertEquals(1, DataChecksum.numChunks(1));
        assertEquals(1, DataChecksum.numChunks(512));
        assertEquals(2, DataChecksum.numChunks(513));
        assertEquals(128, DataChecksum.numChunks(64 * 1024));
    }

    @Test
    void computedSumsVerify() {
        byte[] d = data(1300);  // 2 full chunks + 1 partial chunk
        int[] sums = DataChecksum.computeChunkedSums(d, 0, d.length);
        assertEquals(3, sums.length);
        assertDoesNotThrow(() -> DataChecksum.verifyChunkedSums(d, 0, d.length, sums, "test"));
    }

    @Test
    void lastPartialChunkHasOwnCrc() {
        byte[] d = data(1300);
        int[] sums = DataChecksum.computeChunkedSums(d, 0, d.length);
        assertEquals(DataChecksum.crc(d, 1024, 276), sums[2]);
    }

    @Test
    void flippedBitIsDetected() {
        byte[] d = data(2048);
        int[] sums = DataChecksum.computeChunkedSums(d, 0, d.length);
        d[1500] ^= 0x01;
        ChecksumException e = assertThrows(ChecksumException.class,
                () -> DataChecksum.verifyChunkedSums(d, 0, d.length, sums, "blk"));
        assertEquals(true, e.getMessage().contains("chunk 2"));
    }

    @Test
    void wrongNumberOfChecksumsIsRejected() {
        byte[] d = data(1024);
        assertThrows(ChecksumException.class,
                () -> DataChecksum.verifyChunkedSums(d, 0, d.length, new int[1], "blk"));
    }
}
