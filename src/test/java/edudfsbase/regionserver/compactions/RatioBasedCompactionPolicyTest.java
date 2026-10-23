package edudfsbase.regionserver.compactions;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Which files a minor compaction selects (sizes oldest first; minFiles 3, maxFiles 10, ratio 1.2, minSize 0). */
class RatioBasedCompactionPolicyTest {

    private static int[] select(long... sizes) {
        return RatioBasedCompactionPolicy.applyCompactionPolicy(sizes, 3, 10, 1.2, 0);
    }

    @Test
    void tooFewFiles() {
        assertNull(select(100, 100));
    }

    @Test
    void similarSizesAreAllSelected() {
        assertArrayEquals(new int[]{0, 3}, select(100, 90, 80));
    }

    @Test
    void largeOldFileIsLeftAlone() {
        // 1000 > 1.2 * (10 + 10 + 10) -> skipped; the three small new files are merged
        assertArrayEquals(new int[]{1, 4}, select(1000, 10, 10, 10));
    }

    @Test
    void nothingIfTheRestIsTooSmallAfterSkipping() {
        assertNull(select(1000, 500, 10, 10));
    }

    @Test
    void atMostMaxFiles() {
        long[] sizes = new long[15];
        Arrays.fill(sizes, 10);
        assertArrayEquals(new int[]{0, 10}, select(sizes));
    }

    @Test
    void smallFilesBelowMinSizeAreAlwaysSelected() {
        assertArrayEquals(new int[]{0, 3},
                RatioBasedCompactionPolicy.applyCompactionPolicy(new long[]{500, 10, 10}, 3, 10, 1.2, 1000));
    }
}
