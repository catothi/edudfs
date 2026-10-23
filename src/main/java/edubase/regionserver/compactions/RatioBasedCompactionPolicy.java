package edubase.regionserver.compactions;

import edubase.HBaseConfiguration;
import edubase.regionserver.HStoreFile;

import java.util.ArrayList;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.compactions.RatioBasedCompactionPolicy:
 * which HFiles a minor compaction picks.
 *
 * Idea: merge files of similar size; do not rewrite a large old file again and again just because a
 * few small new ones were added. Files are considered from OLD to NEW; an old file is skipped as long as
 * <pre>
 *   size(file) &gt; max(minSize, ratio * sum of the sizes of the following (newer) files)
 * </pre>
 * From the first file that does not satisfy this, up to {@code compaction.max} files are taken -
 * if at least {@code compactionThreshold} files remain.
 *
 * Simplification: the original's default is the ExploringCompactionPolicy, which tries all
 * contiguous windows and picks the "cheapest"; the ratio rule is the same.
 */
public final class RatioBasedCompactionPolicy {

    private RatioBasedCompactionPolicy() {
    }

    /** True if a minor compaction would be worthwhile (enough files). */
    public static boolean needsCompaction(List<HStoreFile> files) {
        return files.size() >= HBaseConfiguration.COMPACTION_MIN_FILES;
    }

    /**
     * @param candidates all files of the store, oldest first
     * @return null if there is nothing to do
     */
    public static CompactionRequest selectCompaction(List<HStoreFile> candidates, boolean forceMajor, String why) {
        if (candidates.isEmpty()) {
            return null;
        }
        if (forceMajor) {
            return new CompactionRequest(new ArrayList<>(candidates), true, true, why);
        }
        long[] size = new long[candidates.size()];
        for (int i = 0; i < size.length; i++) {
            size[i] = candidates.get(i).length();
        }
        int[] window = applyCompactionPolicy(size, HBaseConfiguration.COMPACTION_MIN_FILES,
                HBaseConfiguration.COMPACTION_MAX_FILES, HBaseConfiguration.COMPACTION_RATIO,
                HBaseConfiguration.COMPACTION_MIN_SIZE);
        if (window == null) {
            return null;
        }
        List<HStoreFile> selected = new ArrayList<>(candidates.subList(window[0], window[1]));
        return new CompactionRequest(selected, false, selected.size() == candidates.size(), why);
    }

    /**
     * The ratio rule on the file sizes (oldest first).
     * @return [start, end) of the selected files, or null
     */
    static int[] applyCompactionPolicy(long[] size, int minFiles, int maxFiles, double ratio, long minSize) {
        int n = size.length;
        if (n < minFiles) {
            return null;
        }
        int start = 0;
        while (n - start >= minFiles
                && size[start] > Math.max(minSize, (long) (sumNewer(size, start + 1, maxFiles) * ratio))) {
            start++;
        }
        int end = Math.min(n, start + maxFiles);
        return end - start < minFiles ? null : new int[]{start, end};
    }

    /** Sum of the sizes of the (at most maxFiles - 1) files starting at from. */
    private static long sumNewer(long[] size, int from, int maxFiles) {
        long sum = 0;
        for (int i = from; i < Math.min(size.length, from + maxFiles - 1); i++) {
            sum += size[i];
        }
        return sum;
    }
}
