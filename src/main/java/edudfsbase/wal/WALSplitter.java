package edudfsbase.wal;

import edudfsbase.HConstants;
import edudfsbase.TableName;
import edudfsbase.fs.CommonFSUtils;
import edudfsbase.fs.HFileSystem;
import edudfs.client.DFSInputStream;
import edudfs.common.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Corresponds to org.apache.hadoop.hbase.wal.WALSplitter: splits the WAL file of a crashed
 * RegionServer by region.
 *
 * The edits of all regions of the dead server are mixed in its WAL. Before a region can be opened
 * on another server, its edits must be sorted out:
 * <pre>
 *   /hbase/WALs/&lt;dead server&gt;-splitting/&lt;wal&gt;
 *        -&gt; /hbase/data/default/&lt;table&gt;/&lt;region&gt;/recovered.edits/&lt;highest seqid, 19 digits&gt;
 * </pre>
 * The files in recovered.edits have the WAL format. When opening the region, HRegion replays them
 * into the MemStore (only edits newer than what the HFiles already contain) and flushes.
 */
public final class WALSplitter {

    private static final String LOG = "WALSplitter";

    private WALSplitter() {
    }

    /** Result of splitting one WAL file (for log and shell). */
    public record SplitResult(String wal, int entries, Map<String, Integer> editsPerRegion, boolean truncated) {
    }

    /** The lease of the file must already be recovered (RecoverLeaseFSUtils). */
    public static SplitResult splitLogFile(HFileSystem fs, String walPath) throws IOException {
        Map<String, List<WAL.Entry>> byRegion = new LinkedHashMap<>();
        Map<String, TableName> tables = new LinkedHashMap<>();
        int entries = 0;
        boolean truncated;
        try (DFSInputStream in = fs.openStream(walPath);
             TextLogReader reader = new TextLogReader(in, walPath)) {
            WAL.Entry e;
            while ((e = reader.next()) != null) {
                byRegion.computeIfAbsent(e.getKey().getEncodedRegionName(), k -> new ArrayList<>()).add(e);
                tables.put(e.getKey().getEncodedRegionName(), e.getKey().getTableName());
                entries++;
            }
            truncated = reader.isTruncated();
        }
        Map<String, Integer> perRegion = new LinkedHashMap<>();
        for (Map.Entry<String, List<WAL.Entry>> r : byRegion.entrySet()) {
            String regionDir = CommonFSUtils.getTableDir(tables.get(r.getKey())) + "/" + r.getKey();
            if (!fs.exists(regionDir)) {
                Log.info(LOG, "Region " + r.getKey() + " no longer exists (table dropped?) - skipping "
                        + r.getValue().size() + " edits");
                continue;
            }
            long maxSeqId = 0;
            for (WAL.Entry e : r.getValue()) {
                maxSeqId = Math.max(maxSeqId, e.getKey().getSequenceId());
            }
            String dir = regionDir + "/" + HConstants.RECOVERED_EDITS_DIR;
            String tmp = dir + "/" + String.format("%019d", maxSeqId) + ".temp";
            try (TextLogWriter w = new TextLogWriter(fs.create(tmp, true))) {
                for (WAL.Entry e : r.getValue()) {
                    w.append(e);
                }
            }
            String dst = dir + "/" + String.format("%019d", maxSeqId);
            fs.delete(dst, false);
            fs.rename(tmp, dst);
            perRegion.put(r.getKey(), r.getValue().size());
            Log.info(LOG, "Wrote " + r.getValue().size() + " edits for region " + r.getKey() + " to " + dst);
        }
        return new SplitResult(CommonFSUtils.getName(walPath), entries, perRegion, truncated);
    }
}
