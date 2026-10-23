package edudfsbase.io.hfile;

import java.util.Map;
import java.util.TreeMap;

/**
 * Corresponds to org.apache.hadoop.hbase.io.hfile.HFileInfo (the "FileInfo"): metadata of an HFile
 * as key/value pairs. Stored as block FILEINF2 in the load-on-open section.
 * The key names are those of the original (HFileInfo, HStoreFile).
 */
public class HFileInfo {

    // written by the HFile writer
    public static final String LASTKEY = "hfile.LASTKEY";
    public static final String AVG_KEY_LEN = "hfile.AVG_KEY_LEN";
    public static final String AVG_VALUE_LEN = "hfile.AVG_VALUE_LEN";
    public static final String CREATE_TIME_KEY = "hfile.CREATE_TIME_KEY";

    // written by the StoreFileWriter (constants of HStoreFile in the original)
    /** Highest sequence ID of the edits in this file. Decides which WAL edits must be replayed. */
    public static final String MAX_SEQ_ID_KEY = "MAX_SEQ_ID_KEY";
    /** true if the file is the result of a major compaction (contains no delete markers any more). */
    public static final String MAJOR_COMPACTION_KEY = "MAJOR_COMPACTION_KEY";
    public static final String BLOOM_FILTER_TYPE_KEY = "BLOOM_FILTER_TYPE";
    /** Smallest and largest timestamp in the file - lets a scan with a time range skip whole files. */
    public static final String TIMERANGE_KEY = "TIMERANGE";
    public static final String EARLIEST_PUT_TS = "EARLIEST_PUT_TS";
    public static final String DELETE_FAMILY_COUNT = "DELETE_FAMILY_COUNT";
    public static final String MAX_MEMSTORE_TS_KEY = "MAX_MEMSTORE_TS_KEY";
    public static final String KEY_VALUE_VERSION = "KEY_VALUE_VERSION";
    /** Names of the files a compaction combined into this one. */
    public static final String COMPACTION_EVENT_KEY = "COMPACTION_EVENT_KEY";
    /** EduDFSBase only: who wrote the file (flush, minor compaction, major compaction). */
    public static final String EDU_CREATED_BY = "EDU_CREATED_BY";

    private final Map<String, String> map = new TreeMap<>();

    public void put(String key, String value) {
        map.put(key, value);
    }

    public String get(String key) {
        return map.get(key);
    }

    public Map<String, String> asMap() {
        return map;
    }

    String toBody() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : map.entrySet()) {
            sb.append(e.getKey()).append('=').append(e.getValue()).append('\n');
        }
        return sb.toString();
    }

    static HFileInfo parse(HFileBlock block) {
        HFileInfo fi = new HFileInfo();
        for (String line : block.getLines()) {
            int i = line.indexOf('=');
            fi.put(line.substring(0, i), line.substring(i + 1));
        }
        return fi;
    }
}
