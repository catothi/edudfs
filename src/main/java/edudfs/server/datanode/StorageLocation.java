package edudfs.server.datanode;

import edudfs.protocol.StorageType;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.datanode.StorageLocation:
 * one entry from dfs.datanode.data.dir, e.g. "[SSD]/data/3/dfs/dn".
 */
public class StorageLocation {

    private static final Pattern REGEX = Pattern.compile("^\\[(\\w*)\\](.+)$");

    private final StorageType storageType;
    private final File dir;

    public StorageLocation(StorageType storageType, File dir) {
        this.storageType = storageType;
        this.dir = dir;
    }

    /**
     * Parses an entry. EduDFS: the path is resolved relative to the root directory of the
     * (simulated) machine - in the original it is an absolute path on the machine.
     */
    public static StorageLocation parse(String rawLocation, File hostRoot) {
        StorageType type = StorageType.DEFAULT;
        String path = rawLocation.trim();
        Matcher m = REGEX.matcher(path);
        if (m.matches()) {
            if (!m.group(1).isEmpty()) {
                type = StorageType.valueOf(m.group(1).toUpperCase());
            }
            path = m.group(2);
        }
        if (path.startsWith("file://")) {
            path = path.substring("file://".length());
        }
        return new StorageLocation(type, new File(hostRoot, path));
    }

    public static List<StorageLocation> parseAll(List<String> rawLocations, File hostRoot) {
        List<StorageLocation> r = new ArrayList<>();
        for (String s : rawLocations) {
            r.add(parse(s, hostRoot));
        }
        return r;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    /** The volume directory (= value from dfs.datanode.data.dir). */
    public File getDir() {
        return dir;
    }

    @Override
    public String toString() {
        return "[" + storageType + "]" + dir.getPath();
    }
}
