package edudfs.protocol;

/**
 * Corresponds to org.apache.hadoop.fs.StorageType: the kind of storage medium of a volume.
 *
 * Given as a prefix in dfs.datanode.data.dir, e.g. "[SSD]/data/3/dfs/dn".
 * In the original, storage policies (HOT, COLD, ALL_SSD, ...) decide based on the type
 * where replicas are placed. Here only DISK is used; the other types
 * are parsed and reported, but not specifically selected.
 */
public enum StorageType {
    RAM_DISK,
    SSD,
    DISK,
    ARCHIVE;

    public static final StorageType DEFAULT = DISK;
}
