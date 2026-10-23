package edudfsbase.regionserver;

import edudfsbase.fs.CommonFSUtils;
import edudfsbase.fs.HFileSystem;
import edudfsbase.io.hfile.HFile;
import edudfsbase.io.hfile.HFileInfo;
import edudfsbase.io.hfile.LruBlockCache;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.HStoreFile: an HFile of a store, opened.
 *
 * After a compaction the old files are "compacted away", but may still be read by scanners that
 * were opened before. They are closed only when the last of these scanners is done
 * (reference counting, in the original: refCount + CompactedHFilesDischarger).
 */
public class HStoreFile {

    private final String path;
    private final String name;
    private final StoreFileReader reader;
    private final long maxSequenceId;
    private final boolean majorCompaction;
    private final String createdBy;
    private final AtomicInteger refCount = new AtomicInteger();
    private volatile boolean compactedAway;
    private volatile boolean closed;

    public HStoreFile(HFileSystem fs, String path, LruBlockCache cache) throws IOException {
        this.path = path;
        this.name = CommonFSUtils.getName(path);
        this.reader = new StoreFileReader(HFile.createReader(name, fs.open(path), cache));
        HFileInfo fi = reader.getHFileReader().getHFileInfo();
        this.maxSequenceId = Long.parseLong(fi.get(HFileInfo.MAX_SEQ_ID_KEY));
        this.majorCompaction = Boolean.parseBoolean(fi.get(HFileInfo.MAJOR_COMPACTION_KEY));
        String by = fi.get(HFileInfo.EDU_CREATED_BY);
        this.createdBy = by == null ? "?" : by;
    }

    public String getPath() {
        return path;
    }

    public String getName() {
        return name;
    }

    /** First 8 characters of the name - enough to tell files apart in tables. */
    public String getShortName() {
        return name.substring(0, Math.min(8, name.length()));
    }

    public StoreFileReader getReader() {
        return reader;
    }

    public long getMaxSequenceId() {
        return maxSequenceId;
    }

    public boolean isMajorCompactionResult() {
        return majorCompaction;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public long length() {
        return reader.length();
    }

    void increaseRefCount() {
        refCount.incrementAndGet();
    }

    void decreaseRefCount() {
        refCount.decrementAndGet();
        closeIfUnused();
    }

    void markCompactedAway() {
        compactedAway = true;
        closeIfUnused();
    }

    private synchronized void closeIfUnused() {
        if (compactedAway && refCount.get() <= 0 && !closed) {
            closeStoreFile();
        }
    }

    public synchronized void closeStoreFile() {
        if (!closed) {
            closed = true;
            try {
                reader.close();
            } catch (IOException ignored) {
                // nothing left to do
            }
        }
    }

    @Override
    public String toString() {
        return name;
    }
}
