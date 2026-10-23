package edubase.regionserver.compactions;

import edubase.regionserver.HStoreFile;

import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hbase.regionserver.compactions.CompactionRequestImpl:
 * which files of a store are compacted together.
 *
 * Important detail of the original: whether delete markers are dropped does not depend on
 * "major" or "minor", but on whether ALL files of the store take part ({@link #isAllFiles()}).
 * Only then is it certain that no older file contains a cell the marker still has to hide.
 */
public class CompactionRequest {

    private final List<HStoreFile> files;
    private final boolean major;
    private final boolean allFiles;
    private final String reason;

    public CompactionRequest(List<HStoreFile> files, boolean major, boolean allFiles, String reason) {
        this.files = files;
        this.major = major;
        this.allFiles = allFiles;
        this.reason = reason;
    }

    public List<HStoreFile> getFiles() {
        return files;
    }

    public boolean isMajor() {
        return major;
    }

    public boolean isAllFiles() {
        return allFiles;
    }

    public String getReason() {
        return reason;
    }
}
