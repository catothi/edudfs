package edudfs.protocol;

import java.io.Serializable;

/** Corresponds to org.apache.hadoop.hdfs.protocol.HdfsFileStatus (without owner/group/permission). */
public class HdfsFileStatus implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String path;
    private final boolean isdir;
    private final long length;
    private final short replication;
    private final long blockSize;
    private final long modificationTime;
    private final long fileId;
    private final boolean underConstruction;

    public HdfsFileStatus(String path, boolean isdir, long length, short replication, long blockSize,
                          long modificationTime, long fileId, boolean underConstruction) {
        this.path = path;
        this.isdir = isdir;
        this.length = length;
        this.replication = replication;
        this.blockSize = blockSize;
        this.modificationTime = modificationTime;
        this.fileId = fileId;
        this.underConstruction = underConstruction;
    }

    public String getPath() {
        return path;
    }

    public boolean isDir() {
        return isdir;
    }

    public long getLen() {
        return length;
    }

    public short getReplication() {
        return replication;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    /** The INode ID - the client uses it in addBlock/complete so that renames do not interfere. */
    public long getFileId() {
        return fileId;
    }

    public boolean isUnderConstruction() {
        return underConstruction;
    }
}
