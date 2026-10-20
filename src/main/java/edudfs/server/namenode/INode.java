package edudfs.server.namenode;

import edudfs.server.blockmanagement.BlockInfo;

import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.namenode.INode.
 *
 * Base class for files and directories in the in-memory namespace.
 * Every INode has a unique 64-bit ID that is never reused.
 * Simplification: name as a String (original: byte[] UTF-8, saves memory),
 * no permissions/owner/group, no access time, no snapshots/quotas/xattrs.
 */
public abstract class INode {

    private final long id;
    private String localName;
    private INodeDirectory parent;
    private long modificationTime;

    protected INode(long id, String localName, long modificationTime) {
        this.id = id;
        this.localName = localName;
        this.modificationTime = modificationTime;
    }

    public long getId() {
        return id;
    }

    public String getLocalName() {
        return localName;
    }

    void setLocalName(String name) {
        this.localName = name;
    }

    public INodeDirectory getParent() {
        return parent;
    }

    void setParent(INodeDirectory parent) {
        this.parent = parent;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    void setModificationTime(long t) {
        this.modificationTime = t;
    }

    public abstract boolean isFile();

    public abstract boolean isDirectory();

    public INodeFile asFile() {
        throw new IllegalStateException("Not a file: " + getFullPathName());
    }

    public INodeDirectory asDirectory() {
        throw new IllegalStateException("Not a directory: " + getFullPathName());
    }

    public boolean isRoot() {
        return id == INodeId.ROOT_INODE_ID;
    }

    /** Path built by walking up the parent references (the path is not stored anywhere!). */
    public String getFullPathName() {
        if (isRoot()) {
            return "/";
        }
        if (parent == null) {
            return localName;  // already removed from the tree
        }
        String p = parent.getFullPathName();
        return p.equals("/") ? "/" + localName : p + "/" + localName;
    }

    /** Destroy the subtree: collect blocks and removed INodes (for delete). */
    abstract void destroyAndCollectBlocks(List<BlockInfo> collectedBlocks, List<INode> removedINodes);

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + getFullPathName() + ", id=" + id + ")";
    }
}
