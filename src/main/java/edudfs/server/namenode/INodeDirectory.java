package edudfs.server.namenode;

import edudfs.server.blockmanagement.BlockInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.namenode.INodeDirectory.
 * Children are kept - as in the original - in an ArrayList SORTED by name;
 * lookup via binary search (no HashMap overhead per directory).
 */
public class INodeDirectory extends INode {

    private List<INode> children = null;  // lazy, many directories are empty

    public INodeDirectory(long id, String name, long mtime) {
        super(id, name, mtime);
    }

    @Override
    public boolean isFile() {
        return false;
    }

    @Override
    public boolean isDirectory() {
        return true;
    }

    @Override
    public INodeDirectory asDirectory() {
        return this;
    }

    /** Binary search; if not found: -(insertion point) - 1. */
    private int searchChildren(String name) {
        if (children == null) {
            return -1;
        }
        int lo = 0;
        int hi = children.size() - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int c = children.get(mid).getLocalName().compareTo(name);
            if (c < 0) {
                lo = mid + 1;
            } else if (c > 0) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return -(lo + 1);
    }

    public INode getChild(String name) {
        int i = searchChildren(name);
        return i < 0 ? null : children.get(i);
    }

    boolean addChild(INode node) {
        int i = searchChildren(node.getLocalName());
        if (i >= 0) {
            return false;
        }
        if (children == null) {
            children = new ArrayList<>(5);
        }
        children.add(-i - 1, node);
        node.setParent(this);
        return true;
    }

    boolean removeChild(INode child) {
        int i = searchChildren(child.getLocalName());
        if (i < 0 || children.get(i) != child) {
            return false;
        }
        children.remove(i);
        child.setParent(null);
        return true;
    }

    public List<INode> getChildrenList() {
        return children == null ? List.of() : Collections.unmodifiableList(children);
    }

    @Override
    void destroyAndCollectBlocks(List<BlockInfo> collectedBlocks, List<INode> removedINodes) {
        for (INode child : new ArrayList<>(getChildrenList())) {
            child.destroyAndCollectBlocks(collectedBlocks, removedINodes);
        }
        removedINodes.add(this);
    }
}
