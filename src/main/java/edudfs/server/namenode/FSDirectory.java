package edudfs.server.namenode;

import edudfs.common.Time;
import edudfs.protocol.FileAlreadyExistsException;
import edudfs.protocol.HdfsFileStatus;
import edudfs.server.blockmanagement.BlockInfo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Corresponds to org.apache.hadoop.hdfs.server.namenode.FSDirectory
 * (including the logic from FSDirMkdirOp, FSDirWriteFileOp, FSDirDeleteOp, FSDirRenameOp).
 *
 * Holds the entire namespace in main memory:
 * <ul>
 *   <li>the INode tree starting at rootDir</li>
 *   <li>the INodeMap: INode ID -> INode (for access by fileId, e.g. from leases and BlockInfo.bcId)</li>
 * </ul>
 * Synchronization: all calls are made under the FSNamesystem lock.
 * Simplification: changes are NOT written to the EditLog (no persistence).
 */
public class FSDirectory {

    private final INodeDirectory rootDir;
    /** In the original a GSet (like the BlocksMap). */
    private final Map<Long, INode> inodeMap = new HashMap<>();
    private final INodeId inodeId = new INodeId();

    FSDirectory() {
        rootDir = new INodeDirectory(INodeId.ROOT_INODE_ID, "", Time.now());
        inodeMap.put(rootDir.getId(), rootDir);
    }

    public INodeDirectory getRoot() {
        return rootDir;
    }

    public INode getInode(long id) {
        return inodeMap.get(id);
    }

    public int getInodeCount() {
        return inodeMap.size();
    }

    public long getLastInodeId() {
        return inodeId.getCurrentValue();
    }

    /** Contents of the INodeMap, sorted by ID (for metaSave). */
    List<INode> getInodesSortedById() {
        List<INode> list = new ArrayList<>(inodeMap.values());
        list.sort(Comparator.comparingLong(INode::getId));
        return list;
    }

    INodesInPath getINodesInPath(String src) throws IOException {
        return INodesInPath.resolve(rootDir, INodesInPath.getPathComponents(src));
    }

    // ------------------------------------------------------- FSDirMkdirOp
    /** Creates all missing directories (like 'mkdir -p'). */
    INodeDirectory mkdirs(String src, long now) throws IOException {
        String[] comps = INodesInPath.getPathComponents(src);
        INodeDirectory cur = rootDir;
        for (int i = 1; i < comps.length; i++) {
            INode child = cur.getChild(comps[i]);
            if (child == null) {
                INodeDirectory dir = new INodeDirectory(inodeId.nextValue(), comps[i], now);
                cur.addChild(dir);
                cur.setModificationTime(now);
                inodeMap.put(dir.getId(), dir);
                child = dir;
            } else if (!child.isDirectory()) {
                throw new FileAlreadyExistsException("Parent path is not a directory: " + child.getFullPathName());
            }
            cur = child.asDirectory();
        }
        return cur;
    }

    // --------------------------------------------------- FSDirWriteFileOp
    INodeFile addFile(INodeDirectory parent, String name, short replication, long blockSize,
                      String clientName, String clientMachine, long now) throws IOException {
        INodeFile file = new INodeFile(inodeId.nextValue(), name, now, replication, blockSize);
        file.toUnderConstruction(clientName, clientMachine);
        if (!parent.addChild(file)) {
            throw new FileAlreadyExistsException(parent.getFullPathName() + "/" + name + " already exists");
        }
        parent.setModificationTime(now);
        inodeMap.put(file.getId(), file);
        return file;
    }

    // ------------------------------------------------------ FSDirDeleteOp
    /** Detaches the subtree and collects all blocks/INodes. */
    void delete(INode target, List<BlockInfo> collectedBlocks, List<INode> removedINodes, long now) {
        INodeDirectory parent = target.getParent();
        parent.removeChild(target);
        parent.setModificationTime(now);
        target.destroyAndCollectBlocks(collectedBlocks, removedINodes);
        for (INode n : removedINodes) {
            inodeMap.remove(n.getId());
        }
    }

    // ------------------------------------------------------ FSDirRenameOp
    /**
     * Rename is a pure namespace operation: no data is moved.
     * Leases are bound to the INode ID and therefore do not need to be adjusted.
     */
    boolean rename(String src, String dst, long now) throws IOException {
        INode srcNode = getINodesInPath(src).getLastINode();
        if (srcNode == null) {
            throw new FileNotFoundException("rename source " + src + " is not found.");
        }
        if (srcNode.isRoot()) {
            throw new IOException("Cannot rename root");
        }
        INodesInPath dstIIP = getINodesInPath(dst);
        INode dstNode = dstIIP.getLastINode();
        INodeDirectory dstParent;
        String dstName;
        if (dstNode != null && dstNode.isDirectory()) {
            dstParent = dstNode.asDirectory();  // move into the directory
            dstName = srcNode.getLocalName();
        } else if (dstNode != null) {
            return false;  // target exists as a file
        } else {
            INode p = dstIIP.getINode(-2);
            if (p == null || !p.isDirectory()) {
                return false;
            }
            dstParent = p.asDirectory();
            dstName = dstIIP.getLastLocalName();
        }
        if (dstParent.getChild(dstName) != null) {
            return false;
        }
        for (INode n = dstParent; n != null; n = n.getParent()) {
            if (n == srcNode) {
                throw new IOException("Cannot move " + src + " into its own subdirectory " + dst);
            }
        }
        INodeDirectory srcParent = srcNode.getParent();
        srcParent.removeChild(srcNode);
        srcParent.setModificationTime(now);
        srcNode.setLocalName(dstName);
        dstParent.addChild(srcNode);
        dstParent.setModificationTime(now);
        return true;
    }

    // ---------------------------------------------------------- Status
    HdfsFileStatus createFileStatus(INode node) {
        if (node.isDirectory()) {
            return new HdfsFileStatus(node.getFullPathName(), true, 0, (short) 0, 0, node.getModificationTime(),
                    node.getId(), false);
        }
        INodeFile f = node.asFile();
        return new HdfsFileStatus(f.getFullPathName(), false, f.computeFileSize(true), f.getFileReplication(),
                f.getPreferredBlockSize(), f.getModificationTime(), f.getId(), f.isUnderConstruction());
    }
}
