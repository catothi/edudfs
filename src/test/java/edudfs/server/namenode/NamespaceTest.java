package edudfs.server.namenode;

import edudfs.protocol.Block;
import edudfs.protocol.FileAlreadyExistsException;
import edudfs.server.blockmanagement.BlockIdManager;
import edudfs.server.blockmanagement.BlockInfo;
import edudfs.server.blockmanagement.BlockManager;
import edudfs.server.blockmanagement.SequentialBlockIdGenerator;
import edudfs.server.common.GenerationStamp;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The in-memory namespace: INodes, path resolution, FSDirectory, ID allocation. */
class NamespaceTest {

    private final FSDirectory dir = new FSDirectory();

    private INodeFile addFile(String parent, String name) throws IOException {
        return dir.addFile(dir.mkdirs(parent, 0), name, (short) 3, 1024, "client", "host", 0);
    }

    // ------------------------------------------------------------- INode-IDs
    @Test
    void reservedIdsAndRootId() {
        assertEquals(16384, INodeId.LAST_RESERVED_ID);
        assertEquals(16385, INodeId.ROOT_INODE_ID);
        assertEquals(16385, dir.getRoot().getId());
        assertTrue(dir.getRoot().isRoot());
        assertEquals("/", dir.getRoot().getFullPathName());
    }

    @Test
    void inodeIdsAreSequentialAndNeverReused() throws IOException {
        INodeDirectory a = dir.mkdirs("/a", 0);
        INodeFile f = addFile("/a", "f");
        assertEquals(16386, a.getId());
        assertEquals(16387, f.getId());
        dir.delete(f, new ArrayList<>(), new ArrayList<>(), 0);
        assertEquals(16388, addFile("/a", "g").getId());
    }

    @Test
    void inodeMapFindsInodesById() throws IOException {
        INodeFile f = addFile("/x/y", "z");
        assertSame(f, dir.getInode(f.getId()));
        assertEquals("/x/y/z", dir.getInode(f.getId()).getFullPathName());
    }

    // ------------------------------------------------------------ directory
    @Test
    void childrenAreKeptSortedByName() throws IOException {
        for (String n : new String[]{"m", "c", "x", "a", "k"}) {
            dir.mkdirs("/d/" + n, 0);
        }
        List<String> names = new ArrayList<>();
        for (INode n : dir.getINodesInPath("/d").getLastINode().asDirectory().getChildrenList()) {
            names.add(n.getLocalName());
        }
        assertEquals(List.of("a", "c", "k", "m", "x"), names);
    }

    @Test
    void duplicateChildIsRejected() throws IOException {
        INodeDirectory d = dir.mkdirs("/d", 0);
        assertTrue(d.addChild(new INodeDirectory(99999, "same", 0)));
        assertFalse(d.addChild(new INodeDirectory(99998, "same", 0)));
    }

    @Test
    void mkdirsIsIdempotentAndCreatesParents() throws IOException {
        INodeDirectory c = dir.mkdirs("/a/b/c", 0);
        assertSame(c, dir.mkdirs("/a/b/c", 0));
        assertEquals("/a/b/c", c.getFullPathName());
        assertEquals(4, dir.getInodeCount());  // root, a, b, c
    }

    @Test
    void mkdirsThroughFileFails() throws IOException {
        addFile("/a", "file");
        assertThrows(FileAlreadyExistsException.class, () -> dir.mkdirs("/a/file/sub", 0));
    }

    // -------------------------------------------------------- INodesInPath
    @Test
    void pathComponents() throws IOException {
        assertArrayEquals(new String[]{""}, INodesInPath.getPathComponents("/"));
        assertArrayEquals(new String[]{"", "a", "b"}, INodesInPath.getPathComponents("/a//b/"));
        assertThrows(IOException.class, () -> INodesInPath.getPathComponents("relative/path"));
        assertThrows(IOException.class, () -> INodesInPath.getPathComponents("/a/../b"));
    }

    @Test
    void resolveMarksMissingComponentsAsNull() throws IOException {
        dir.mkdirs("/a", 0);
        INodesInPath iip = dir.getINodesInPath("/a/missing/deeper");
        assertEquals(4, iip.length());
        assertSame(dir.getRoot(), iip.getINode(0));
        assertEquals("a", iip.getINode(1).getLocalName());
        assertNull(iip.getINode(2));
        assertNull(iip.getLastINode());
        assertEquals("deeper", iip.getLastLocalName());
        assertEquals("/a/missing", iip.getParentPath());
    }

    // --------------------------------------------------------------- Rename
    @Test
    void renameKeepsInodeId() throws IOException {
        INodeFile f = addFile("/src", "f");
        long id = f.getId();
        assertFalse(dir.rename("/src/f", "/dst/g", 0));  // target parent directory is missing
        dir.mkdirs("/dst", 0);
        assertTrue(dir.rename("/src/f", "/dst/g", 0));
        INode moved = dir.getINodesInPath("/dst/g").getLastINode();
        assertSame(f, moved);
        assertEquals(id, moved.getId());
        assertNull(dir.getINodesInPath("/src/f").getLastINode());
    }

    @Test
    void renameIntoExistingDirectoryKeepsName() throws IOException {
        addFile("/a", "f");
        dir.mkdirs("/b", 0);
        assertTrue(dir.rename("/a/f", "/b", 0));
        assertEquals("/b/f", dir.getINodesInPath("/b/f").getLastINode().getFullPathName());
    }

    @Test
    void renameOntoExistingFileFails() throws IOException {
        addFile("/a", "f");
        addFile("/a", "g");
        assertFalse(dir.rename("/a/f", "/a/g", 0));
    }

    @Test
    void renameIntoOwnSubtreeFails() throws IOException {
        dir.mkdirs("/a/b", 0);
        assertThrows(IOException.class, () -> dir.rename("/a", "/a/b/c", 0));
    }

    @Test
    void renameMissingSourceFails() {
        assertThrows(FileNotFoundException.class, () -> dir.rename("/nope", "/x", 0));
    }

    // --------------------------------------------------------------- Delete
    @Test
    void deleteCollectsBlocksAndRemovesInodesFromMap() throws IOException {
        INodeFile f1 = addFile("/d/sub", "f1");
        INodeFile f2 = addFile("/d", "f2");
        f1.addBlock(new BlockInfo(new Block(1, 10, 1001), (short) 3));
        f2.addBlock(new BlockInfo(new Block(2, 10, 1002), (short) 3));
        f2.addBlock(new BlockInfo(new Block(3, 10, 1003), (short) 3));
        List<BlockInfo> blocks = new ArrayList<>();
        List<INode> removed = new ArrayList<>();
        INode d = dir.getINodesInPath("/d").getLastINode();
        dir.delete(d, blocks, removed, 0);
        assertEquals(3, blocks.size());
        assertEquals(4, removed.size());  // d, sub, f1, f2
        assertNull(dir.getInode(f1.getId()));
        assertNull(dir.getINodesInPath("/d").getLastINode());
        assertEquals(1, dir.getInodeCount());
    }

    // ------------------------------------------------------------- INodeFile
    @Test
    void headerPacksReplicationAndBlockSize() {
        long bs = 128L * 1024 * 1024;
        INodeFile f = new INodeFile(1, "f", 0, (short) 3, bs);
        assertEquals(3, f.getFileReplication());
        assertEquals(bs, f.getPreferredBlockSize());
        INodeFile big = new INodeFile(2, "g", 0, (short) 512, (1L << 40));
        assertEquals(512, big.getFileReplication());
        assertEquals(1L << 40, big.getPreferredBlockSize());
    }

    @Test
    void fileSizeExcludesLastUnderConstructionBlockOnRequest() throws IOException {
        INodeFile f = addFile("/", "f");
        BlockInfo b1 = new BlockInfo(new Block(1, 100, 1001), (short) 3);
        BlockInfo b2 = new BlockInfo(new Block(2, 40, 1002), (short) 3);
        b2.convertToBlockUnderConstruction(edudfs.server.common.HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION,
                new edudfs.server.blockmanagement.DatanodeStorageInfo[0]);
        f.addBlock(b1);
        f.addBlock(b2);
        assertEquals(140, f.computeFileSize(true));
        assertEquals(100, f.computeFileSize(false));
        assertSame(b2, f.getLastBlock());
        assertSame(b1, f.getPenultimateBlock());
        assertThrows(IllegalStateException.class, () -> f.toCompleteFile(0));  // b2 not COMPLETE
        assertTrue(f.removeLastBlock(b2));
        f.toCompleteFile(0);
        assertFalse(f.isUnderConstruction());
    }

    // ------------------------------------------------------- Block-IDs / GS
    @Test
    void firstBlockIdAndGenerationStamp() {
        BlockIdManager ids = new BlockIdManager(new BlockManager(new FSNamesystem()));
        assertEquals(1073741825L, ids.nextBlockId());
        assertEquals(1073741826L, ids.nextBlockId());
        assertEquals(1001, ids.nextGenerationStamp());
        assertEquals(1002, ids.nextGenerationStamp());
        assertEquals(1L << 30, SequentialBlockIdGenerator.LAST_RESERVED_BLOCK_ID);
        assertEquals(1000, GenerationStamp.LAST_RESERVED_STAMP);
    }

    @Test
    void blockIdGeneratorSkipsIdsAlreadyInUse() {
        FSNamesystem ns = new FSNamesystem();
        BlockManager bm = ns.getBlockManager();
        // "legacy" block that happens to occupy the next ID
        bm.addBlockCollection(new BlockInfo(new Block(1073741825L, 0, 1), (short) 1), 16386);
        BlockIdManager ids = new BlockIdManager(bm);
        assertEquals(1073741826L, ids.nextBlockId());
    }
}
