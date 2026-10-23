package edudfsbase.io.hfile;

import edudfsbase.Cell;
import edudfsbase.KeyValue;
import edudfsbase.codec.KeyValueCodec;
import edudfsbase.io.PositionedReadable;
import edudfsbase.util.BloomFilterChunk;
import edudfsbase.util.Bytes;
import edudfsbase.util.ReadTrace;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HFile format: write into a byte[], read back, seek via the block index (single- and multi-level), detect
 * corruption, the views of HFilePrettyPrinter.
 */
class HFileTest {

    private static final byte[] CF = Bytes.toBytes("cf");

    private static KeyValue kv(int row, String q, long ts, String value) {
        return new KeyValue(Bytes.toBytes(String.format("row%03d", row)), CF, Bytes.toBytes(q), ts,
                KeyValue.Type.Put, Bytes.toBytes(value), row);
    }

    private static byte[] write(List<KeyValue> cells, int blockSize, BloomFilterChunk bloom) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        HFile.Writer w = HFile.getWriter(bos, "test", blockSize);
        for (KeyValue c : cells) {
            w.append(c);
            if (bloom != null) {
                bloom.add(c.getRow());
            }
        }
        w.appendFileInfo(HFileInfo.MAX_SEQ_ID_KEY, "99");
        w.setGeneralBloomFilter(bloom);
        w.close();
        return bos.toByteArray();
    }

    private static List<KeyValue> cells(int rows) {
        List<KeyValue> r = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            r.add(kv(i, "a", 1000, "value-" + i));
            r.add(kv(i, "b", 1000, "other value " + i));
        }
        return r;
    }

    @Test
    void writeAndReadBackWithManyBlocks() throws IOException {
        List<KeyValue> in = cells(100);
        byte[] file = write(in, 256, null);
        HFile.Reader r = HFile.createReader("test", PositionedReadable.of(file), null);
        assertEquals(200, r.getEntries());
        assertTrue(r.getDataBlockCount() > 10);
        assertEquals("99", r.getHFileInfo().get(HFileInfo.MAX_SEQ_ID_KEY));
        assertNull(r.getGeneralBloomFilter());
        HFileScanner s = r.getScanner(false, null);
        assertTrue(s.seekTo());
        int n = 0;
        do {
            Cell c = s.getCell();
            assertEquals(in.get(n).toString(), c.toString());
            assertArrayEquals(in.get(n).getValue(), c.getValue());
            n++;
        } while (s.next());
        assertEquals(200, n);
        assertEquals(KeyValueCodec.encodeKey(in.get(199)), KeyValueCodec.encodeKey(r.getLastKey()));
    }

    @Test
    void fileIsReadableTextWithOriginalMagics() throws IOException {
        String text = new String(write(cells(3), 64 * 1024, new BloomFilterChunk(3, 0.01)), StandardCharsets.US_ASCII);
        assertTrue(text.startsWith("DATABLK* onDiskSizeWithoutHeader="));
        assertTrue(text.contains("K: row001/cf:a/1000/Put/vlen=7/seqid=1 V: value-1\n"));
        for (BlockType t : BlockType.values()) {
            if (t != BlockType.LEAF_INDEX && t != BlockType.INTERMEDIATE_INDEX) {  // a 3-cell file has 1 level
                assertTrue(text.contains(t.getMagic()), t.toString());
            }
        }
        assertTrue(text.endsWith("majorVersion=3 minorVersion=3\n"));
    }

    @Test
    void seekUsesIndexAndFindsFirstCellAtOrAfterKey() throws IOException {
        HFile.Reader r = HFile.createReader("test", PositionedReadable.of(write(cells(100), 200, null)), null);
        HFileScanner s = r.getScanner(false, null);
        assertTrue(s.seekTo(KeyValue.createFirstOnRow(Bytes.toBytes("row057"))));
        assertEquals("row057/cf:a/1000/Put/vlen=8/seqid=57", s.getCell().toString());
        assertTrue(s.reseekTo(KeyValue.createFirstOnRowCol(Bytes.toBytes("row057"), CF, Bytes.toBytes("b"))));
        assertEquals("b", Bytes.toString(s.getCell().getQualifier()));
        assertTrue(s.seekTo(KeyValue.createLastOnRow(Bytes.toBytes("row057"))));
        assertEquals("row058", Bytes.toString(s.getCell().getRow()));
        assertFalse(s.seekTo(KeyValue.createFirstOnRow(Bytes.toBytes("row999"))));
    }

    private static List<KeyValue> numbered(int rows) {
        List<KeyValue> r = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            r.add(new KeyValue(Bytes.toBytes(String.format("%010d", i)), CF, Bytes.toBytes("0"), 1000,
                    KeyValue.Type.Put, Bytes.toBytes("value-" + i), i + 1));
        }
        return r;
    }

    private static HFileReaderImpl multiLevel(List<KeyValue> in, LruBlockCache cache) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        HFile.Writer w = HFile.getWriter(bos, "test", 1024, 512, 4);
        for (KeyValue c : in) {
            w.append(c);
        }
        w.close();
        return (HFileReaderImpl) HFile.createReader("test", PositionedReadable.of(bos.toByteArray()), cache);
    }

    @Test
    void largeFileGetsThreeLevelIndexAndEveryKeyIsFound() throws IOException {
        List<KeyValue> in = numbered(3000);
        HFileReaderImpl r = multiLevel(in, null);
        FixedFileTrailer t = r.getTrailer();
        assertEquals(3, t.getNumDataIndexLevels());
        assertEquals(r.getDataBlockIndexReader().getRootBlockCount(), t.getDataIndexCount());
        assertTrue(t.getUncompressedDataIndexSize() > 0);
        assertEquals(r.length(), t.getTotalUncompressedBytes());

        HFileBlockMap map = new HFileBlockMap(r);
        long dataBlocks = map.getItems().stream().filter(i -> i.type == BlockType.DATA).count();
        assertEquals(dataBlocks, r.getDataBlockCount());
        assertTrue(map.getItems().stream().anyMatch(i -> i.type == BlockType.LEAF_INDEX
                && i.section == HFileBlockMap.Section.SCANNED));
        assertTrue(map.getItems().stream().anyMatch(i -> i.type == BlockType.INTERMEDIATE_INDEX
                && i.section == HFileBlockMap.Section.NON_SCANNED));
        assertEquals(BlockType.ROOT_INDEX, map.at(t.getLoadOnOpenDataOffset()).type);

        HFileScanner s = r.getScanner(false, null);
        for (int i = 0; i < in.size(); i += 7) {
            assertTrue(s.seekTo(KeyValue.createFirstOnRow(in.get(i).getRow())));
            assertEquals(in.get(i).toString(), s.getCell().toString());
        }
        assertTrue(s.seekTo(KeyValue.createFirstOnRow(Bytes.toBytes(""))));
        assertEquals(in.get(0).toString(), s.getCell().toString());
        assertFalse(s.seekTo(KeyValue.createFirstOnRow(Bytes.toBytes("9999999999"))));

        // a full scan crosses the leaf index blocks between the data blocks
        assertTrue(s.seekTo());
        int n = 0;
        do {
            assertEquals(in.get(n).toString(), s.getCell().toString());
            n++;
        } while (s.next());
        assertEquals(3000, n);

        List<HFileBlockIndex.Entry> blocks = r.getDataBlockEntries();
        assertEquals(KeyValueCodec.encodeKey(blocks.get((blocks.size() - 1) / 2).getFirstKey()),
                KeyValueCodec.encodeKey(r.midKey()));
    }

    @Test
    void seekTraceShowsOneStepPerLevelAndCachedIndexBlocks() throws IOException {
        LruBlockCache cache = new LruBlockCache(1 << 20);
        HFileReaderImpl r = multiLevel(numbered(3000), cache);
        ReadTrace trace = new ReadTrace();
        r.getScanner(true, trace).seekTo(KeyValue.createFirstOnRow(Bytes.toBytes("0000001234")));
        String text = String.join("\n", trace.getLines());
        assertTrue(text.contains("block index level 1/3, root IDXROOT2 (in memory"), text);
        assertTrue(text.contains("-> intermediate index block @"), text);
        assertTrue(text.contains("block index level 2/3, IDXINTE2 @"), text);
        assertTrue(text.contains("block index level 3/3, IDXLEAF2 @"), text);
        assertTrue(text.contains("-> data block @"), text);
        long misses = cache.getMissCount();
        assertTrue(misses >= 3, "intermediate, leaf and data block: " + misses);  // + next block at a boundary
        r.getScanner(true, null).seekTo(KeyValue.createFirstOnRow(Bytes.toBytes("0000001234")));
        assertEquals(misses, cache.getHitCount());  // the same index and data blocks, now from the BlockCache
    }

    @Test
    void mediumFileHasTwoLevelsAndSmallFileOne() throws IOException {
        assertEquals(2, multiLevel(numbered(300), null).getTrailer().getNumDataIndexLevels());
        HFileReaderImpl small = multiLevel(numbered(20), null);
        assertEquals(1, small.getTrailer().getNumDataIndexLevels());
        assertEquals(-1, small.getDataBlockIndexReader().getMidLeafBlockOffset());
        assertTrue(new HFileBlockMap(small).getItems().stream().noneMatch(i -> i.type == BlockType.LEAF_INDEX));
    }

    @Test
    void prettyPrinterViews() throws IOException {
        List<KeyValue> in = numbered(3000);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        HFile.Writer w = HFile.getWriter(bos, "test", 1024, 512, 4);
        for (KeyValue c : in) {
            w.append(c);
        }
        w.close();
        java.io.File f = java.io.File.createTempFile("hfile", ".txt");
        java.io.File html = java.io.File.createTempFile("hfile", ".html");
        f.deleteOnExit();
        html.deleteOnExit();
        java.nio.file.Files.write(f.toPath(), bos.toByteArray());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HFilePrettyPrinter pp = new HFilePrettyPrinter(new java.io.PrintStream(out, true, StandardCharsets.UTF_8));
        assertEquals(0, pp.run(new String[]{"-m", "--layout", "--tree", "--offset", "100", "--dump", "--html",
                html.getPath(), "-f", "file:" + f.getPath()}));
        String o = out.toString(StandardCharsets.UTF_8);
        assertTrue(o.contains("numDataIndexLevels=3"), o);
        assertTrue(o.contains("ROOT INDEX OFFSET"), o);
        assertTrue(o.contains("non-scanned block section"), o);
        assertTrue(o.contains("IDXLEAF2 @"), o);
        assertTrue(o.contains("Offset 100 lies inside DATABLK* @0"), o);
        assertTrue(o.contains("Mid-key: 00000"), o);
        String page = java.nio.file.Files.readString(html.toPath());
        assertTrue(page.contains("const D = {"));
        assertFalse(page.contains("__DATA__"));
    }

    @Test
    void keysOutOfOrderAreRejected() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        HFile.Writer w = HFile.getWriter(bos, "test", 1024);
        assertThrows(IOException.class, () -> {
            w.append(kv(2, "a", 1, "x"));
            w.append(kv(1, "a", 1, "x"));
        });
    }

    @Test
    void corruptBlockIsDetectedByChecksum() throws IOException {
        byte[] file = write(cells(10), 64 * 1024, null);
        int i = new String(file, StandardCharsets.US_ASCII).indexOf("value-5");
        file[i] = 'X';
        HFile.Reader r = HFile.createReader("test", PositionedReadable.of(file), null);
        IOException e = assertThrows(IOException.class, () -> r.getScanner(false, null).seekTo());
        assertTrue(e.getMessage().contains("Checksum failed"), e.getMessage());
    }

    @Test
    void bloomFilterIsStoredAndHasNoFalseNegatives() throws IOException {
        byte[] file = write(cells(500), 1024, new BloomFilterChunk(500, 0.01));
        BloomFilterChunk bloom = HFile.createReader("test", PositionedReadable.of(file), null).getGeneralBloomFilter();
        assertNotNull(bloom);
        for (int i = 0; i < 500; i++) {
            assertTrue(bloom.contains(Bytes.toBytes(String.format("row%03d", i))));
        }
        int falsePositives = 0;
        for (int i = 0; i < 10000; i++) {
            if (bloom.contains(Bytes.toBytes("absent" + i))) {
                falsePositives++;
            }
        }
        assertTrue(falsePositives < 300, "false positive rate too high: " + falsePositives + "/10000");
    }

    @Test
    void blockCacheServesSecondRead() throws IOException {
        LruBlockCache cache = new LruBlockCache(1 << 20);
        HFile.Reader r = HFile.createReader("test", PositionedReadable.of(write(cells(50), 256, null)), cache);
        HFileScanner s = r.getScanner(true, null);
        s.seekTo(KeyValue.createFirstOnRow(Bytes.toBytes("row010")));
        long misses = cache.getMissCount();
        assertEquals(0, cache.getHitCount());
        assertTrue(misses >= 1);
        r.getScanner(true, null).seekTo(KeyValue.createFirstOnRow(Bytes.toBytes("row010")));
        assertEquals(misses, cache.getHitCount());  // the same blocks again, now from the cache
        assertEquals(misses, cache.getMissCount());
    }

    @Test
    void codecEscapesSeparatorsAndBinaryBytes() throws IOException {
        KeyValue c = new KeyValue(new byte[]{'a', '/', 'b', ' ', 0, (byte) 0xff}, CF, Bytes.toBytes("x:y"), 5,
                KeyValue.Type.Put, Bytes.toBytes("line1\nline2 \\ end"), 3);
        String line = KeyValueCodec.encode(c);
        assertFalse(line.contains("\n"));
        KeyValue back = KeyValueCodec.decode(line);
        assertArrayEquals(c.getRow(), back.getRow());
        assertArrayEquals(c.getQualifier(), back.getQualifier());
        assertArrayEquals(c.getValue(), back.getValue());
        assertEquals(3, back.getSequenceId());
    }
}
