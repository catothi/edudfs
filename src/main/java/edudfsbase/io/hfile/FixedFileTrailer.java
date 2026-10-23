package edudfsbase.io.hfile;

import edudfsbase.io.PositionedReadable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Corresponds to org.apache.hadoop.hbase.io.hfile.FixedFileTrailer: the last
 * {@value #TRAILER_SIZE} bytes of every HFile.
 *
 * Reading an HFile always starts here: the reader knows the file length, reads the last
 * TRAILER_SIZE bytes and learns from them where the load-on-open section (index, FileInfo,
 * Bloom metadata) begins. Only then can it find any data at all.
 *
 * Original: magic "TRABLK\"$" + protobuf FileTrailerProto, padded to a size fixed per version;
 * the version is in the LAST 4 bytes (so the reader can determine the trailer size). Here:
 * key=value lines, padded with spaces; the last line carries the version.
 */
public class FixedFileTrailer {

    public static final int TRAILER_SIZE = 512;
    public static final int MAJOR_VERSION = 3;
    public static final int MINOR_VERSION = 3;

    long fileInfoOffset;
    long loadOnOpenDataOffset;
    int dataIndexCount;
    int metaIndexCount;
    long totalUncompressedBytes;
    long entryCount;
    String compressionCodec = "NONE";
    int numDataIndexLevels = 1;
    long firstDataBlockOffset = -1;
    long lastDataBlockOffset = -1;
    String comparatorClassName = "edudfsbase.CellComparator";

    byte[] serialize() {
        StringBuilder sb = new StringBuilder();
        sb.append(BlockType.TRAILER.getMagic()).append('\n');
        for (Map.Entry<String, String> e : toMap().entrySet()) {
            sb.append(e.getKey()).append('=').append(e.getValue()).append('\n');
        }
        String version = "majorVersion=" + MAJOR_VERSION + " minorVersion=" + MINOR_VERSION + "\n";
        int pad = TRAILER_SIZE - sb.length() - version.length() - 1;
        sb.append(" ".repeat(pad)).append('\n').append(version);
        return sb.toString().getBytes(StandardCharsets.US_ASCII);
    }

    public Map<String, String> toMap() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("fileInfoOffset", String.valueOf(fileInfoOffset));
        m.put("loadOnOpenDataOffset", String.valueOf(loadOnOpenDataOffset));
        m.put("dataIndexCount", String.valueOf(dataIndexCount));
        m.put("metaIndexCount", String.valueOf(metaIndexCount));
        m.put("totalUncompressedBytes", String.valueOf(totalUncompressedBytes));
        m.put("entryCount", String.valueOf(entryCount));
        m.put("compressionCodec", compressionCodec);
        m.put("numDataIndexLevels", String.valueOf(numDataIndexLevels));
        m.put("firstDataBlockOffset", String.valueOf(firstDataBlockOffset));
        m.put("lastDataBlockOffset", String.valueOf(lastDataBlockOffset));
        m.put("comparatorClassName", comparatorClassName);
        return m;
    }

    public static FixedFileTrailer readFromStream(PositionedReadable in, long fileSize, String name)
            throws IOException {
        if (fileSize < TRAILER_SIZE) {
            throw new IOException("Not an HFile (too short for the trailer): " + name);
        }
        byte[] b = new byte[TRAILER_SIZE];
        in.readFully(fileSize - TRAILER_SIZE, b, 0, TRAILER_SIZE);
        String s = new String(b, StandardCharsets.US_ASCII);
        if (!s.startsWith(BlockType.TRAILER.getMagic() + "\n")) {
            throw new IOException("Not an HFile (trailer magic missing): " + name);
        }
        Map<String, String> m = new LinkedHashMap<>();
        for (String line : s.split("\n")) {
            int i = line.indexOf('=');
            if (i > 0 && !line.startsWith("majorVersion")) {
                m.put(line.substring(0, i), line.substring(i + 1));
            }
        }
        if (!s.endsWith("majorVersion=" + MAJOR_VERSION + " minorVersion=" + MINOR_VERSION + "\n")) {
            throw new IOException("Unsupported HFile version in " + name);
        }
        FixedFileTrailer t = new FixedFileTrailer();
        t.fileInfoOffset = Long.parseLong(m.get("fileInfoOffset"));
        t.loadOnOpenDataOffset = Long.parseLong(m.get("loadOnOpenDataOffset"));
        t.dataIndexCount = Integer.parseInt(m.get("dataIndexCount"));
        t.metaIndexCount = Integer.parseInt(m.get("metaIndexCount"));
        t.totalUncompressedBytes = Long.parseLong(m.get("totalUncompressedBytes"));
        t.entryCount = Long.parseLong(m.get("entryCount"));
        t.compressionCodec = m.get("compressionCodec");
        t.numDataIndexLevels = Integer.parseInt(m.get("numDataIndexLevels"));
        t.firstDataBlockOffset = Long.parseLong(m.get("firstDataBlockOffset"));
        t.lastDataBlockOffset = Long.parseLong(m.get("lastDataBlockOffset"));
        t.comparatorClassName = m.get("comparatorClassName");
        return t;
    }

    public long getFileInfoOffset() {
        return fileInfoOffset;
    }

    public long getLoadOnOpenDataOffset() {
        return loadOnOpenDataOffset;
    }

    public int getDataIndexCount() {
        return dataIndexCount;
    }

    public long getEntryCount() {
        return entryCount;
    }

    public long getFirstDataBlockOffset() {
        return firstDataBlockOffset;
    }

    public long getLastDataBlockOffset() {
        return lastDataBlockOffset;
    }
}
