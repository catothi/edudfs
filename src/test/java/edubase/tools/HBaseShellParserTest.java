package edubase.tools;

import edubase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** The Ruby-like argument syntax of the HBase shell. */
class HBaseShellParserTest {

    private static List<Object> parse(String line) {
        return new HBaseShell.Parser(line).parseCommand();
    }

    private static String text(Object o) {
        return ((HBaseShell.Str) o).text();
    }

    @Test
    void simpleCommand() {
        List<Object> t = parse("put 't', 'row1', 'cf:a', 'hello world'");
        assertEquals("put", t.get(0));
        assertEquals("hello world", text(t.get(4)));
    }

    @Test
    void hashesArraysAndTrailingPairs() {
        List<Object> t = parse("create 't', {NAME => 'cf', VERSIONS => 3, BLOOMFILTER => 'ROWCOL'},"
                + " SPLITS => ['g', 'n']");
        Map<?, ?> fam = (Map<?, ?>) t.get(2);
        assertEquals("cf", text(fam.get("NAME")));
        assertEquals(3L, fam.get("VERSIONS"));
        Map<?, ?> trailing = (Map<?, ?>) t.get(3);
        List<?> splits = (List<?>) trailing.get("SPLITS");
        assertEquals(List.of("g", "n"), List.of(text(splits.get(0)), text(splits.get(1))));
    }

    @Test
    void booleansAndNumbers() {
        List<Object> t = parse("scan 't', {RAW => true, VERSIONS => 10, TIMERANGE => [0, 2000]}");
        Map<?, ?> o = (Map<?, ?>) t.get(2);
        assertEquals("true", o.get("RAW"));
        assertInstanceOf(List.class, o.get("TIMERANGE"));
    }

    @Test
    void doubleQuotedStringsAllowBinaryEscapes() {
        List<Object> t = parse("get 't', \"row\\x00\\xff\"");
        assertArrayEquals(new byte[]{'r', 'o', 'w', 0, (byte) 0xff}, ((HBaseShell.Str) t.get(2)).bytes());
        List<Object> s = parse("get 't', 'it\\'s'");
        assertArrayEquals(Bytes.toBytes("it's"), ((HBaseShell.Str) s.get(2)).bytes());
    }
}
