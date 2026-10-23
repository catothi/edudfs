package edudfsbase.tools;

import edudfsbase.Cell;
import edudfsbase.HRegionLocation;
import edudfsbase.TableName;
import edudfsbase.TableNotFoundException;
import edudfsbase.client.Admin;
import edudfsbase.client.ColumnFamilyDescriptor;
import edudfsbase.client.ColumnFamilyDescriptorBuilder;
import edudfsbase.client.Connection;
import edudfsbase.client.ConnectionFactory;
import edudfsbase.client.Delete;
import edudfsbase.client.Get;
import edudfsbase.client.Put;
import edudfsbase.client.Result;
import edudfsbase.client.ResultScanner;
import edudfsbase.client.Scan;
import edudfsbase.client.Table;
import edudfsbase.client.TableDescriptor;
import edudfsbase.client.TableDescriptorBuilder;
import edudfsbase.regionserver.BloomType;
import edudfsbase.regionserver.compactions.CompactionRecord;
import edudfsbase.util.Bytes;
import edudfs.common.Log;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Corresponds to the HBase shell ({@code hbase shell}, in the original a JRuby REPL, module hbase-shell).
 * Same commands and the same argument syntax (Ruby literals), same output format.
 * <pre>
 *   create 't', 'cf'                          put 't', 'row1', 'cf:a', 'value'
 *   create 't', {NAME => 'cf', VERSIONS => 3}, SPLITS => ['g', 'n']
 *   get 't', 'row1'                           get 't', 'row1', {COLUMN => 'cf:a', VERSIONS => 3}
 *   scan 't'                                  scan 't', {STARTROW => 'a', STOPROW => 'm', RAW => true, VERSIONS => 10}
 *   delete 't', 'row1', 'cf:a'                deleteall 't', 'row1'
 *   flush 't'   compact 't'   major_compact 't'   count 't'   describe 't'   list   status
 *   disable 't'   enable 't'   drop 't'   list_regions 't'   scan 'hbase:meta'
 * </pre>
 * EduDFSBase additions: {@code metasave}, {@code compaction_history}, and the options TRACE =&gt; true
 * (read path of a get/scan) and SOURCE =&gt; true (MemStore or HFile per cell).
 *
 * Modes: interactive; {@code -n} or a pipe: commands from stdin; a file name: run the script;
 * {@code -e "command"}: one command (EduDFSBase).
 */
public class HBaseShell {

    private final Connection conn;
    private final Admin admin;
    private final PrintStream out;

    public HBaseShell(Connection conn, PrintStream out) {
        this.conn = conn;
        this.admin = conn.getAdmin();
        this.out = out;
    }

    public static void main(String[] args) throws Exception {
        Log.quiet = !Boolean.getBoolean("edudfs.verbose");
        Connection conn = ConnectionFactory.createConnection();
        HBaseShell shell = new HBaseShell(conn, System.out);
        if (args.length > 1 && args[0].equals("-e")) {  // EduDFSBase: ./hbase shell -e "command"
            shell.execute(String.join(" ", Arrays.copyOfRange(args, 1, args.length)));
            conn.close();
            System.exit(0);
        }
        boolean interactive = System.console() != null && args.length == 0;
        BufferedReader in = args.length > 0 && !args[0].equals("-n")
                ? new BufferedReader(new FileReader(args[0], StandardCharsets.UTF_8))
                : new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        if (interactive) {
            System.out.println("EduDFSBase shell - teaching implementation modeled on the HBase shell.");
            System.out.println("Type \"help\" for the commands, \"exit\" to leave.\n");
        }
        int n = 1;
        String line;
        while (true) {
            if (interactive) {
                System.out.printf("hbase:%03d:0> ", n++);
                System.out.flush();
            }
            line = in.readLine();
            if (line == null) {
                break;
            }
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            if (!interactive) {
                System.out.println("hbase> " + line);
            }
            if (line.equals("exit") || line.equals("quit")) {
                break;
            }
            shell.execute(line);
        }
        conn.close();
        System.exit(0);
    }

    /** Executes one command line and prints the result (errors as "ERROR: ..."). */
    public void execute(String line) {
        long start = System.nanoTime();
        try {
            List<Object> tokens = new Parser(line).parseCommand();
            String cmd = (String) tokens.remove(0);
            run(cmd, tokens);
            out.printf(Locale.ROOT, "Took %.4f seconds%n", (System.nanoTime() - start) / 1e9);
        } catch (TableNotFoundException e) {
            out.println("ERROR: Unknown table " + e.getMessage() + "!");
        } catch (IOException | RuntimeException e) {
            String msg = e.getMessage() == null ? e.toString() : e.getMessage();
            out.println("ERROR: " + msg.lines().findFirst().orElse(msg));
        }
        out.println();
    }

    // =============================================================== commands
    private void run(String cmd, List<Object> a) throws IOException {
        switch (cmd) {
            case "help" -> help();
            case "status" -> out.print(admin.getClusterStatus(a.size() > 0 && str(a.get(0)).equals("detailed")));
            case "list" -> {
                out.println("TABLE");
                List<String> names = new ArrayList<>();
                for (TableDescriptor d : admin.listTableDescriptors()) {
                    out.println(d.getTableName());
                    names.add("\"" + d.getTableName() + "\"");
                }
                out.println(names.size() + " row(s)");
                out.println("=> " + names);
            }
            case "create" -> create(a);
            case "describe", "desc" -> {
                TableName t = table(a);
                TableDescriptor d = admin.getDescriptor(t);
                out.println("Table " + t + " is " + (admin.isTableEnabled(t) ? "ENABLED" : "DISABLED"));
                out.println(t);
                out.println("COLUMN FAMILIES DESCRIPTION");
                for (ColumnFamilyDescriptor f : d.getColumnFamilies()) {
                    out.println(f);
                }
                out.println(d.getColumnFamilies().size() + " row(s)");
            }
            case "exists" -> {
                TableName t = table(a);
                out.println("Table " + t + (admin.tableExists(t) ? " does exist" : " does not exist"));
            }
            case "is_enabled" -> out.println(admin.isTableEnabled(table(a)));
            case "disable" -> admin.disableTable(table(a));
            case "enable" -> admin.enableTable(table(a));
            case "drop" -> admin.deleteTable(table(a));
            case "put" -> put(a);
            case "get" -> get(a);
            case "scan" -> scan(a);
            case "count" -> {
                int n = 0;
                try (ResultScanner s = conn.getTable(table(a)).getScanner(new Scan())) {
                    while (s.next() != null) {
                        n++;
                    }
                }
                out.println(n + " row(s)");
            }
            case "delete" -> delete(a, false);
            case "deleteall" -> delete(a, true);
            case "flush" -> out.print(admin.flush(table(a)));
            case "compact", "major_compact" -> {
                List<CompactionRecord> recs = cmd.equals("compact") ? admin.compact(table(a))
                        : admin.majorCompact(table(a));
                if (recs.isEmpty()) {
                    out.println(cmd.equals("compact") ? "No minor compaction: no store has enough HFiles"
                            + " (hbase.hstore.compactionThreshold), or the ratio rule selects none."
                            : "Nothing to compact (no HFiles).");
                }
                for (CompactionRecord r : recs) {
                    out.print(r);
                }
            }
            case "compaction_history" -> {
                List<CompactionRecord> recs = admin.getCompactionHistory();
                if (recs.isEmpty()) {
                    out.println("No compactions yet.");
                }
                for (CompactionRecord r : recs) {
                    out.print(r);
                }
            }
            case "list_regions" -> {
                String fmt = " %-36s | %-34s | %-10s | %-10s%n";
                out.printf(fmt, "SERVER_NAME", "ENCODED_REGION_NAME", "START_KEY", "END_KEY");
                List<HRegionLocation> locs = admin.getRegions(table(a));
                for (HRegionLocation l : locs) {
                    out.printf(fmt, l.getServerName() == null ? "(not open)" : l.getServerName(),
                            l.getRegion().getEncodedName(), Bytes.toStringBinary(l.getRegion().getStartKey()),
                            Bytes.toStringBinary(l.getRegion().getEndKey()));
                }
                out.println(locs.size() + " rows");
            }
            case "metasave" -> out.print(admin.metaSave());
            default -> throw new IllegalArgumentException("Unknown command '" + cmd + "'. Try 'help'.");
        }
    }

    private void help() {
        out.println(String.join("\n",
                "COMMAND GROUPS:",
                "  general:  status ['detailed'], list, help, exit",
                "  ddl:      create 't', 'cf1'[, 'cf2'...]",
                "            create 't', {NAME => 'cf', VERSIONS => 3, BLOOMFILTER => 'ROW|ROWCOL|NONE',",
                "                         BLOCKSIZE => 1024, TTL => 60, BLOCKCACHE => true}[, SPLITS => ['g', 'n']]",
                "            describe 't', disable 't', enable 't', drop 't', exists 't', is_enabled 't',",
                "            list_regions 't'",
                "  dml:      put 't', 'row', 'cf:q', 'value'[, ts]",
                "            get 't', 'row'[, 'cf:q' | {COLUMN => 'cf:q' or [...], VERSIONS => n,",
                "                                   TIMERANGE => [min, max]}]",
                "            scan 't'[, {STARTROW => 'a', STOPROW => 'z', COLUMNS => [...], VERSIONS => n,",
                "                        RAW => true, LIMIT => n, TIMERANGE => [min, max]}]",
                "            delete 't', 'row', 'cf:q'[, ts]      (all versions <= ts: DeleteColumn marker)",
                "            deleteall 't', 'row'[, 'cf:q'][, ts] (whole row: DeleteFamily markers)",
                "            count 't'",
                "  tools:    flush 't', compact 't', major_compact 't', scan 'hbase:meta'",
                "  EduDFSBase:  metasave                         state of master and all RegionServers",
                "            compaction_history               the last compactions with dropped cells",
                "            get/scan ... {TRACE => true}     read path: MemStore, Bloom, block index, BlockCache",
                "            get/scan ... {SOURCE => true}    per cell: from the MemStore or from which HFile",
                "Strings: 'single quoted' literally, \"double quoted\" with \\xNN escapes (binary keys)."));
    }

    private void create(List<Object> a) throws IOException {
        TableName t = table(a);
        TableDescriptorBuilder b = TableDescriptorBuilder.newBuilder(t);
        byte[][] splits = null;
        for (int i = 1; i < a.size(); i++) {
            Object o = a.get(i);
            if (o instanceof Str s) {
                b.setColumnFamily(ColumnFamilyDescriptorBuilder.of(s.text()));
            } else if (o instanceof Map<?, ?> m) {
                if (m.containsKey("SPLITS")) {
                    List<?> l = (List<?>) m.get("SPLITS");
                    splits = new byte[l.size()][];
                    for (int k = 0; k < l.size(); k++) {
                        splits[k] = ((Str) l.get(k)).bytes();
                    }
                }
                if (m.containsKey("NAME")) {
                    b.setColumnFamily(family(m));
                }
            }
        }
        admin.createTable(b.build(), splits);
        out.println("Created table " + t);
        out.println("=> Hbase::Table - " + t);
    }

    private static ColumnFamilyDescriptor family(Map<?, ?> m) {
        ColumnFamilyDescriptorBuilder b = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(str(m.get("NAME"))));
        if (m.containsKey("VERSIONS")) {
            b.setMaxVersions((int) num(m.get("VERSIONS")));
        }
        if (m.containsKey("BLOOMFILTER")) {
            b.setBloomFilterType(BloomType.valueOf(str(m.get("BLOOMFILTER")).toUpperCase()));
        }
        if (m.containsKey("BLOCKSIZE")) {
            b.setBlocksize((int) num(m.get("BLOCKSIZE")));
        }
        if (m.containsKey("TTL") && !str(m.get("TTL")).equals("FOREVER")) {
            b.setTimeToLive(num(m.get("TTL")));
        }
        if (m.containsKey("BLOCKCACHE")) {
            b.setBlockCacheEnabled(Boolean.parseBoolean(str(m.get("BLOCKCACHE"))));
        }
        return b.build();
    }

    private void put(List<Object> a) throws IOException {
        byte[][] col = column(str(a.get(2)));
        Put p = a.size() > 4 ? new Put(bytes(a.get(1)), num(a.get(4))) : new Put(bytes(a.get(1)));
        p.addColumn(col[0], col[1], bytes(a.get(3)));
        conn.getTable(table(a)).put(p);
    }

    private void delete(List<Object> a, boolean all) throws IOException {
        Delete d;
        Long ts = null;
        String column = null;
        for (int i = 2; i < a.size(); i++) {
            if (a.get(i) instanceof Long l) {
                ts = l;
            } else {
                column = str(a.get(i));
            }
        }
        d = ts == null ? new Delete(bytes(a.get(1))) : new Delete(bytes(a.get(1)), ts);
        if (column != null) {
            byte[][] col = column(column);
            if (col[1] == null) {
                d.addFamily(col[0]);
            } else {
                d.addColumns(col[0], col[1]);
            }
        } else if (!all) {
            throw new IllegalArgumentException("delete needs a column: delete 't', 'row', 'cf:q' (or use deleteall)");
        }
        conn.getTable(table(a)).delete(d);
    }

    private void get(List<Object> a) throws IOException {
        Table t = conn.getTable(table(a));
        Get g = new Get(bytes(a.get(1)));
        Map<?, ?> opts = Map.of();
        for (int i = 2; i < a.size(); i++) {
            if (a.get(i) instanceof Map<?, ?> m) {
                opts = m;
            } else {
                addColumn(g, null, str(a.get(i)));
            }
        }
        if (opts.containsKey("COLUMN") || opts.containsKey("COLUMNS")) {
            Object c = opts.containsKey("COLUMN") ? opts.get("COLUMN") : opts.get("COLUMNS");
            for (Object o : c instanceof List<?> l ? l : List.of(c)) {
                addColumn(g, null, str(o));
            }
        }
        if (opts.containsKey("VERSIONS")) {
            g.readVersions((int) num(opts.get("VERSIONS")));
        }
        if (opts.containsKey("TIMERANGE")) {
            List<?> tr = (List<?>) opts.get("TIMERANGE");
            g.setTimeRange(num(tr.get(0)), num(tr.get(1)));
        }
        g.setTrace(isTrue(opts.get("TRACE")));
        g.setShowSource(isTrue(opts.get("SOURCE")));
        Result r = t.get(g);
        out.printf("%-28s %s%n", "COLUMN", "CELL");
        Cell[] cells = r.rawCells();
        for (int i = 0; i < cells.length; i++) {
            Cell c = cells[i];
            out.printf(" %-27s %s%n",
                    Bytes.toStringBinary(c.getFamily()) + ":" + Bytes.toStringBinary(c.getQualifier()),
                    formatCell(c, r.getSources() == null ? null : r.getSources().get(i)));
        }
        out.println((r.isEmpty() ? 0 : 1) + " row(s)");
        printTrace(r);
    }

    private void scan(List<Object> a) throws IOException {
        TableName tn = table(a);
        Map<?, ?> opts = a.size() > 1 && a.get(1) instanceof Map<?, ?> m ? m : Map.of();
        List<Result> results = new ArrayList<>();
        if (tn.equals(TableName.META_TABLE_NAME)) {
            results.addAll(admin.scanMeta());
        } else {
            Scan s = new Scan();
            if (opts.containsKey("STARTROW")) {
                s.withStartRow(bytes(opts.get("STARTROW")));
            }
            if (opts.containsKey("STOPROW")) {
                s.withStopRow(bytes(opts.get("STOPROW")));
            }
            Object cols = opts.containsKey("COLUMNS") ? opts.get("COLUMNS") : opts.get("COLUMN");
            if (cols != null) {
                for (Object o : cols instanceof List<?> l ? l : List.of(cols)) {
                    addColumn(null, s, str(o));
                }
            }
            if (opts.containsKey("VERSIONS")) {
                s.readVersions((int) num(opts.get("VERSIONS")));
            }
            if (opts.containsKey("TIMERANGE")) {
                List<?> tr = (List<?>) opts.get("TIMERANGE");
                s.setTimeRange(num(tr.get(0)), num(tr.get(1)));
            }
            if (opts.containsKey("LIMIT")) {
                s.setLimit((int) num(opts.get("LIMIT")));
            }
            s.setRaw(isTrue(opts.get("RAW")));
            s.setTrace(isTrue(opts.get("TRACE")));
            s.setShowSource(isTrue(opts.get("SOURCE")));
            try (ResultScanner rs = conn.getTable(tn).getScanner(s)) {
                Result r;
                while ((r = rs.next()) != null) {
                    results.add(r);
                }
            }
        }
        out.printf("%-28s %s%n", "ROW", "COLUMN+CELL");
        for (Result r : results) {
            Cell[] cells = r.rawCells();
            for (int i = 0; i < cells.length; i++) {
                Cell c = cells[i];
                String source = r.getSources() == null ? null : r.getSources().get(i);
                out.printf(" %-27s column=%s:%s, %s%n", Bytes.toStringBinary(c.getRow()),
                        Bytes.toStringBinary(c.getFamily()), Bytes.toStringBinary(c.getQualifier()),
                        formatCell(c, source));
            }
        }
        out.println(results.size() + " row(s)");
        for (Result r : results) {
            printTrace(r);
        }
    }

    private static String formatCell(Cell c, String source) {
        String s = "timestamp=" + c.getTimestamp() + (c.getType().isDelete() ? ", type=" + c.getType()
                : ", value=" + Bytes.toStringBinary(c.getValue()));
        return source == null ? s : s + ", source=" + source;
    }

    private void printTrace(Result r) {
        if (r.getTrace() != null) {
            out.println("Read path (TRACE):");
            for (String l : r.getTrace()) {
                out.println("  " + l);
            }
        }
    }

    private static void addColumn(Get g, Scan s, String col) {
        byte[][] c = column(col);
        if (c[1] == null) {
            if (g != null) {
                g.addFamily(c[0]);
            } else {
                s.addFamily(c[0]);
            }
        } else if (g != null) {
            g.addColumn(c[0], c[1]);
        } else {
            s.addColumn(c[0], c[1]);
        }
    }

    /** 'cf:q' -&gt; {cf, q}; 'cf' -&gt; {cf, null}. */
    private static byte[][] column(String col) {
        int i = col.indexOf(':');
        return i < 0 ? new byte[][]{Bytes.toBytes(col), null}
                : new byte[][]{Bytes.toBytes(col.substring(0, i)), Bytes.toBytesBinary(col.substring(i + 1))};
    }

    // ================================================================ helpers
    private static TableName table(List<Object> a) {
        if (a.isEmpty()) {
            throw new IllegalArgumentException("Table name missing");
        }
        return TableName.valueOf(str(a.get(0)));
    }

    private static String str(Object o) {
        if (o instanceof Str s) {
            return s.text();
        }
        return String.valueOf(o);
    }

    private static byte[] bytes(Object o) {
        return o instanceof Str s ? s.bytes() : Bytes.toBytes(String.valueOf(o));
    }

    private static long num(Object o) {
        return o instanceof Long l ? l : Long.parseLong(str(o));
    }

    private static boolean isTrue(Object o) {
        return o != null && str(o).equalsIgnoreCase("true");
    }

    /** A string literal: text for names, bytes for keys and values. */
    record Str(String text, byte[] bytes) {
    }

    /**
     * Parses the Ruby-like arguments of the original shell: 'single' and "double" quoted strings,
     * numbers, identifiers (true, false, FOREVER), {KEY =&gt; value, ...}, [a, b] and trailing
     * KEY =&gt; value pairs without braces (as in create 't', 'cf', SPLITS =&gt; [...]).
     */
    static final class Parser {
        private final String s;
        private int pos;

        Parser(String s) {
            this.s = s;
        }

        List<Object> parseCommand() {
            skipWs();
            int start = pos;
            while (pos < s.length() && (Character.isLetterOrDigit(s.charAt(pos)) || s.charAt(pos) == '_')) {
                pos++;
            }
            List<Object> r = new ArrayList<>();
            r.add(s.substring(start, pos));
            Map<String, Object> trailing = null;
            skipWs();
            while (pos < s.length()) {
                Object v = value();
                skipWs();
                if (s.startsWith("=>", pos)) {  // KEY => value without braces
                    pos += 2;
                    if (trailing == null) {
                        trailing = new LinkedHashMap<>();
                        r.add(trailing);
                    }
                    trailing.put(str(v), value());
                } else {
                    r.add(v);
                }
                skipWs();
                if (pos < s.length()) {
                    expect(',');
                }
            }
            return r;
        }

        private Object value() {
            skipWs();
            if (pos >= s.length()) {
                throw new IllegalArgumentException("Unexpected end of input");
            }
            char c = s.charAt(pos);
            if (c == '\'' || c == '"') {
                return string(c);
            }
            if (c == '{') {
                pos++;
                Map<String, Object> m = new LinkedHashMap<>();
                skipWs();
                while (s.charAt(pos) != '}') {
                    Object k = value();
                    skipWs();
                    if (!s.startsWith("=>", pos)) {
                        throw new IllegalArgumentException("Expected => after " + str(k));
                    }
                    pos += 2;
                    m.put(str(k), value());
                    skipWs();
                    if (s.charAt(pos) == ',') {
                        pos++;
                        skipWs();
                    }
                }
                pos++;
                return m;
            }
            if (c == '[') {
                pos++;
                List<Object> l = new ArrayList<>();
                skipWs();
                while (s.charAt(pos) != ']') {
                    l.add(value());
                    skipWs();
                    if (s.charAt(pos) == ',') {
                        pos++;
                        skipWs();
                    }
                }
                pos++;
                return l;
            }
            int start = pos;
            while (pos < s.length()
                    && (Character.isLetterOrDigit(s.charAt(pos)) || "_-.".indexOf(s.charAt(pos)) >= 0)) {
                pos++;
            }
            if (start == pos) {
                throw new IllegalArgumentException("Unexpected '" + c + "' at position " + pos);
            }
            String word = s.substring(start, pos);
            return word.matches("-?\\d+") ? (Object) Long.parseLong(word) : word;
        }

        private Str string(char quote) {
            pos++;
            StringBuilder sb = new StringBuilder();
            while (pos < s.length() && s.charAt(pos) != quote) {
                char c = s.charAt(pos);
                if (c == '\\' && pos + 1 < s.length()) {
                    char n = s.charAt(pos + 1);
                    if (n == quote || n == '\\') {
                        sb.append(n);
                        pos += 2;
                        continue;
                    }
                }
                sb.append(c);
                pos++;
            }
            if (pos >= s.length()) {
                throw new IllegalArgumentException("Unterminated string");
            }
            pos++;
            String text = sb.toString();
            return new Str(text, quote == '"' ? Bytes.toBytesBinary(text) : Bytes.toBytes(text));
        }

        private void skipWs() {
            while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) {
                pos++;
            }
        }

        private void expect(char c) {
            skipWs();
            if (pos >= s.length() || s.charAt(pos) != c) {
                throw new IllegalArgumentException("Expected '" + c + "' at position " + pos + " in: " + s);
            }
            pos++;
        }
    }
}
