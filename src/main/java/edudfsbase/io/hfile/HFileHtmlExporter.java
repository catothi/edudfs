package edudfsbase.io.hfile;

import edudfsbase.codec.KeyValueCodec;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;

/**
 * EduDFSBase: {@code ./hbase hfile --html out.html -f <hfile>} writes a self-contained HTML page (no
 * internet needed) that shows the file graphically:
 * <ul>
 *   <li>the file as a bar, every block proportional to its size, grouped by section,</li>
 *   <li>the block index as a B+ tree: root, intermediate, leaf and data blocks with their edges,</li>
 *   <li>a lookup: type a row key and see the path a get takes (binary search per level),</li>
 *   <li>the raw text of every block with the byte offset of every line - to compare with an editor,</li>
 *   <li>the trailer with what every field means and where its offsets point.</li>
 * </ul>
 */
public class HFileHtmlExporter {

    private final HFileReaderImpl reader;
    private final HFileBlockMap map;

    public HFileHtmlExporter(HFileReaderImpl reader, HFileBlockMap map) {
        this.reader = reader;
        this.map = map;
    }

    public void write(File file) throws IOException {
        Files.writeString(file.toPath(), TEMPLATE.replace("__DATA__", json()), StandardCharsets.UTF_8);
    }

    private String json() throws IOException {
        FixedFileTrailer t = reader.getTrailer();
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"name\":").append(js(reader.getName()));
        sb.append(",\"size\":").append(reader.length());
        sb.append(",\"levels\":").append(t.getNumDataIndexLevels());
        sb.append(",\"cells\":").append(t.getEntryCount());
        sb.append(",\"rootOffset\":").append(t.getLoadOnOpenDataOffset());
        sb.append(",\"indexMaxSize\":").append(edudfsbase.HBaseConfiguration.INDEX_BLOCK_MAX_SIZE);
        sb.append(",\"trailer\":[");
        boolean first = true;
        for (Map.Entry<String, String> e : t.toMap().entrySet()) {
            sb.append(first ? "" : ",").append("[").append(js(e.getKey())).append(",").append(js(e.getValue()))
                    .append("]");
            first = false;
        }
        sb.append("],\"blocks\":[");
        first = true;
        for (HFileBlockMap.Item i : map.getItems()) {
            byte[] b = new byte[i.size];
            reader.getInput().readFully(i.offset, b, 0, i.size);
            sb.append(first ? "" : ",").append("{\"o\":").append(i.offset).append(",\"s\":").append(i.size)
                    .append(",\"t\":").append(js(i.type.name())).append(",\"m\":").append(js(i.type.getMagic()))
                    .append(",\"sec\":").append(js(i.section.title)).append(",\"role\":").append(js(i.role))
                    .append(",\"desc\":").append(js(i.description)).append(",\"no\":").append(i.dataBlockNo)
                    .append(",\"text\":").append(js(new String(b, StandardCharsets.US_ASCII)));
            if (i.block != null && (i.type == BlockType.LEAF_INDEX || i.type == BlockType.INTERMEDIATE_INDEX
                    || (i.type == BlockType.ROOT_INDEX && i.offset == t.getLoadOnOpenDataOffset()))) {
                sb.append(",\"entries\":[");
                boolean f2 = true;
                for (HFileBlockIndex.Entry e : i.block.getIndexEntries()) {
                    sb.append(f2 ? "" : ",").append("[").append(e.getOffset()).append(",").append(e.getOnDiskSize())
                            .append(",").append(js(KeyValueCodec.encodeKey(e.getFirstKey()))).append("]");
                    f2 = false;
                }
                sb.append("]");
            }
            sb.append("}");
            first = false;
        }
        return sb.append("]}").toString();
    }

    /** JSON string literal; "</" is split so that the data cannot end the script element. */
    static String js(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '<' -> sb.append("\\u003c");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    private static final String TEMPLATE = """
            <!doctype html>
            <html lang="en">
            <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>HFile Explorer</title>
            <style>
            :root {
              color-scheme: light dark;
              --bg: #ffffff; --fg: #1d2330; --muted: #5d6677; --line: #d8dde6; --panel: #f5f7fa; --hl: #fff3c4;
              --DATA: #4f7cd6; --LEAF_INDEX: #2f9e6e; --INTERMEDIATE_INDEX: #17a2b8; --ROOT_INDEX: #e07b39;
              --META_INDEX: #9aa3b2; --FILE_INFO: #8a5cc7; --BLOOM_CHUNK: #d0517a; --GENERAL_BLOOM_META: #b7798f;
              --TRAILER: #4a5468; --path: #e5383b;
            }
            @media (prefers-color-scheme: dark) {
              :root { --bg: #14171d; --fg: #e3e7ee; --muted: #9aa4b5; --line: #2c323d; --panel: #1c2028; --hl: #4a3f12;
                --TRAILER: #8691a6; --META_INDEX: #687286; }
            }
            * { box-sizing: border-box; }
            body { margin: 0; padding: 16px; background: var(--bg); color: var(--fg);
              font: 14px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; }
            h1 { font-size: 20px; margin: 0 0 4px; }
            h2 { font-size: 15px; margin: 24px 0 8px; text-transform: uppercase; letter-spacing: .04em; color: var(--muted); }
            code, .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12.5px; }
            .sub { color: var(--muted); word-break: break-all; }
            .legend { display: flex; flex-wrap: wrap; gap: 6px 14px; margin: 8px 0; font-size: 12.5px; }
            .legend span::before { content: ""; display: inline-block; width: 11px; height: 11px; border-radius: 2px;
              margin-right: 5px; vertical-align: -1px; background: var(--c); }
            .sections { display: flex; width: 100%; font-size: 11.5px; color: var(--muted); }
            .sections div { border-left: 1px solid var(--line); padding: 0 4px; overflow: hidden; white-space: nowrap;
              text-overflow: ellipsis; }
            .bar { display: flex; width: 100%; height: 34px; border: 1px solid var(--line); border-radius: 4px;
              overflow: hidden; }
            .bar div { min-width: 2px; border-right: 1px solid var(--bg); cursor: pointer; }
            .bar div.sel, svg .sel { outline: 3px solid var(--fg); outline-offset: -3px; }
            .bar div.onpath { box-shadow: inset 0 -6px 0 var(--path); }
            .treebox { overflow-x: auto; border: 1px solid var(--line); border-radius: 4px; background: var(--panel); }
            svg text { font: 11px ui-monospace, Menlo, monospace; fill: var(--fg); pointer-events: none; }
            svg rect { cursor: pointer; }
            svg line { stroke: var(--line); stroke-width: 1; }
            svg line.onpath { stroke: var(--path); stroke-width: 2.5; }
            svg rect.onpath { stroke: var(--path); stroke-width: 3; }
            .lookup { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin: 10px 0; }
            .lookup input { font: inherit; padding: 5px 8px; border: 1px solid var(--line); border-radius: 4px;
              background: var(--bg); color: var(--fg); min-width: 220px; }
            .lookup button { font: inherit; padding: 5px 12px; border: 1px solid var(--line); border-radius: 4px;
              background: var(--panel); color: var(--fg); cursor: pointer; }
            ol.steps { margin: 6px 0; padding-left: 22px; }
            ol.steps li { margin: 2px 0; }
            .grid { display: grid; grid-template-columns: minmax(0, 1fr); gap: 16px; }
            @media (min-width: 1000px) { .grid { grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); } }
            .panel { border: 1px solid var(--line); border-radius: 4px; background: var(--panel); padding: 12px;
              min-width: 0; }
            table { border-collapse: collapse; width: 100%; }
            td, th { text-align: left; padding: 3px 8px 3px 0; vertical-align: top; border-bottom: 1px solid var(--line); }
            th { color: var(--muted); font-weight: 600; font-size: 12.5px; }
            .raw { overflow-x: auto; background: var(--bg); border: 1px solid var(--line); border-radius: 4px; padding: 6px 0;
              max-height: 520px; overflow-y: auto; }
            .raw div { white-space: pre; padding: 0 8px; }
            .raw div.hl { background: var(--hl); }
            .raw .off { color: var(--muted); display: inline-block; width: 7.5em; text-align: right; margin-right: 1.2em;
              user-select: none; }
            .raw .hdr { color: var(--ROOT_INDEX); }
            a.jump { color: inherit; text-decoration: underline dotted; cursor: pointer; }
            .note { color: var(--muted); font-size: 12.5px; }
            </style>
            </head>
            <body>
            <script>const D = __DATA__;</script>
            <h1>HFile Explorer</h1>
            <div class="sub mono" id="fname"></div>
            <div id="facts" class="note"></div>

            <h2>1 · The file, byte by byte</h2>
            <div class="note">Every block is a box, width proportional to its size. Click a box to see its bytes below.</div>
            <div class="legend" id="legend"></div>
            <div class="sections" id="sections"></div>
            <div class="bar" id="bar"></div>

            <h2>2 · The block index as a B+ tree</h2>
            <div class="note" id="treenote"></div>
            <div class="lookup">
              <label for="row">Look up a row:</label>
              <input id="row" class="mono" placeholder="row key, e.g. 0000001234">
              <button id="go">Find</button>
              <button id="clear">Clear</button>
            </div>
            <ol class="steps mono" id="steps"></ol>
            <div class="treebox"><svg id="tree"></svg></div>

            <div class="grid">
              <div>
                <h2>3 · Selected block</h2>
                <div class="panel" id="detail"><span class="note">Click a block in the file bar or in the tree.</span></div>
              </div>
              <div>
                <h2>4 · Trailer</h2>
                <div class="panel"><table id="trailer"></table>
                <p class="note">The reader opens the file from the end: file size − 512 → trailer → loadOnOpenDataOffset →
                root index, meta index, FileInfo, Bloom meta. Everything else is read on demand.</p></div>
              </div>
            </div>

            <script>
            const COLORS = t => `var(--${t})`;
            const byOff = new Map(D.blocks.map(b => [b.o, b]));
            const typeOf = b => b.t === "ROOT_INDEX" && b.o !== D.rootOffset ? "META_INDEX" : b.t;
            const NL = String.fromCharCode(10);
            const EXPLAIN = {
              fileInfoOffset: "where the FileInfo block is",
              loadOnOpenDataOffset: "start of the load-on-open section = offset of the ROOT index",
              dataIndexCount: "entries in the root level of the data index",
              uncompressedDataIndexSize: "bytes of all index blocks (leaf + intermediate + root)",
              metaIndexCount: "entries of the meta block index (0 = no META blocks)",
              totalUncompressedBytes: "all blocks + trailer before compression (= file size, no compression)",
              entryCount: "number of cells",
              compressionCodec: "block compression",
              numDataIndexLevels: "height of the index tree",
              firstDataBlockOffset: "first data block - a full scan starts here",
              lastDataBlockOffset: "last data block - a scan stops after it",
              comparatorClassName: "sort order of the keys"
            };
            const NAMES = { DATA: "data block", LEAF_INDEX: "leaf index", INTERMEDIATE_INDEX: "intermediate index",
              ROOT_INDEX: "root index", META_INDEX: "meta index", BLOOM_CHUNK: "Bloom chunk", FILE_INFO: "FileInfo",
              GENERAL_BLOOM_META: "Bloom meta", TRAILER: "trailer" };
            const el = id => document.getElementById(id);
            const esc = s => s.replace(/[&<>]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));

            el("fname").textContent = D.name;
            const nData = D.blocks.filter(b => b.t === "DATA").length;
            el("facts").textContent = `${D.size} bytes · ${D.cells} cells · ${nData} data blocks · index height ${D.levels}`
              + ` (numDataIndexLevels) · hfile.index.block.max.size = ${D.indexMaxSize} bytes`;

            // ---- legend, sections, bar
            const present = [...new Set(D.blocks.map(typeOf))];
            el("legend").innerHTML = present.map(t => `<span style="--c:${COLORS(t)}">${NAMES[t]}</span>`).join("");
            const secs = [];
            for (const b of D.blocks) {
              if (!secs.length || secs[secs.length - 1].name !== b.sec) secs.push({ name: b.sec, bytes: 0 });
              secs[secs.length - 1].bytes += b.s;
            }
            el("sections").innerHTML = secs.map(s => `<div style="flex:${s.bytes} 1 0" title="${s.name}">${s.name}</div>`).join("");
            const bar = el("bar");
            for (const b of D.blocks) {
              const d = document.createElement("div");
              d.style.flex = `${b.s} 1 0`;
              d.style.background = COLORS(typeOf(b));
              d.title = `@${b.o}  ${b.m}  ${b.s} bytes\\n${b.desc}`;
              d.dataset.o = b.o;
              d.onclick = () => select(b.o);
              bar.appendChild(d);
            }

            // ---- tree layout: level 0 = root, last level = data blocks
            const levels = [[{ o: D.rootOffset, parent: null }]];
            for (let l = 0; l < D.levels; l++) {
              const next = [];
              for (const n of levels[l]) {
                n.children = (byOff.get(n.o).entries || []).map(e => ({ o: e[0], key: e[2], parent: n }));
                next.push(...n.children);
              }
              levels.push(next);
            }
            const leaves = levels[levels.length - 1];
            const slot = Math.max(7, Math.min(60, 1100 / Math.max(1, leaves.length)));
            const W = Math.max(slot * leaves.length + 40, 600);
            const rowH = 92, topY = 24;
            leaves.forEach((n, i) => { n.x = 20 + i * slot + slot / 2; n.w = Math.max(3, slot - 2); });
            for (let l = levels.length - 2; l >= 0; l--) {
              for (const n of levels[l]) {
                if (!n.children.length) { n.x = 20; n.w = 10; continue; }
                const xs = n.children.map(c => c.x);
                n.x = (Math.min(...xs) + Math.max(...xs)) / 2;
                n.w = Math.max(10, Math.min(Math.max(...xs) - Math.min(...xs) + (n.children[0].w || 6), 150));
              }
            }
            const svg = el("tree");
            const H = topY + rowH * (levels.length - 1) + 50;
            svg.setAttribute("width", W); svg.setAttribute("height", H);
            const ns = "http://www.w3.org/2000/svg";
            const mk = (tag, attrs) => { const e = document.createElementNS(ns, tag);
              for (const k in attrs) e.setAttribute(k, attrs[k]); return e; };
            const levelName = l => l === 0 ? "root" : l === levels.length - 1 ? "data" : l === levels.length - 2 ? "leaf" : "intermediate";
            const nodeH = 22;
            levels.forEach((lv, l) => {
              const y = topY + l * rowH;
              const label = mk("text", { x: 4, y: y - 6 });
              label.textContent = `level ${l + 1}: ${levelName(l)} (${lv.length})`;
              if (l === levels.length - 1) label.textContent = `data blocks (${lv.length})`;
              svg.appendChild(label);
              for (const n of lv) {
                n.y = y;
                if (n.parent) {
                  n.edge = mk("line", { x1: n.parent.x, y1: n.parent.y + nodeH, x2: n.x, y2: y });
                  svg.insertBefore(n.edge, svg.firstChild);
                }
                const b = byOff.get(n.o);
                n.rect = mk("rect", { x: n.x - n.w / 2, y, width: n.w, height: nodeH, rx: 3, fill: COLORS(typeOf(b)) });
                n.rect.appendChild(mk("title", {})).textContent = `@${n.o}  ${b.m}  ${b.desc}` + (n.key ? `\\nfirst key: ${n.key}` : "");
                n.rect.onclick = () => select(n.o);
                svg.appendChild(n.rect);
                if (n.w >= 70) {
                  const t = mk("text", { x: n.x, y: y + 15, "text-anchor": "middle" });
                  t.textContent = `${b.m} @${n.o}`;
                  t.style.fill = "#fff";
                  svg.appendChild(t);
                }
              }
            });
            el("treenote").textContent = `numDataIndexLevels = ${D.levels}: `
              + ["root → data blocks", "root → leaf → data blocks", "root → intermediate → leaf → data blocks"][Math.min(D.levels, 3) - 1]
              + (D.levels > 3 ? " (more intermediate levels)" : "")
              + ". Only the root is in memory after opening the file; every other level costs one block read (or a BlockCache hit).";

            // ---- lookup: binary search per level, as HFileBlockIndex.binarySearch
            const rowOf = key => key.substring(0, key.indexOf("/"));
            function lookup(row) {
              clearPath();
              const steps = [];
              let n = levels[0][0];
              for (let l = 0; l < D.levels; l++) {
                const b = byOff.get(n.o);
                const e = b.entries;
                let lo = 0, hi = e.length - 1, res = 0;
                while (lo <= hi) { const mid = (lo + hi) >> 1; if (rowOf(e[mid][2]) < row) { res = mid; lo = mid + 1; } else hi = mid - 1; }
                const child = n.children[res];
                const kind = l === D.levels - 1 ? "data block" : (l === D.levels - 2 ? "leaf index block" : "intermediate index block");
                steps.push(`level ${l + 1} · ${b.m} @${b.o} (${e.length} entries${l === 0 ? ", in memory" : ", 1 block read"}): `
                  + `binary search → entry #${res}, first key ${e[res][2]} → ${kind} @${child.o}`);
                mark(n); n = child;
              }
              mark(n);
              const db = byOff.get(n.o);
              const found = db.text.split(NL).some(line => line.startsWith("K: " + row + "/"));
              steps.push(`data block @${db.o} (1 block read): ${found ? "row found - the cells are in this block"
                : "row not in this block - so it is not in the file (the Bloom filter would have said so without any read)"}`);
              el("steps").innerHTML = steps.map(s => `<li>${esc(s)}</li>`).join("");
              select(db.o, row);
            }
            function mark(n) {
              n.rect.classList.add("onpath"); if (n.edge) n.edge.classList.add("onpath");
              const d = bar.querySelector(`[data-o="${n.o}"]`); if (d) d.classList.add("onpath");
            }
            function clearPath() {
              document.querySelectorAll(".onpath").forEach(e => e.classList.remove("onpath"));
              el("steps").innerHTML = "";
            }
            el("go").onclick = () => { const r = el("row").value.trim(); if (r) lookup(r); };
            el("row").addEventListener("keydown", e => { if (e.key === "Enter") el("go").click(); });
            el("clear").onclick = () => { clearPath(); el("row").value = ""; };

            // ---- detail panel
            function select(o, row) {
              document.querySelectorAll(".sel").forEach(e => e.classList.remove("sel"));
              const d = bar.querySelector(`[data-o="${o}"]`); if (d) d.classList.add("sel");
              for (const lv of levels) for (const n of lv) if (n.o === o) n.rect.classList.add("sel");
              const b = byOff.get(o);
              const lines = b.text.split(NL);
              let pos = b.o, html = "";
              lines.forEach((line, i) => {
                if (pos >= b.o + b.s) return;
                let shown = esc(line);
                if (/^ {20,}$/.test(line)) shown = `<span class="note">(${line.length} spaces padding)</span>`;
                const m = line.match(/^offset=(\\d+) /);
                if (m && byOff.has(+m[1])) shown = shown.replace(/^offset=(\\d+)/, `offset=<a class="jump" data-j="$1">$1</a>`);
                const hl = row && line.startsWith("K: " + row + "/") ? " hl" : "";
                html += `<div class="${i === 0 && b.t !== "TRAILER" ? "hdr" : ""}${hl}"><span class="off">${pos}</span>${shown}</div>`;
                pos += line.length + 1;
              });
              const head = b.t === "TRAILER" ? "" : (() => {
                const h = lines[0];
                const f = n => (h.match(new RegExp(n + "=\\\\s*(\\\\S+)")) || [])[1];
                return `<tr><th>header</th><td class="mono">magic ${b.m} · onDiskSizeWithoutHeader ${f("onDiskSizeWithoutHeader")}`
                  + ` · prevBlockOffset ${f("prevBlockOffset")} · checksum ${f("checksum")}</td></tr>`;
              })();
              el("detail").innerHTML = `<table>
                <tr><th>block</th><td><b>${b.m}</b> · ${esc(b.role)}</td></tr>
                <tr><th>bytes</th><td class="mono">${b.o} … ${b.o + b.s - 1} (${b.s} bytes)</td></tr>
                <tr><th>section</th><td>${esc(b.sec)}</td></tr>
                <tr><th>content</th><td>${esc(b.desc)}</td></tr>${head}</table>
                <p class="note">Left: byte offset of each line - the same offsets the index and the trailer use.
                Header line first, then the body.</p><div class="raw mono">${html}</div>`;
              el("detail").querySelectorAll("a.jump").forEach(a => a.onclick = () => select(+a.dataset.j));
              const hlLine = el("detail").querySelector(".hl"); if (hlLine) hlLine.scrollIntoView({ block: "nearest" });
            }

            // ---- trailer
            el("trailer").innerHTML = "<tr><th>field</th><th>value</th><th>meaning</th></tr>" + D.trailer.map(([k, v]) => {
              const target = k.endsWith("Offset") && byOff.has(+v)
                ? `<a class="jump" data-j="${v}">${v}</a> → ${byOff.get(+v).m}` : esc(v);
              return `<tr><td class="mono">${k}</td><td class="mono">${target}</td><td>${EXPLAIN[k] || ""}</td></tr>`;
            }).join("");
            el("trailer").querySelectorAll("a.jump").forEach(a => a.onclick = () => select(+a.dataset.j));

            // ---- deep link: out.html#row=<key> runs the lookup, out.html#offset=<n> selects a block
            const h = decodeURIComponent(location.hash.substring(1));
            if (h.startsWith("row=")) { el("row").value = h.substring(4); lookup(h.substring(4)); }
            if (h.startsWith("offset=") && byOff.has(+h.substring(7))) select(+h.substring(7));
            </script>
            </body>
            </html>
            """;
}
