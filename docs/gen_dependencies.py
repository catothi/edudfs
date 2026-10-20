#!/usr/bin/env python3
"""
Generates docs/DEPENDENCIES.md: the dependencies of ALL classes of EduDFS.

Sources:
  1. Source code analysis (comments/strings removed, identifiers matched against all project classes)
     - also finds constants that javac inlines (e.g. HdfsConfig.BYTES_PER_CHECKSUM)
  2. jdeps on the bytecode (if out/ exists and jdeps is available)

Usage (in the project directory):  ./hdfs build && python3 docs/gen_dependencies.py
"""
import re
import subprocess
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src" / "main" / "java"
OUT = ROOT / "out"
TARGET = ROOT / "docs" / "DEPENDENCIES.md"
PREFIX = "edudfs."


def strip_comments_and_strings(code: str) -> str:
    code = re.sub(r"/\*.*?\*/", " ", code, flags=re.S)
    code = re.sub(r"//[^\n]*", " ", code)
    code = re.sub(r'"(?:\\.|[^"\\])*"', '""', code)
    code = re.sub(r"'(?:\\.|[^'\\])'", "''", code)
    return code


# ----------------------------------------------------------------- read classes
classes = {}  # fqcn -> info
for f in sorted(SRC.rglob("*.java")):
    text = f.read_text(encoding="utf-8")
    pkg = re.search(r"^package\s+([\w.]+);", text, re.M).group(1)
    name = f.stem
    fq = f"{pkg}.{name}"
    code = strip_comments_and_strings(text)

    decl = re.search(
        r"^(public\s+)?((?:abstract|final)\s+)*(class|interface|enum)\s+" + name +
        r"\b([^{]*)\{", code, re.M)
    kind = decl.group(3)
    if kind == "class" and decl.group(2) and "abstract" in decl.group(2):
        kind = "abstract class"
    header = decl.group(4)
    ext = re.search(r"\bextends\s+([\w.<>, ]+?)(?=\bimplements\b|$)", header)
    impl = re.search(r"\bimplements\s+([\w.<>, ]+)", header)
    parents = []
    for grp in (ext, impl):
        if grp:
            parents += [p.strip().split("<")[0] for p in grp.group(1).split(",") if p.strip()]

    # Purpose: first paragraph of the Javadoc directly before the type declaration
    doc = ""
    m = re.search(r"/\*\*(.*?)\*/\s*(?:@\w+\s*)*(?:public\s+)?(?:(?:abstract|final)\s+)*(?:class|interface|enum)\s+"
                  + name + r"\b", text, re.S)
    if m:
        lines = [ln.strip().lstrip("*").strip() for ln in m.group(1).splitlines()]
        para = []
        for ln in lines:
            if not ln:
                if para:
                    break
                continue
            if ln.startswith("<") or ln.startswith("@"):
                break
            para.append(ln)
        doc = re.sub(r"\{@\w+\s+([^}]*)\}", r"\1", " ".join(para))
        doc = doc.replace("|", "\\|")
        if len(doc) > 170:
            doc = doc[:167].rsplit(" ", 1)[0] + " …"

    nested = set(re.findall(r"\b(?:class|interface|enum|record)\s+(\w+)", code)) - {name}
    classes[fq] = dict(pkg=pkg, name=name, kind=kind, parents=parents, doc=doc,
                       code=code, nested=nested, lines=text.count("\n"))

simple_to_fq = {c["name"]: fq for fq, c in classes.items()}
nested_to_fq = {}
for fq, c in classes.items():
    for n in c["nested"]:
        nested_to_fq.setdefault(n, fq)


def resolve(simple, own_fq):
    if simple in simple_to_fq:
        return simple_to_fq[simple]
    if simple in nested_to_fq and nested_to_fq[simple] != own_fq:
        return nested_to_fq[simple]
    return None


# ------------------------------------------------------------ dependencies
deps = defaultdict(set)
for fq, c in classes.items():
    for tok in set(re.findall(r"\b[A-Z]\w*\b", c["code"])):
        if tok in c["nested"]:
            continue  # own inner class beats a foreign one with the same name (not needed, but safe)
        target = resolve(tok, fq)
        if target and target != fq:
            deps[fq].add(target)

jdeps_used = False
if OUT.is_dir():
    try:
        raw = subprocess.run(["jdeps", "-verbose:class", "-filter:none", str(OUT)],
                             capture_output=True, text=True, check=True).stdout
        for line in raw.splitlines():
            parts = line.split()
            if len(parts) >= 3 and parts[1] == "->" and parts[0].startswith(PREFIX) and parts[2].startswith(PREFIX):
                a, b = parts[0].split("$")[0], parts[2].split("$")[0]
                if a != b and a in classes and b in classes:
                    deps[a].add(b)
        jdeps_used = True
    except (OSError, subprocess.CalledProcessError):
        pass

used_by = defaultdict(set)
for a, bs in deps.items():
    for b in bs:
        used_by[b].add(a)

for fq, c in classes.items():
    c["parents_fq"] = [p for p in (resolve(x.split(".")[-1], fq) for x in c["parents"]) if p]
    c["parents_ext"] = [x for x in c["parents"] if not resolve(x.split(".")[-1], fq)]


def short_pkg(pkg):
    return pkg[len(PREFIX):] if pkg.startswith(PREFIX) else pkg


def node_id(fq):
    return fq[len(PREFIX):].replace(".", "_")


packages = sorted({c["pkg"] for c in classes.values()})
pkg_edges = defaultdict(int)
for a, bs in deps.items():
    for b in bs:
        pa, pb = classes[a]["pkg"], classes[b]["pkg"]
        if pa != pb:
            pkg_edges[(pa, pb)] += 1

# ------------------------------------------------------------ build Markdown
md = []
w = md.append
w("# EduDFS – Dependencies of All Classes\n")
w("> Generated automatically by `docs/gen_dependencies.py` – do not edit by hand.  ")
w("> Regenerate: `./hdfs build && python3 docs/gen_dependencies.py`\n")
w(f"**{len(classes)} classes** in **{len(packages)} packages**, "
  f"**{sum(len(v) for v in deps.values())} class dependencies**. "
  f"Sources: source code analysis{' + `jdeps` (Bytecode)' if jdeps_used else ''}. "
  "Inner classes (e.g. `LeaseManager.Lease`, `DFSOutputStream.DataStreamer`) are attributed to their outer "
  "class. Package names are given without the prefix `edudfs.`.\n")
w("**How to read:** “A → B” means *A uses B* (import, field, parameter, call, inheritance or constant).\n")

w("## Contents\n")
w("1. [Package dependencies](#1-package-dependencies)")
w("2. [Inheritance and interfaces](#2-inheritance-and-interfaces)")
w("3. [Class graphs per package](#3-class-graphs-per-package)")
w("4. [All classes in detail](#4-all-classes-in-detail)")
w("5. [Metrics](#5-metrics)\n")

# 1. packages
w("## 1. Package dependencies\n")
w("```mermaid\nflowchart LR")
for p in packages:
    n = sum(1 for c in classes.values() if c["pkg"] == p)
    w(f'    {node_id(p)}["{short_pkg(p)}<br/>({n} classes)"]')
for (pa, pb), n in sorted(pkg_edges.items()):
    w(f"    {node_id(pa)} -- {n} --> {node_id(pb)}")
w("```\n")
w("The number on an edge is the number of class dependencies between the two packages.\n")

w("### Matrix (row uses column)\n")
w("| uses → | " + " | ".join(f"`{short_pkg(p)}`" for p in packages) + " |")
w("|---|" + "---|" * len(packages))
for pa in packages:
    row = []
    for pb in packages:
        row.append("·" if pa == pb else (str(pkg_edges[(pa, pb)]) if pkg_edges.get((pa, pb)) else ""))
    w(f"| `{short_pkg(pa)}` | " + " | ".join(row) + " |")
w("")

cycles = sorted({tuple(sorted((a, b))) for (a, b) in pkg_edges if (b, a) in pkg_edges})
w("### Cycles between packages\n")
if cycles:
    for a, b in cycles:
        w(f"* `{short_pkg(a)}` ⇄ `{short_pkg(b)}` ({pkg_edges[(a, b)]} / {pkg_edges[(b, a)]} dependencies)")
    w("\nThese cycles exist in exactly the same way in the Hadoop original: `FSNamesystem` owns the `BlockManager`, and the "
      "`BlockManager` needs the lock or the file (`INodeFile`) of a block via the FSNamesystem.\n")
else:
    w("None.\n")

w("### Layering\n")
w("* `common` and `protocol` depend on no server or client package – they are the shared foundation.")
client_srv = [k for k in pkg_edges if short_pkg(k[0]) == "client" and short_pkg(k[1]).startswith("server")]
w("* `client` does " + ("**not** " if not client_srv else "") +
  "depend on `server.*` – client and server share only the protocol classes.")
dn_nn = [k for k in pkg_edges if short_pkg(k[0]) == "server.datanode"
         and short_pkg(k[1]) in ("server.namenode", "server.blockmanagement")]
w("* `server.datanode` does " + ("**not** " if not dn_nn else "") +
  "depend on `server.namenode`/`server.blockmanagement` – DataNode and NameNode know each other only via "
  "`server.protocol`.")
w("* `tools` sits on top and uses everything.\n")

# 2. inheritance
w("## 2. Inheritance and interfaces\n")
w("```mermaid\nclassDiagram")
for fq, c in sorted(classes.items()):
    for p in c["parents_fq"]:
        rel = "<|.." if classes[p]["kind"] == "interface" and c["kind"] != "interface" else "<|--"
        w(f"    {classes[p]['name']} {rel} {c['name']}")
    for p in c["parents_ext"]:
        rel = "<|.." if p in ("Remote", "Runnable", "Closeable", "Serializable", "Comparable") else "<|--"
        w(f"    {p} {rel} {c['name']}")
w("```\n")
w("| Class | extends / implements |")
w("|---|---|")
for fq, c in sorted(classes.items(), key=lambda kv: kv[1]["name"]):
    if c["parents"]:
        w(f"| `{c['name']}` | " + ", ".join(f"`{p}`" for p in c["parents"]) + " |")
w("")

# 3. per package
w("## 3. Class graphs per package\n")
w("Per package: all classes of the package, their dependencies among each other (solid) and on classes "
  "of other packages (dashed, grouped by package).\n")
for p in packages:
    members = sorted(fq for fq, c in classes.items() if c["pkg"] == p)
    w(f"### `{short_pkg(p)}`\n")
    w("```mermaid\nflowchart LR")
    w(f'    subgraph {node_id(p)}_box["{short_pkg(p)}"]')
    for fq in members:
        w(f'        {node_id(fq)}["{classes[fq]["name"]}"]')
    w("    end")
    ext = defaultdict(set)
    lines = []
    for fq in members:
        for d in sorted(deps.get(fq, ())):
            if classes[d]["pkg"] == p:
                lines.append(f"    {node_id(fq)} --> {node_id(d)}")
            else:
                ext[classes[d]["pkg"]].add(d)
                lines.append(f"    {node_id(fq)} -.-> {node_id(d)}")
    for ep in sorted(ext):
        w(f'    subgraph {node_id(ep)}_ext_{node_id(p)}["{short_pkg(ep)}"]')
        for d in sorted(ext[ep]):
            w(f'        {node_id(d)}["{classes[d]["name"]}"]')
        w("    end")
    md.extend(lines)
    w("```\n")

# 4. detail
w("## 4. All classes in detail\n")
w("Columns: **uses** = outgoing dependencies (fan-out), **used by** = incoming (fan-in).\n")
for p in packages:
    w(f"### Package `{short_pkg(p)}`\n")
    for fq in sorted(fq for fq, c in classes.items() if c["pkg"] == p):
        c = classes[fq]
        w(f"#### `{c['name']}`\n")
        rel = fq[len(PREFIX):].replace(".", "/")
        w(f"*{c['kind']}* · [`src/main/java/edudfs/{rel}.java`](../src/main/java/edudfs/{rel}.java) · "
          f"{c['lines']} lines\n")
        if c["doc"]:
            w(f"{c['doc']}\n")
        if c["parents"]:
            w("**extends / implements:** " + ", ".join(f"`{x}`" for x in c["parents"]) + "\n")
        if c["nested"]:
            w("**inner types:** " + ", ".join(f"`{x}`" for x in sorted(c["nested"])) + "\n")

        def grouped(names):
            g = defaultdict(list)
            for d in names:
                g[classes[d]["pkg"]].append(classes[d]["name"])
            return "<br/>".join(f"*{short_pkg(k)}:* " + ", ".join(f"`{n}`" for n in sorted(v))
                                for k, v in sorted(g.items())) or "–"

        w("| uses (" + str(len(deps.get(fq, ()))) + ") | used by (" + str(len(used_by.get(fq, ()))) + ") |")
        w("|---|---|")
        w(f"| {grouped(deps.get(fq, ()))} | {grouped(used_by.get(fq, ()))} |\n")

# 5. metrics
w("## 5. Metrics\n")
w("### Most used classes (fan-in)\n")
w("| Class | Package | used by |")
w("|---|---|---|")
for fq in sorted(classes, key=lambda x: (-len(used_by.get(x, ())), classes[x]["name"]))[:15]:
    w(f"| `{classes[fq]['name']}` | `{short_pkg(classes[fq]['pkg'])}` | {len(used_by.get(fq, ()))} |")
w("\n### Classes with the most dependencies (fan-out)\n")
w("| Class | Package | uses |")
w("|---|---|---|")
for fq in sorted(classes, key=lambda x: (-len(deps.get(x, ())), classes[x]["name"]))[:15]:
    w(f"| `{classes[fq]['name']}` | `{short_pkg(classes[fq]['pkg'])}` | {len(deps.get(fq, ()))} |")
w("\n### Package sizes\n")
w("| Package | Classes | Lines |")
w("|---|---|---|")
for p in packages:
    cs = [c for c in classes.values() if c["pkg"] == p]
    w(f"| `{short_pkg(p)}` | {len(cs)} | {sum(c['lines'] for c in cs)} |")
w("")

TARGET.write_text("\n".join(md) + "\n", encoding="utf-8")
print(f"{TARGET.relative_to(ROOT)}: {len(classes)} classes, {sum(len(v) for v in deps.values())} dependencies,"
      f" jdeps={'yes' if jdeps_used else 'no'}")
