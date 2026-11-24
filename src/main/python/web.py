"""Web UI for the benchmark: ./parquet web [port]  ->  http://localhost:8050/

Serves bench.html and a small JSON API around bench.py. Benchmarks run one at a time (a lock), so that
two browser tabs do not distort each other's timings. Needs ./hdfs minicluster and ./hdfs httpfs.
"""

import json
import sys
import threading
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import bench
import edudfs_fs

LOCK = threading.Lock()
PAGE = Path(__file__).with_name("bench.html")


def meta():
    return {"queries": {k: {"label": q["label"], "sql": q["sql"]} for k, q in bench.QUERIES.items()},
            "storage": bench.STORAGE_PRESETS,
            "setups": {k: {kk: v for kk, v in s.items()} for k, s in bench.SETUPS.items()}}


class Handler(BaseHTTPRequestHandler):

    def send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        try:
            if self.path in ("/", "/index.html"):
                self.send(200, PAGE.read_bytes(), "text/html; charset=utf-8")
            elif self.path == "/api/meta":
                self.send(200, meta())
            elif self.path == "/api/status":
                with LOCK:
                    self.send(200, {k: bench.info(k) for k in bench.SETUPS})
            else:
                self.send(404, {"error": "not found"})
        except Exception as e:  # noqa: BLE001 - report everything to the page
            self.fail(e)

    def do_POST(self):
        try:
            req = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
            with LOCK:
                if self.path == "/api/build":
                    self.send(200, bench.build(req["setup"]))
                elif self.path == "/api/run":
                    if not bench.info(req["setup"])["exists"]:
                        bench.build(req["setup"])
                    self.send(200, bench.run(req["setup"], req["query"], req.get("storage", "local"),
                                             int(req.get("repeat", 3))))
                else:
                    self.send(404, {"error": "not found"})
        except Exception as e:  # noqa: BLE001
            self.fail(e)

    def fail(self, e):
        traceback.print_exc()
        self.send(500, {"error": f"{type(e).__name__}: {e}"})

    def log_message(self, fmt, *args):
        if "/api/run" in self.requestline or "/api/build" in self.requestline:
            sys.stderr.write(f"[web] {self.requestline}\n")


def main(argv):
    port = int(argv[0]) if argv else 8050
    try:
        edudfs_fs.status("/")
    except edudfs_fs.WebHdfsError as e:
        sys.exit(f"{e}\nStart EduDFS first: ./hdfs minicluster 3   and in another terminal: ./hdfs httpfs")
    server = ThreadingHTTPServer(("localhost", port), Handler)
    print(f"Parquet benchmark: http://localhost:{port}/   (Ctrl+C stops)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main(sys.argv[1:])
