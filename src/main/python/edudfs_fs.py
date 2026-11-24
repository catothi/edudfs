"""EduDFS as a pyarrow file system - over the WebHDFS REST API of ./hdfs httpfs.

pyarrow normally talks to HDFS through libhdfs (JNI + the Hadoop jars). EduDFS has its own
protocol, so here pyarrow gets a small Python file system instead (pyarrow.fs.PyFileSystem) that
sends every read as an HTTP range request: GET /webhdfs/v1/<path>?op=OPEN&offset=..&length=..

There is deliberately NO read-ahead and NO cache: every byte the Parquet reader asks for becomes
exactly one request, and every request is recorded in IO. That is what makes pushdown visible -
"read 312 KB of 6.1 MB" - and the HttpFS terminal shows the same requests with their HDFS blocks.
"""

import io
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

import pyarrow as pa
import pyarrow.fs as pafs

BASE_URL = os.environ.get("EDUDFS_HTTPFS", "http://localhost:14000/webhdfs/v1")

# Simulated storage (used by the benchmark): every request waits latency_ms, every byte costs 1/mb_per_s.
# On localhost a request costs well under a millisecond; a real HDFS cluster or an object store does not.
STORAGE = {"latency_ms": 0.0, "mb_per_s": 0.0}


# --------------------------------------------------------------------------- I/O statistics
class IOStats:
    """All byte ranges read since the last reset(), per file."""

    def __init__(self):
        self.reads = []          # (path, offset, length)
        self.meta = 0            # metadata requests (GETFILESTATUS, LISTSTATUS)

    def reset(self):
        self.reads.clear()
        self.meta = 0

    def record(self, path, offset, length):
        self.reads.append((path, offset, length))

    @property
    def bytes_read(self):
        return sum(n for _, _, n in self.reads)

    @property
    def requests(self):
        return len(self.reads)

    @property
    def files(self):
        return sorted({p for p, _, _ in self.reads})

    def ranges(self, path):
        return [(o, n) for p, o, n in self.reads if p == path]


IO = IOStats()


# --------------------------------------------------------------------------- WebHDFS client
class WebHdfsError(IOError):
    pass


def _url(path, op, **params):
    path = "/" + path.lstrip("/")
    params = {"op": op, **{k: v for k, v in params.items() if v is not None}}
    return BASE_URL + urllib.parse.quote(path) + "?" + urllib.parse.urlencode(params)


def _call(method, path, op, data=None, **params):
    req = urllib.request.Request(_url(path, op, **params), data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/octet-stream")
    try:
        with urllib.request.urlopen(req) as resp:
            body = resp.read()
        if STORAGE["latency_ms"] or STORAGE["mb_per_s"]:
            time.sleep(STORAGE["latency_ms"] / 1000
                       + (len(body) / (STORAGE["mb_per_s"] * 1048576) if STORAGE["mb_per_s"] else 0))
        return body
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        try:
            remote = json.loads(body)["RemoteException"]
            msg = f"{remote['exception']}: {remote['message']}"
        except (ValueError, KeyError):
            msg = body or str(e)
        if e.code == 404:
            raise FileNotFoundError(msg) from None
        raise WebHdfsError(msg) from None
    except urllib.error.URLError as e:
        raise WebHdfsError(f"HttpFS not reachable at {BASE_URL} ({e.reason}) - start it with ./hdfs httpfs") from None


def status(path):
    """FileStatus dict (type, length, blockSize, replication, ...) or None if the path does not exist."""
    IO.meta += 1
    try:
        return json.loads(_call("GET", path, "GETFILESTATUS"))["FileStatus"]
    except FileNotFoundError:
        return None


def listdir(path):
    IO.meta += 1
    return json.loads(_call("GET", path, "LISTSTATUS"))["FileStatuses"]["FileStatus"]


def read_range(path, offset, length):
    data = _call("GET", path, "OPEN", offset=offset, length=length)
    IO.record("/" + path.lstrip("/"), offset, len(data))
    return data


def read_all(path):
    """Whole file without recording it in IO (for tools that look at the file, not for queries)."""
    return _call("GET", path, "OPEN")


def write(path, data, overwrite=True):
    _call("PUT", path, "CREATE", data=data, overwrite=str(overwrite).lower())


def mkdirs(path):
    _call("PUT", path, "MKDIRS")


def delete(path, recursive=False):
    return json.loads(_call("DELETE", path, "DELETE", recursive=str(recursive).lower()))["boolean"]


def block_locations(path):
    """[{offset, length, block, names}] - the HDFS blocks of a file, in file order."""
    return json.loads(_call("GET", path, "GETFILEBLOCKLOCATIONS"))["BlockLocations"]["BlockLocation"]


# --------------------------------------------------------------------------- file objects
class RangeReader:
    """Seekable, read-only file: every read() becomes one op=OPEN request for exactly that range."""

    def __init__(self, path, size):
        self.path, self.size_, self.pos, self.closed = path, size, 0, False

    def readable(self):
        return True

    def seekable(self):
        return True

    def writable(self):
        return False

    def tell(self):
        return self.pos

    def size(self):
        return self.size_

    def seek(self, offset, whence=0):
        self.pos = {0: offset, 1: self.pos + offset, 2: self.size_ + offset}[whence]
        return self.pos

    def read(self, n=-1):
        if n is None or n < 0:
            n = self.size_ - self.pos
        n = max(0, min(n, self.size_ - self.pos))
        if n == 0:
            return b""
        data = read_range(self.path, self.pos, n)
        self.pos += len(data)
        return data

    def close(self):
        self.closed = True


class BufferedWriter(io.BytesIO):
    """Collects the whole file in memory and sends it with one op=CREATE on close()."""

    def __init__(self, path):
        super().__init__()
        self.path = path

    def close(self):
        if not self.closed:
            write(self.path, self.getvalue())
        super().close()


# --------------------------------------------------------------------------- pyarrow file system
class EduDfsHandler(pafs.FileSystemHandler):
    """pyarrow.fs.FileSystemHandler for EduDFS; use it via edudfs_filesystem()."""

    def get_type_name(self):
        return "edudfs"

    def equals(self, other):
        return isinstance(other, EduDfsHandler)

    def normalize_path(self, path):
        return "/" + path.lstrip("/")

    def _info(self, path, st):
        if st is None:
            return pafs.FileInfo(path, pafs.FileType.NotFound)
        if st["type"] == "DIRECTORY":
            return pafs.FileInfo(path, pafs.FileType.Directory)
        return pafs.FileInfo(path, pafs.FileType.File, size=st["length"],
                             mtime_ns=st["modificationTime"] * 1_000_000)

    def get_file_info(self, paths):
        return [self._info(p, status(p)) for p in paths]

    def get_file_info_selector(self, selector):
        base = self.normalize_path(selector.base_dir).rstrip("/")
        try:
            return self._list(base, selector.recursive)
        except FileNotFoundError:
            if selector.allow_not_found:
                return []
            raise

    def _list(self, base, recursive):
        """One LISTSTATUS per directory, like DistributedFileSystem.listStatus."""
        result = []
        for child in listdir(base or "/"):
            p = f"{base}/{child['pathSuffix']}"
            result.append(self._info(p, child))
            if recursive and child["type"] == "DIRECTORY":
                result += self._list(p, True)
        return result

    def create_dir(self, path, recursive):
        mkdirs(path)

    def delete_dir(self, path):
        delete(path, recursive=True)

    def delete_dir_contents(self, path, missing_dir_ok=False):
        if status(path) is None:
            if missing_dir_ok:
                return
            raise FileNotFoundError(path)
        for child in listdir(path):
            delete(f"{path.rstrip('/')}/{child['pathSuffix']}", recursive=True)

    def delete_root_dir_contents(self):
        self.delete_dir_contents("/")

    def delete_file(self, path):
        delete(path)

    def move(self, src, dest):
        raise NotImplementedError("move is not needed for the demo")

    def copy_file(self, src, dest):
        write(dest, read_all(src))

    def open_input_stream(self, path):
        return self.open_input_file(path)

    def open_input_file(self, path):
        st = status(path)
        if st is None:
            raise FileNotFoundError(path)
        return pa.PythonFile(RangeReader(self.normalize_path(path), st["length"]), mode="r")

    def open_output_stream(self, path, metadata):
        return pa.PythonFile(BufferedWriter(self.normalize_path(path)), mode="w")

    def open_append_stream(self, path, metadata):
        raise NotImplementedError("append is not needed for the demo")


def edudfs_filesystem():
    return pafs.PyFileSystem(EduDfsHandler())
