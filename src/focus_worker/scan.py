"""Locate billing export files and stream their record batches.

Paths may be ``sample://<name>`` (synthetic exports bundled with the worker, so
examples run anywhere), a local file, a directory (read recursively), a glob (``*``, ``?``,
``**``), an ``http(s)://`` URL (read with HTTP range requests, so a Parquet file
is never downloaded whole), or an ``s3://`` URI / glob (pyarrow's S3 client,
credentials from a DuckDB ``s3`` secret scoped to the path; ``gs://`` works the
same way through a ``gcs`` HMAC secret, using GCS's S3-compatible endpoint).

Files are scanned one *unit* at a time: a Parquet row group, or a whole CSV /
JSON file. A scan position is ``(file, unit, batch)``, which is what the table
functions keep in their serializable cursor, so a scan resumes anywhere, in any
worker process, without holding a file open between calls. Within one process a
small cache keeps the open batch iterator so sequential reads do not re-open.
"""

from __future__ import annotations

import fnmatch
import glob as _glob
import io
import os
import re
import threading
import urllib.error
import urllib.request
from collections import OrderedDict
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, cast

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.json as pajson

FORMATS = ("auto", "parquet", "csv", "json")
BATCH_ROWS = 65_536
HTTP_TIMEOUT = 60.0
USER_AGENT = "vgi-focus-worker"
_PARQUET_EXT = (".parquet", ".parq", ".pq")
_CSV_EXT = (".csv", ".csv.gz", ".csv.zst", ".csv.bz2", ".tsv", ".csv.zip")
_JSON_EXT = (".json", ".jsonl", ".ndjson", ".json.gz", ".jsonl.gz", ".ndjson.gz")
_KNOWN_EXT = _PARQUET_EXT + _CSV_EXT + _JSON_EXT


class ScanError(ValueError):
    """The path could not be resolved or read."""


# --------------------------------------------------------------------- HTTP


class _HttpRangeReader(io.RawIOBase):
    """A seekable, read-only file over HTTP Range requests."""

    def __init__(self, url: str, size: int) -> None:
        self.url = url
        self.size = size
        self.pos = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self.pos, io.SEEK_END: self.size}[whence]
        self.pos = max(0, base + offset)
        return self.pos

    def readinto(self, b: Any) -> int:
        want = len(b)
        if want == 0 or self.pos >= self.size:
            return 0
        end = min(self.size, self.pos + want) - 1
        data = _http_get(self.url, {"Range": f"bytes={self.pos}-{end}"})
        n = len(data)
        b[:n] = data
        self.pos += n
        return n


def _http_get(url: str, headers: dict[str, str]) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, **headers})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            data: bytes = resp.read()
            return data
    except urllib.error.HTTPError as err:
        raise ScanError(f"GET {url} failed with HTTP {err.code}") from err
    except urllib.error.URLError as err:
        raise ScanError(f"GET {url} failed: {err.reason}") from err


def _http_size(url: str) -> int:
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            length = resp.headers.get("Content-Length")
    except urllib.error.HTTPError as err:
        raise ScanError(f"HEAD {url} failed with HTTP {err.code}") from err
    except urllib.error.URLError as err:
        raise ScanError(f"HEAD {url} failed: {err.reason}") from err
    if length is None:
        raise ScanError(f"HEAD {url}: the server sent no Content-Length, so the file cannot be range-read")
    return int(length)


class _HttpHandler(pafs.FileSystemHandler):
    """Minimal read-only pyarrow filesystem whose paths are full http(s) URLs."""

    def __init__(self) -> None:
        self._sizes: dict[str, int] = {}

    def _size(self, url: str) -> int:
        if url not in self._sizes:
            self._sizes[url] = _http_size(url)
        return self._sizes[url]

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _HttpHandler)

    def __ne__(self, other: object) -> bool:
        return not isinstance(other, _HttpHandler)

    def get_type_name(self) -> str:
        return "vgi-http"

    def normalize_path(self, path: str) -> str:
        return path

    def get_file_info(self, paths: Any) -> list[pafs.FileInfo]:
        return [pafs.FileInfo(p, pafs.FileType.File, size=self._size(p)) for p in paths]

    def get_file_info_selector(self, selector: pafs.FileSelector) -> list[pafs.FileInfo]:
        raise ScanError("listing is not possible over HTTP; pass one URL per call")

    def open_input_file(self, path: str) -> pa.NativeFile:
        return pa.PythonFile(io.BufferedReader(_HttpRangeReader(path, self._size(path)), buffer_size=1 << 20), mode="r")

    def open_input_stream(self, path: str) -> pa.NativeFile:
        return self.open_input_file(path)

    def create_dir(self, path: str, recursive: bool) -> None:
        raise ScanError("read-only")

    def delete_dir(self, path: str) -> None:
        raise ScanError("read-only")

    def delete_dir_contents(self, path: str, missing_dir_ok: bool = False) -> None:
        raise ScanError("read-only")

    def delete_root_dir_contents(self) -> None:
        raise ScanError("read-only")

    def delete_file(self, path: str) -> None:
        raise ScanError("read-only")

    def move(self, src: str, dest: str) -> None:
        raise ScanError("read-only")

    def copy_file(self, src: str, dest: str) -> None:
        raise ScanError("read-only")

    def open_output_stream(self, path: str, metadata: Any = None) -> pa.NativeFile:
        raise ScanError("read-only")

    def open_append_stream(self, path: str, metadata: Any = None) -> pa.NativeFile:
        raise ScanError("read-only")


# ---------------------------------------------------------------- resolution


@dataclass(frozen=True)
class ObjectStoreConfig:
    """Connection settings for s3:// (and gs:// via HMAC), from a DuckDB secret."""

    key_id: str | None = None
    secret: str | None = None
    session_token: str | None = None
    region: str | None = None
    endpoint: str | None = None
    use_ssl: bool = True
    url_style: str | None = None

    def filesystem(self, scheme: str) -> pafs.FileSystem:
        """Return a pyarrow S3 filesystem for these settings."""
        endpoint = self.endpoint
        if scheme == "gs" and not endpoint:
            endpoint = "storage.googleapis.com"
        kwargs: dict[str, Any] = {"region": self.region or "us-east-1"}
        if self.key_id:
            kwargs.update(access_key=self.key_id, secret_key=self.secret or "")
            if self.session_token:
                kwargs["session_token"] = self.session_token
        else:
            kwargs["anonymous"] = True
        if endpoint:
            kwargs["endpoint_override"] = endpoint
            kwargs["scheme"] = "https" if self.use_ssl else "http"
        if self.url_style == "path":
            kwargs["force_virtual_addressing"] = False
        return pafs.S3FileSystem(**kwargs)


def object_store_config(secret: dict[str, Any] | None) -> ObjectStoreConfig:
    """Read the fields of a DuckDB ``s3`` / ``gcs`` / ``r2`` secret."""
    if not secret:
        return ObjectStoreConfig()

    def field(name: str) -> Any:
        v = secret.get(name)
        if v is None:
            return None
        v = v.as_py() if hasattr(v, "as_py") else v
        return v if v not in ("",) else None

    use_ssl = field("use_ssl")
    return ObjectStoreConfig(
        key_id=field("key_id"),
        secret=field("secret"),
        session_token=field("session_token"),
        region=field("region"),
        endpoint=field("endpoint"),
        use_ssl=True if use_ssl is None else str(use_ssl).lower() in ("true", "1"),
        url_style=field("url_style"),
    )


@dataclass(frozen=True)
class FileSet:
    """The files a path names, on one filesystem, in a stable order."""

    filesystem: pafs.FileSystem
    files: tuple[str, ...]  # filesystem paths
    uris: tuple[str, ...]  # what the user would recognise (for x_SourceFile)
    formats: tuple[str, ...]


def file_format(path: str, fmt: str) -> str:
    """'parquet' | 'csv' | 'json' from ``fmt`` or the file extension."""
    if fmt != "auto":
        return fmt
    low = path.lower().split("?", 1)[0]
    if low.endswith(_PARQUET_EXT) or ".parquet" in low.rsplit("/", 1)[-1]:
        return "parquet"
    if low.endswith(_JSON_EXT):
        return "json"
    if low.endswith(_CSV_EXT):
        return "csv"
    raise ScanError(f"cannot tell the format of {path!r} from its extension; pass format := 'parquet' | 'csv' | 'json'")


def scheme_of(path: str) -> str:
    """URI scheme ('' for local paths)."""
    m = re.match(r"^([a-zA-Z][a-zA-Z0-9+.-]*)://", path)
    return m.group(1).lower() if m else ""


def _has_glob(path: str) -> bool:
    return any(ch in path for ch in "*?[")


def _wanted(name: str, fmt: str) -> bool:
    base = name.replace("\\", "/").lower().rsplit("/", 1)[-1]
    if base.startswith((".", "_")):
        return False
    if "manifest" in base and base.endswith(".json"):
        return False  # AWS/Azure export manifests sit next to the data files
    if fmt != "auto":
        return True
    return base.endswith(_KNOWN_EXT) or ".parquet" in base


def resolve(path: str, fmt: str = "auto", store: ObjectStoreConfig | None = None) -> FileSet:
    """Expand ``path`` into the files to scan."""
    path = path.strip()
    if not path:
        raise ScanError("path is empty")
    scheme = scheme_of(path)
    if scheme in ("http", "https"):
        if _has_glob(path.split("?", 1)[0].rsplit("/", 1)[-1]):
            raise ScanError("globs are not supported over HTTP; pass one URL per call")
        http_fs: pafs.FileSystem = pafs.PyFileSystem(_HttpHandler())
        return FileSet(http_fs, (path,), (path,), (file_format(path, fmt),))
    if scheme in ("s3", "gs", "gcs", "r2", "s3a"):
        fs = (store or ObjectStoreConfig()).filesystem("gs" if scheme in ("gs", "gcs") else scheme)
        bare = path.split("://", 1)[1]
        return _expand(fs, bare, fmt, prefix=f"{scheme}://")
    if scheme == "sample":
        return _sample(path, fmt)
    if scheme == "file":
        path = path[len("file://") :]
    elif scheme:
        raise ScanError(
            f"{scheme}:// paths are not read directly; download the export first, or read it with DuckDB "
            "(e.g. read_parquet over the azure extension) and pass the rows to focus.main.to_focus(TABLE ...)"
        )
    local = os.path.abspath(os.path.expanduser(path))
    if _has_glob(path):
        matches = sorted(m for m in _glob.glob(os.path.expanduser(path), recursive=True) if os.path.isfile(m))
        matches = [os.path.abspath(m) for m in matches if _wanted(m, fmt)]
        if not matches:
            raise ScanError(f"no files match {path!r}")
        return FileSet(
            pafs.LocalFileSystem(), tuple(matches), tuple(matches), tuple(file_format(m, fmt) for m in matches)
        )
    if os.path.isdir(local):
        found = []
        for root, dirs, files in os.walk(local):
            dirs[:] = sorted(d for d in dirs if not d.startswith((".", "_")))
            found += [os.path.join(root, f) for f in sorted(files) if _wanted(f, fmt)]
        if not found:
            raise ScanError(f"no export files (.parquet, .csv[.gz], .json[l]) under {path!r}")
        return FileSet(pafs.LocalFileSystem(), tuple(found), tuple(found), tuple(file_format(f, fmt) for f in found))
    if not os.path.isfile(local):
        raise ScanError(f"no such file or directory: {path!r}")
    return FileSet(pafs.LocalFileSystem(), (local,), (local,), (file_format(local, fmt),))


SAMPLES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "samples")


def sample_names() -> list[str]:
    """Return the bundled synthetic sample datasets (``sample://<name>``)."""
    return sorted(d for d in os.listdir(SAMPLES_DIR) if os.path.isdir(os.path.join(SAMPLES_DIR, d)))


def _sample(path: str, fmt: str) -> FileSet:
    """Resolve ``sample://<name>[/<file or glob>]`` to the bundled synthetic exports."""
    rel = path[len("sample://") :].strip("/")
    names = sample_names()
    if not rel or rel.split("/", 1)[0] not in names:
        raise ScanError(f"unknown sample {rel!r}; bundled samples: {', '.join('sample://' + n for n in names)}")
    local = resolve(os.path.join(SAMPLES_DIR, rel), fmt)
    uris = tuple("sample://" + os.path.relpath(f, SAMPLES_DIR).replace(os.sep, "/") for f in local.files)
    return FileSet(local.filesystem, local.files, uris, local.formats)


def _expand(fs: pafs.FileSystem, bare: str, fmt: str, *, prefix: str) -> FileSet:
    try:
        if _has_glob(bare):
            static = re.split(r"[*?\[]", bare, maxsplit=1)[0].rsplit("/", 1)[0]
            infos = fs.get_file_info(pafs.FileSelector(static, recursive=True))
            pattern = bare.replace("**/", "*").replace("**", "*")
            files = sorted(
                i.path for i in infos if i.type == pafs.FileType.File and fnmatch.fnmatchcase(i.path, pattern)
            )
        else:
            info = fs.get_file_info(bare)
            if info.type == pafs.FileType.Directory:
                infos = fs.get_file_info(pafs.FileSelector(bare.rstrip("/"), recursive=True))
                files = sorted(i.path for i in infos if i.type == pafs.FileType.File and _wanted(i.path, fmt))
            elif info.type == pafs.FileType.File:
                files = [bare]
            else:
                raise ScanError(f"no such object: {prefix}{bare}")
    except OSError as err:
        raise ScanError(f"cannot list {prefix}{bare}: {err}") from err
    if not files:
        raise ScanError(f"no files match {prefix}{bare}")
    return FileSet(fs, tuple(files), tuple(prefix + f for f in files), tuple(file_format(f, fmt) for f in files))


# ------------------------------------------------------------------- reading


def _csv_names(fs: pafs.FileSystem, path: str) -> list[str]:
    with fs.open_input_stream(path, compression="detect") as stream:
        reader = pacsv.open_csv(cast(Any, stream), read_options=pacsv.ReadOptions(block_size=1 << 20))
        return list(reader.schema.names)


def file_format_object(fs: pafs.FileSystem, path: str, fmt: str) -> ds.FileFormat:
    """Return the dataset format for one file. CSV columns are all read as text."""
    if fmt == "parquet":
        return ds.ParquetFileFormat()
    if fmt == "csv":
        names = _csv_names(fs, path)
        return ds.CsvFileFormat(
            read_options=pacsv.ReadOptions(block_size=1 << 22),
            convert_options=pacsv.ConvertOptions(
                column_types=dict.fromkeys(names, pa.string()), strings_can_be_null=True
            ),
        )
    return ds.JsonFileFormat(read_options=pajson.ReadOptions(block_size=1 << 24))


def fragment(files: FileSet, i: int) -> Any:
    """Return a dataset fragment for file ``i``."""
    path, fmt = files.files[i], files.formats[i]
    try:
        return file_format_object(files.filesystem, path, fmt).make_fragment(path, filesystem=files.filesystem)
    except (OSError, pa.ArrowInvalid) as err:
        raise ScanError(f"cannot open {files.uris[i]}: {err}") from err


def file_schema(files: FileSet, i: int) -> pa.Schema:
    """Return the physical schema of file ``i``."""
    try:
        schema: pa.Schema = fragment(files, i).physical_schema
        return schema
    except (OSError, pa.ArrowInvalid) as err:
        raise ScanError(f"cannot read the schema of {files.uris[i]}: {err}") from err


def row_groups(frag: Any, expr: ds.Expression | None) -> list[int]:
    """Return the row group ids of a Parquet fragment that may contain matching rows."""
    if not isinstance(frag, ds.ParquetFileFragment):
        return [-1]
    if expr is None:
        return list(range(frag.metadata.num_row_groups))
    try:
        subset: Any = frag.subset(filter=expr)
        return [rg.id for rg in subset.row_groups]
    except pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError:
        return list(range(frag.metadata.num_row_groups))


def iter_unit(frag: Any, rg: int, columns: list[str], expr: ds.Expression | None) -> Iterator[pa.RecordBatch]:
    """Stream the batches of one unit (row group ``rg``, or the whole file for -1)."""
    unit: Any = frag.subset(row_group_ids=[rg]) if rg >= 0 and isinstance(frag, ds.ParquetFileFragment) else frag
    try:
        yield from unit.to_batches(columns=columns, filter=expr, batch_size=BATCH_ROWS, use_threads=False)
    except TypeError:
        yield from unit.to_batches(columns=columns, batch_size=BATCH_ROWS, use_threads=False)


def hive_partitions(uri: str) -> dict[str, str]:
    """``key=value`` directory segments of a path, keys lower-cased."""
    out = {}
    for seg in uri.replace("\\", "/").split("/")[:-1]:
        if "=" in seg:
            k, _, v = seg.partition("=")
            out[k.lower()] = v
    return out


# --------------------------------------------------- process-local iterators


class IteratorCache:
    """Open batch iterators keyed by scan id, so sequential calls do not re-open files."""

    def __init__(self, capacity: int = 32) -> None:
        """Create an empty cache holding at most ``capacity`` scans."""
        self._items: OrderedDict[str, tuple[tuple[int, int, int], Iterator[pa.RecordBatch]]] = OrderedDict()
        self._lock = threading.Lock()
        self._capacity = capacity

    def take(self, scan_id: str, position: tuple[int, int, int]) -> Iterator[pa.RecordBatch] | None:
        """Return the cached iterator if it is exactly at ``position``."""
        with self._lock:
            item = self._items.pop(scan_id, None)
        if item is None or item[0] != position:
            return None
        return item[1]

    def put(self, scan_id: str, position: tuple[int, int, int], it: Iterator[pa.RecordBatch]) -> None:
        """Remember an iterator positioned at ``position``."""
        with self._lock:
            self._items[scan_id] = (position, it)
            while len(self._items) > self._capacity:
                self._items.popitem(last=False)

    def drop(self, scan_id: str) -> None:
        """Forget a scan."""
        with self._lock:
            self._items.pop(scan_id, None)


ITERATORS = IteratorCache()
