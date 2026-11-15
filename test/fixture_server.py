"""Serve the sample exports and test/fixtures over HTTP with Range support (for the https:// reader tests).

Prints ``PORT:<n>`` on stdout once listening, like the worker's ``--http`` mode.
``python -m http.server`` ignores Range headers, so it cannot stand in for S3/GCS
static hosting; this does.

    python test/fixture_server.py [--port 0]
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# The bundled synthetic samples, then the test-only fixtures (FOCUS sample data).
ROOTS = [Path(__file__).resolve().parent.parent / "src" / "focus_worker" / "samples", Path(__file__).resolve().parent / "fixtures"]


class RangeHandler(SimpleHTTPRequestHandler):
    """Static files with single-range ``Range: bytes=a-b`` support."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, directory=str(ROOTS[0]), **kwargs)  # type: ignore[arg-type]

    def translate_path(self, path: str) -> str:
        """Look the URL path up in each root in turn."""
        for root in ROOTS:
            self.directory = str(root)
            candidate = super().translate_path(path)
            if os.path.exists(candidate):
                return candidate
        return candidate

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        """Keep stdout clean; log to stderr only."""
        sys.stderr.write(format % args + "\n")

    def send_head(self):  # type: ignore[no-untyped-def]
        """Serve a byte range when asked, else the whole file."""
        path = self.translate_path(self.path)
        rng = self.headers.get("Range")
        if not rng or not os.path.isfile(path):
            return super().send_head()
        m = re.match(r"bytes=(\d+)-(\d*)$", rng.strip())
        size = os.path.getsize(path)
        if not m:
            self.send_error(416)
            return None
        start = int(m.group(1))
        end = int(m.group(2)) if m.group(2) else size - 1
        if start >= size:
            self.send_error(416)
            return None
        end = min(end, size - 1)
        f = open(path, "rb")  # noqa: SIM115 - closed by copyfile's caller
        f.seek(start)
        self.send_response(206)
        self.send_header("Content-Type", self.guess_type(path))
        self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header("Content-Length", str(end - start + 1))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        self._remaining = end - start + 1
        return f

    def copyfile(self, source, outputfile):  # type: ignore[no-untyped-def]
        """Copy only the requested range."""
        remaining = getattr(self, "_remaining", None)
        if remaining is None:
            return super().copyfile(source, outputfile)
        while remaining > 0:
            chunk = source.read(min(65536, remaining))
            if not chunk:
                break
            outputfile.write(chunk)
            remaining -= len(chunk)
        self._remaining = None
        return None


def main() -> None:
    """Serve until killed."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=0)
    args = ap.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), RangeHandler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
