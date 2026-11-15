"""A local S3 (moto) holding the sample exports, for the s3:// reader tests.

Prints ``PORT:<n>`` once the bucket ``focus-samples`` is filled with a copy of
src/focus_worker/samples/. Credentials are the fixed fake pair below.

    uv run --no-project --with 'moto[server]>=5,<6' python test/s3_server.py
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import boto3
from moto.server import ThreadedMotoServer

KEY_ID = "focus-test-key"
SECRET = "focus-test-secret"
BUCKET = "focus-samples"
SAMPLES = Path(__file__).resolve().parent.parent / "src" / "focus_worker" / "samples"


def main() -> None:
    """Start moto, upload the samples, print the port, then serve until killed."""
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    port = server.get_host_and_port()[1]
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://127.0.0.1:{port}",
        aws_access_key_id=KEY_ID,
        aws_secret_access_key=SECRET,
        region_name="us-east-1",
    )
    s3.create_bucket(Bucket=BUCKET)
    for path in SAMPLES.rglob("*"):
        if path.is_file():
            s3.upload_file(str(path), BUCKET, path.relative_to(SAMPLES).as_posix())
    print(f"PORT:{port}", flush=True)
    sys.stderr.write(f"moto S3 on {port} with s3://{BUCKET}/\n")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        server.stop()
        os._exit(0)


if __name__ == "__main__":
    main()
