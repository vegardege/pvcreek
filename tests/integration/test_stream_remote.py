import signal
import socket
import subprocess
import tempfile
import time
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pytest

from pvcreek.stream import download, stream, stream_remote


@pytest.mark.integration
def test_download() -> None:
    """Test the function downloading a gzipped file."""
    root = Path(__file__).parent.parent / "files"
    port = 8080
    base_url = f"http://127.0.0.1:{port}/"

    with run_file_server(root, port):
        filename = "pageviews-20240803-060000.gz"
        filename_ts = datetime(2024, 8, 3, 6, 0, 0)

        with tempfile.TemporaryDirectory() as tempdir:

            target = Path(tempdir) / filename
            download(filename, target, base_url=base_url)

            assert target.exists()
            assert target.is_file()

        with tempfile.TemporaryDirectory() as tempdir:

            target = Path(tempdir) / filename
            download(filename_ts, target, base_url=base_url)

            assert target.exists()
            assert target.is_file()


@pytest.mark.integration
def test_stream_remote() -> None:
    """Test the function streaming a remote gzipped file."""
    root = Path(__file__).parent.parent / "files"
    port = 8080
    base_url = f"http://127.0.0.1:{port}/"

    with run_file_server(root, port):
        filename = "pageviews-20240803-060000.gz"
        filename_ts = datetime(2024, 8, 3, 6, 0, 0)

        # These two should be identical when used with a remote reference.
        # The timestamp and the filename are the same, and should return
        # the same content.
        verify_pageviews_content(stream_remote(filename, base_url=base_url))
        verify_pageviews_content(stream(filename_ts, base_url=base_url))


def verify_pageviews_content(generator: Generator[str]) -> None:
    """Test the content of a generator against the hard coded sample file."""
    # Read the two first lines
    rows = [next(generator), next(generator)]

    assert rows[0] == "en.d circumfluebant 1 0"
    assert rows[1] == "ko 서울_지하철_7호선 2 0"

    # Exhaust the generator to ensure all lines are read
    rows.extend(generator)

    assert len(rows) == 1000
    assert rows[-1] == "da Linjeløb 2 0"


@contextmanager
def run_file_server(directory: Path, port: int = 8080) -> Generator[None, None, None]:
    """Run a simple HTTP file server in the background."""
    try:
        # Create a HTTP file server running in a subprocess
        server = subprocess.Popen(
            ["python", "-m", "http.server", str(port), "--bind", "127.0.0.1"],
            cwd=directory,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Wait for the server to start
        start = time.time()
        timeout = 5  # seconds
        while True:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                    break  # Server is up
            except OSError:
                if time.time() - start > timeout:
                    server.terminate()
                    raise RuntimeError(f"Server did not start within {timeout} seconds")
                time.sleep(0.1)

        # Allow context manager to proceed
        yield
    finally:
        # Terminate the server
        if server.poll() is None:
            server.send_signal(signal.SIGINT)
            server.wait()
