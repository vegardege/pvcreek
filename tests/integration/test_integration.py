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

from pvcreek import pvcreek
from pvcreek.stream import download, is_cached, stream, stream_remote


@pytest.mark.integration
def test_full() -> None:
    """Test the full pipeline from download to sift."""
    root = Path(__file__).parent.parent / "files"
    port = 8080
    base_url = f"http://127.0.0.1:{port}/"

    with run_file_server(root, port):
        filename = "pageviews-20240803-060000.gz"

        # Test pure streaming
        streamed_lines = list(pvcreek(filename, base_url=base_url))
        assert len(streamed_lines) == 1000

        # Test cached streaming
        with tempfile.TemporaryDirectory() as tempdir:

            # First pass should download the file
            assert is_cached(filename, Path(tempdir)) is False
            downloaded = list(
                pvcreek(
                    filename,
                    base_url=base_url,
                    cache_path=Path(tempdir),
                    language="en",
                )
            )
            assert len(downloaded) == 425
            assert all(pv.language == "en" for pv in downloaded)

            # Second pass should not download, but still be able to
            # apply a different filter when streaming.
            assert is_cached(filename, Path(tempdir)) is True
            downloaded = list(
                pvcreek(
                    filename,
                    base_url=base_url,
                    cache_path=Path(tempdir),
                    language="no",
                )
            )
            assert len(downloaded) == 5
            assert all(pv.language == "no" for pv in downloaded)


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

            assert is_cached(filename, Path(tempdir)) is False
            download(filename, Path(tempdir), base_url=base_url)

            # Downloading twice is effectively a no-op
            assert is_cached(filename_ts, Path(tempdir)) is True
            download(filename, Path(tempdir), base_url=base_url)

        with tempfile.TemporaryDirectory() as tempdir:

            assert is_cached(filename_ts, Path(tempdir)) is False
            download(filename_ts, Path(tempdir), base_url=base_url)
            assert is_cached(filename_ts, Path(tempdir)) is True


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

    assert rows[0] == "en.d circumfluebant 1 0\n"
    assert rows[1] == "ko 서울_지하철_7호선 2 0\n"

    # Exhaust the generator to ensure all lines are read
    rows.extend(generator)

    assert len(rows) == 1000
    assert rows[-1] == "da Linjeløb 2 0\n"


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
