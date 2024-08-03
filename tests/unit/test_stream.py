from collections.abc import Generator
from datetime import datetime
from pathlib import Path

import pytest

from pvcreek.stream import (
    filename_from_timestamp,
    stream,
    stream_local,
    url_from_filename,
)


def test_stream_local() -> None:
    """Stream a pageviews file line by line from the local file system."""
    filename = "pageviews-20240803-060000.gz"
    root = Path(__file__).parent.parent / "files"
    path = root / "2024" / "2024-08" / filename

    assert path.exists()
    assert path.is_file()

    # These two should be identical when used with a `Path`
    verify_pageviews_content(stream_local(path))
    verify_pageviews_content(stream(path))


def test_stream_local_error() -> None:
    """Test that a `TypeError` is raised when the input is not a string,
    datetime, or Path object."""
    with pytest.raises(TypeError):
        list(stream(0.0))  # type: ignore[arg-type]


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


def test_filename_from_timestamp_accurate() -> None:
    """Convert a `datetime` object to a pageviews filename."""
    timestamp = datetime(2024, 8, 3, 11, 0, 0)
    expected = "pageviews-20240803-110000.gz"

    assert filename_from_timestamp(timestamp) == expected

    timestamp = datetime(2024, 8, 2, 19, 0, 0)
    expected = "pageviews-20240802-190000.gz"

    assert filename_from_timestamp(timestamp) == expected


def test_filename_from_timestamp_inaccurate() -> None:
    """Convert a `datetime` object to a pageviews filename using rounding when
    the user provides too much detail in the timestamp."""
    timestamp = datetime(2024, 8, 3, 11, 13, 45)
    expected = "pageviews-20240803-110000.gz"

    assert filename_from_timestamp(timestamp) == expected

    timestamp = datetime(2024, 8, 3)
    expected = "pageviews-20240803-000000.gz"

    assert filename_from_timestamp(timestamp) == expected


def test_url_from_filename() -> None:
    """Convert a pageviews filename to a URL."""
    base_url = "https://dumps.wikimedia.org/other/pageviews/"
    filename = "pageviews-20240803-110000.gz"
    expected = "https://dumps.wikimedia.org/other/pageviews/2024/2024-08/pageviews-20240803-110000.gz"

    assert url_from_filename(base_url, filename) == expected

    # Without trailing slash in base URL
    base_url = "https://mirror.accum.se/mirror/wikimedia.org/other/pageviews"
    filename = "pageviews-20240802-190000.gz"
    expected = "https://mirror.accum.se/mirror/wikimedia.org/other/pageviews/2024/2024-08/pageviews-20240802-190000.gz"

    assert url_from_filename(base_url, filename) == expected


def test_url_from_filename_invalid() -> None:
    """An invalid filename should raise a `ValueError`."""
    base_url = "https://dumps.wikimedia.org/other/pageviews/"
    filename = "invalid-filename.gz"

    with pytest.raises(ValueError):
        url_from_filename(base_url, filename)
