from pathlib import Path

import pytest

from pvcreek.sift import sift
from pvcreek.stream import stream_local


@pytest.fixture
def pageviews():
    """Fixture for the sift function"""
    root = Path(__file__).parent.parent / "files"
    path = root / "2024" / "2024-08" / "pageviews-20240803-060000.gz"
    return list(stream_local(path))


def test_sift_empty(pageviews: list[str]) -> None:
    """Ensure post filtering without filters leaves the stream unmodified."""
    assert len(list(sift(pageviews))) == 1000


def test_sift_startswith(pageviews: list[str]) -> None:
    """Test sift with regular expressions."""
    assert len(list(sift(pageviews, startswith="a.b.c.d "))) == 0
    assert len(list(sift(pageviews, startswith="en.d "))) == 7
    assert len(list(sift(pageviews, startswith="commons.m.m "))) == 3


def test_sift_contains(pageviews: list[str]) -> None:
    """Test sift with regular expressions."""
    assert len(list(sift(pageviews, contains="footballer"))) == 0
    assert len(list(sift(pageviews, contains="er "))) == 19


def test_sift_regex(pageviews: list[str]) -> None:
    """Test sift with regular expressions."""
    assert len(list(sift(pageviews, regex=r"^en.d "))) == 7
    assert len(list(sift(pageviews, regex=r"\(.*?\)"))) == 120


def test_sift_custom(pageviews: list[str]) -> None:
    """Test sift with regular expressions."""
    assert len(list(sift(pageviews, custom_filter=lambda l: len(l) > 70))) == 8
    assert (
        len(list(sift(pageviews, custom_filter=lambda l: "(" in l and ")" in l))) == 120
    )
