import re
from pathlib import Path

import pytest

from pvcreek.parse import Pageviews, parse
from pvcreek.sift import sift
from pvcreek.stream import stream_local


@pytest.fixture
def pageviews():
    """Fixture for the sift function"""
    root = Path(__file__).parent.parent / "files"
    path = root / "2024" / "2024-08" / "pageviews-20240803-060000.gz"
    return list(parse(stream_local(path)))


def test_sift_empty(pageviews: list[Pageviews]) -> None:
    """Ensure sifting without filters leaves the stream unmodified."""
    assert len(list(sift(pageviews))) == 1000


def test_sift_strings(pageviews: list[Pageviews]) -> None:
    """Test sifting on the various string types."""
    assert len(list(sift(pageviews, domain_code=None))) == 1000
    assert len(list(sift(pageviews, domain_code="en.d"))) == 7
    assert len(list(sift(pageviews, domain_code="commons.m.m"))) == 3

    assert len(list(sift(pageviews, page_title="circumfluebant"))) == 1

    assert len(list(sift(pageviews, language="en"))) == 425
    assert len(list(sift(pageviews, language="da"))) == 2

    assert len(list(sift(pageviews, project="wikivoyage.org"))) == 4
    assert len(list(sift(pageviews, project="commons.wikimedia.org"))) == 11


def test_sift_regex(pageviews: list[Pageviews]) -> None:
    """Test sifting on string types with regular expressions."""
    assert len(list(sift(pageviews, domain_code=re.compile("^en.*")))) == 412
    assert len(list(sift(pageviews, page_title=re.compile("circ.*")))) == 1
    assert len(list(sift(pageviews, language=re.compile("en|da")))) == 427
    assert len(list(sift(pageviews, project=re.compile("wikivoyage.*")))) == 4


def test_sift_ints(pageviews: list[Pageviews]) -> None:
    """Test sifting on the integer types."""
    assert len(list(sift(pageviews, min_views=None))) == 1000

    assert len(list(sift(pageviews, min_views=50))) == 4
    assert len(list(sift(pageviews, max_views=4))) == 884

    assert len(list(sift(pageviews, min_views=10, max_views=19))) == 25


def test_sift_bool(pageviews: list[Pageviews]) -> None:
    """Test sifting on the boolean types."""
    sample = pageviews[:100]

    assert len(list(sift(sample, mobile=None))) == 100  # No filter
    assert len(list(sift(sample, mobile=True))) == 64  # Only mobile
    assert len(list(sift(sample, mobile=False))) == 36  # Only non-mobile


def test_sift_combinaed(pageviews: list[Pageviews]) -> None:
    """Test sifting with all filters applied."""
    sifted = list(
        sift(
            pageviews,
            page_title=re.compile(".*al.*"),
            min_views=3,
            language=re.compile("en|no"),
            mobile=True,
        )
    )

    assert len(sifted) == 5

    assert all(re.match(r".*al.*", pv.page_title) for pv in sifted)
    assert all(pv.count_views >= 3 for pv in sifted)
    assert all(re.match(r"en|no", pv.language) for pv in sifted)
    assert all(pv.mobile for pv in sifted)
