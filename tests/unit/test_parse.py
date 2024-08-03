import pytest

from pvcreek.parse import Pageviews, parse, parse_domain_code, parse_line


def test_parse_domain_code() -> None:
    """Ensure we follow the standard specified on:

    https://wikitech.wikimedia.org/wiki/Data_Platform/Data_Lake/Traffic/Pageviews

    as far as possible. Note that there is an ambiguity which can't be resolved,
    where e.g. "no.m" is used both for Norwegian mobile pages and Norwegian
    Wikimedia pages."""
    assert parse_domain_code("en") == ("en", "wikipedia.org", False)
    assert parse_domain_code("en.m") == ("en", "wikipedia.org", True)
    assert parse_domain_code("en.b") == ("en", "wikibooks.org", False)
    assert parse_domain_code("en.m.b") == ("en", "wikibooks.org", True)
    assert parse_domain_code("no.m") == ("no", "wikipedia.org", True)
    assert parse_domain_code("no.m.m") == ("no", "wikimedia.org", True)
    assert parse_domain_code("de.m.voy") == ("de", "wikivoyage.org", True)
    assert parse_domain_code("commons.m") == ("en", "commons.wikimedia.org", False)
    assert parse_domain_code("commons.m.m") == ("en", "commons.wikimedia.org", True)


def test_parse_domain_code_invalid() -> None:
    """Test the parsing of an invalid domain code."""
    with pytest.raises(ValueError):
        parse_domain_code("en.m.m.m")  # Too many parts
    with pytest.raises(AttributeError):
        parse_domain_code(1)  # type: ignore[arg-type]


def test_parse_line() -> None:
    """Test the parsing of a single line."""
    assert parse_line("en.d circumfluebant 1 0") == Pageviews(
        domain_code="en.d",
        page_title="circumfluebant",
        count_views=1,
        language="en",
        project="wiktionary.org",
        mobile=False,
    )
    assert parse_line("ko 서울_지하철_7호선 2 0") == Pageviews(
        domain_code="ko",
        page_title="서울_지하철_7호선",
        count_views=2,
        language="ko",
        project="wikipedia.org",
        mobile=False,
    )


def test_parse() -> None:
    """Test the full parsing function."""
    lines = [
        "en.d circumfluebant 1 0",
        "ko 서울_지하철_7호선 2 0",
    ]
    expected = [
        Pageviews(
            domain_code="en.d",
            page_title="circumfluebant",
            count_views=1,
            language="en",
            project="wiktionary.org",
            mobile=False,
        ),
        Pageviews(
            domain_code="ko",
            page_title="서울_지하철_7호선",
            count_views=2,
            language="ko",
            project="wikipedia.org",
            mobile=False,
        ),
    ]
    parsed = list(parse(lines))

    assert len(parsed) == len(expected)
