from collections.abc import Generator, Iterable
from dataclasses import dataclass

DOMAINS = {
    "": "wikipedia.org",
    "b": "wikibooks.org",
    "d": "wiktionary.org",
    "f": "wikimediafoundation.org",
    "m": "wikimedia.org",
    "n": "wikinews.org",
    "q": "wikiquote.org",
    "s": "wikisource.org",
    "v": "wikiversity.org",
    "voy": "wikivoyage.org",
    "w": "mediawiki.org",
    "wd": "wikidata.org",
}

WIKIMEDIA_SUB_DOMAINS = {
    "commons": "commons.wikimedia.org",
    "meta": "meta.wikimedia.org",
    "incubator": "incubator.wikimedia.org",
    "species": "species.wikimedia.org",
    "strategy": "strategy.wikimedia.org",
    "outreach": "outreach.wikimedia.org",
    "usability": "usability.wikimedia.org",
    "quality": "quality.wikimedia.org",
}


@dataclass
class Pageviews:
    """Holds the explicit parts of the row and interpreted fields.

    `domain_code`, `page_title`, and `count_views` are explicit columns in
    the pageviews CSV file. `language`, `project`, and `mobile` are derived
    from the `domain_code` using the rules specificed in the documentation.

    Read the documentation for more details about the format:

    https://wikitech.wikimedia.org/wiki/Data_Platform/Data_Lake/Traffic/Pageviews
    """

    domain_code: str
    page_title: str
    count_views: int

    # Derived fields
    language: str
    project: str
    mobile: bool


def parse(stream: Iterable[str]) -> Generator[Pageviews, None, None]:
    """Convert a stream of pageviews lines into a stream of parsed objects.

    Args:
        stream (Iterable[str]): Stream of pageviews lines. Normally from
            one of the `pgcreek.stream` functions.

    Yields:
        Pageviews: Parsed pageviews objects.
    """
    for line in stream:
        yield parse_line(line)


def parse_line(line: str) -> Pageviews:
    """Parse a single line from the pageviews file.

    Args:
        line (str): A single line from the pageviews file.

    Returns:
        Pageviews: The parsed pageviews object.
    """
    domain_code, page_title, count_views, _ = line.split()
    language, project, mobile = parse_domain_code(domain_code)

    return Pageviews(
        domain_code=domain_code,
        page_title=page_title,
        count_views=int(count_views),
        language=language,
        project=project,
        mobile=mobile,
    )


def parse_domain_code(domain_code: str) -> tuple[str, str, bool]:
    """Extract the language, project, and mobile status from the domain code
    according to the rules from:

    https://wikitech.wikimedia.org/wiki/Data_Platform/Data_Lake/Traffic/Pageviews

    Args:
        domain_code (str): The domain code from the pageviews file.

    Returns:
        tuple[str, str, bool]: A tuple containing the language, project, and
            whether the page was from a mobile subdomain.
    """
    parts = domain_code.split(".")
    language = parts[0]

    if len(parts) == 1:
        # Only one part means a language page on wikipedia.org.
        domain = "wikipedia.org"
        mobile = False
    elif language in WIKIMEDIA_SUB_DOMAINS:
        # If the "language" component is a whitelisted Wikimedia project, it
        # is either followed by '.m' for non-mobile or '.m.m' for mobile. All
        # these Wikimedia subdomains are assumed to be in English.
        language = "en"
        domain = WIKIMEDIA_SUB_DOMAINS[parts[0]]
        mobile = len(parts) == 3
    elif len(parts) == 2:
        # Two parts is either a mobile page ('.m' or '.zero') or a Wikimedia
        # project.
        mobile = parts[1] in {"m", "zero"}
        domain = "wikipedia.org" if mobile else DOMAINS.get(parts[1], parts[1])
    elif len(parts) == 3:
        # Three parts is always a mobile site from a Wikimedia project.
        mobile = True
        domain = DOMAINS.get(parts[2], parts[2])
    else:
        raise ValueError(f"Invalid domain code format: {domain_code}.")

    return (language, domain, mobile)
