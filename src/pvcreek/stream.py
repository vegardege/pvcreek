import gzip
import io
import re
from collections.abc import Generator
from datetime import datetime
from pathlib import Path

import requests

BASE_URL = "https://dumps.wikimedia.org/other/pageviews/"
FILENAME = re.compile(r"^pageviews-(\d{4})(\d{2})\d{2}-\d{2}0000\.gz$")


def download(filename: str | datetime, target: Path, base_url: str = BASE_URL) -> None:
    """Download a file from the remote server to the local file system. Use
    this if you want to cache the gzipped file.

    Args:
        filename (str | datetime): The file to download.
        target (Path): The target path to save the downloaded file.
        base_url (str): Base URL for the pageviews server.
    """
    if isinstance(filename, datetime):
        filename = filename_from_timestamp(filename)

    url = url_from_filename(base_url, filename)

    with requests.get(url, stream=True) as response:
        response.raise_for_status()

        with open(target, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)


def stream(
    file: str | datetime | Path, base_url: str = BASE_URL
) -> Generator[str, None, None]:
    """Stream a pageviews file line by line, either from a local file or from
    a remote server.

    Args:
        file (str | datetime | Path): The file to stream. If set as a path, it
            will read from the local file system, otherwise it will download
            the file from a pageviews server.
        base_url (str): Base URL for the pageviews server.

    Yields:
        str: A line from the pageviews file.
    """
    if isinstance(file, str) or isinstance(file, datetime):
        yield from stream_remote(file, base_url)
    elif isinstance(file, Path):
        yield from stream_local(file)
    else:
        raise TypeError(
            "file must be a string, datetime, or Path object, "
            f"not {type(file).__name__}"
        )


def stream_local(file: Path) -> Generator[str, None, None]:
    """Stream a pageviews file line by line from the local file system.

    Args:
        file (Path): Local file to stream.

    Yields:
        str: A line from the pageviews file.
    """
    with gzip.open(file, "rb") as f:
        for line in f:
            yield line.decode("utf-8").rstrip("\n")


def stream_remote(
    file: str | datetime, base_url: str = BASE_URL
) -> Generator[str, None, None]:
    """Stream a pageviews file line by line from a remote server.

    Args:
        file (str | datetime): Remote file to stream.
        base_url (str): Base URL for the pageviews server.

    Yields:
        str: A line from the pageviews file.
    """
    if isinstance(file, datetime):
        file = filename_from_timestamp(file)

    url = url_from_filename(base_url, file)

    with requests.get(url, stream=True) as response:
        response.raise_for_status()

        with gzip.GzipFile(fileobj=response.raw) as gz:
            with io.TextIOWrapper(gz, encoding="utf-8") as reader:
                for line in reader:
                    yield line.rstrip("\n")


def filename_from_timestamp(timestamp: datetime) -> str:
    """Convert a timestamp to a pageviews filename. Everything more specific
    than hour will be ignored.

    Ignores the timezone information in the timestamp.

    Args:
        timestamp (datetime): The timestamp to convert.

    Returns:
        str: Pageviews filename for the given hour.
    """
    return f"pageviews-{timestamp:%Y%m%d}-{timestamp:%H}0000.gz"


def url_from_filename(base_url: str, filename: str) -> str:
    """Convert a pageviews filename to a URL. Everything more specific than
    hour will be ignored.

    Args:
        base_url (str): The base URL hosting the pageviews files (not including
            directories or filename).
        filename (str): The filename to convert.

    Returns:
        str: Full URL for the specified file.
    """
    if not base_url.endswith("/"):
        base_url += "/"

    if match := FILENAME.match(filename):
        year, month = match.group(1), match.group(2)
        return f"{base_url}{year}/{year}-{month}/{filename}"
    else:
        raise ValueError(f"Invalid filename: {filename}")
