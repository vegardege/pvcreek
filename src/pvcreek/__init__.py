from collections.abc import Callable, Generator
from datetime import datetime
from pathlib import Path
from typing import Optional

from pvcreek.parse import Pageviews, parse
from pvcreek.sift import sift
from pvcreek.stream import BASE_URL, download, stream_local, stream_remote


def pvcreek(
    filename: str | datetime,
    base_url: str = BASE_URL,
    cache_path: Optional[str | Path] = None,
    line_startswith: Optional[str] = None,
    line_contains: Optional[str] = None,
    line_regex: Optional[str] = None,
    line_custom_filter: Optional[Callable[[str], bool]] = None,
) -> Generator[Pageviews, None, None]:
    """Stream `Pageviews` objects from a Wikimedia pageviews file.

    If `filename` is a `str` or a `datetime`, the file will be retrieved from
    a remote server (specified by `base_url`). If `filename` is a `Path`, the file
    will be read from the local file system.

    If `cache_path` is specified, the file will be downloaded and stored in the
    specified directory. If the file already exists, it will not be downloaded
    again. The file will be streamed from the local file system.

    Args:
        filename (str | datetime): Name of the pageviews file.
        base_url (str): Base URL for the pageviews server.
        cache_path (Optional[str | Path]): If set, files will be downloaded to
            and streamed from this directory. If None, all files will be
            streamed from the server. Set this if you plan to use the same file
            more than once.
        line_startswith (Optional[str]): If set, filter the stream to only
            include lines that match the regex.
        line_contains (Optional[str]): If set, filter the stream to only
            include lines that contain the substring.
        line_regex (Optional[str]): If set, filter the stream to only
            include lines that match the regex.
        line_custom_filter (Optional[Callable[[str], bool]]): If set, filter the
            stream to only include lines that match the custom filter function.
            The function should take a single string argument and return a
            boolean value indicating whether to include the line.

    Yields:
        Pageviews: Parsed and sifted pageviews objects.
    """
    if cache_path:
        local_path = download(filename, cache_path, base_url=base_url)
        stream = stream_local(local_path)
    else:
        stream = stream_remote(filename, base_url=base_url)

    yield from parse(
        sift(
            stream,
            line_startswith,
            line_contains,
            line_regex,
            line_custom_filter,
        )
    )
