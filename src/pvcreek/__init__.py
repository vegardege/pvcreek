import re
from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from typing import Optional

from pvcreek.parse import Pageviews, parse
from pvcreek.sift import sift
from pvcreek.stream import BASE_URL, download, stream_local, stream_remote


def pvcreek(
    filename: str | datetime,
    base_url: str = BASE_URL,
    cache_path: Optional[Path] = None,
    domain_code: Optional[str | re.Pattern] = None,
    page_title: Optional[str | re.Pattern] = None,
    min_views: Optional[int] = None,
    max_views: Optional[int] = None,
    language: Optional[str | re.Pattern] = None,
    project: Optional[str | re.Pattern] = None,
    mobile: Optional[bool] = None,
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
        cache_path (Optional[Path]): If set, files will be downloaded to and
            streamed from this directory. If None, all files will be streamed
            from the server. Set this if you plan to use the same file more
            than once.
        domain_code (Optional[str | re.Pattern]): If set, filter the stream
            to only include entries with the specified domain code. If None,
            no filtering is applied.
        page_title (Optional[str | re.Pattern]): If set, filter the stream
            to only include entries with the specified page title. If None,
            no filtering is applied.
        min_views (Optional[int]): If set, filter the stream to only include
            entries with a view count greater than or equal to this value. If
            None, no filtering is applied.
        max_views (Optional[int]): If set, filter the stream to only include
            entries with a view count less than or equal to this value. If
            None, no filtering is applied.
        language (Optional[str | re.Pattern]): If set, filter the stream to
            only include entries with the specified language. If None, no
            filtering is applied.
        project (Optional[str | re.Pattern]): If set, filter the stream to
            only include entries with the specified project. If None, no
            filtering is applied.
        mobile (Optional[bool]): If set, filter the stream to only include
            mobile or non-mobile entries. If None, no filtering is applied.

    Yields:
        Pageviews: Parsed and sifted pageviews objects.
    """
    if cache_path:
        local_path = download(filename, cache_path, base_url=base_url)
        stream = stream_local(local_path)
    else:
        stream = stream_remote(filename, base_url=base_url)

    yield from sift(
        parse(stream),
        domain_code,
        page_title,
        min_views,
        max_views,
        language,
        project,
        mobile,
    )
