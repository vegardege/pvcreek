import re
from collections.abc import Callable, Generator, Iterable
from typing import Optional


def sift(
    stream: Iterable[str],
    startswith: Optional[str] = None,
    contains: Optional[str] = None,
    regex: Optional[str] = None,
    custom_filter: Optional[Callable[[str], bool]] = None,
) -> Generator[str, None, None]:
    """Sift through the stream of lines and filter out unwanted entries
    before parsing.

    Args:
        stream (Iterable[str]): The input stream of lines.
        startswith (Optional[str]): If set, filter the stream to only include
            lines that match the regex.
        contains (Optional[str]): If set, filter the stream to only include
            lines that contain the substring.
        regex (Optional[str]): If set, filter the stream to only include lines
            that match the regex.
        custom_filter (Optional[Callable[[str], bool]]): If set, filter the
            stream to only include lines that match the custom filter function.
            The function should take a single string argument and return a
            boolean value indicating whether to include the line.

    Yields:
        str: Subset of the stream matching the regex.
    """
    pattern = re.compile(regex) if regex else None

    for line in stream:
        if startswith and not line.startswith(startswith):
            continue
        if contains and contains not in line:
            continue
        if pattern and not pattern.search(line):
            continue
        if custom_filter and not custom_filter(line):
            continue

        yield line
