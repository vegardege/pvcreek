import re
from collections.abc import Generator, Iterable
from typing import Optional

from pvcreek.parse import Pageviews


def sift(
    stream: Iterable[Pageviews],
    domain_code: Optional[str | re.Pattern] = None,
    page_title: Optional[str | re.Pattern] = None,
    min_views: Optional[int] = None,
    max_views: Optional[int] = None,
    language: Optional[str | re.Pattern] = None,
    project: Optional[str | re.Pattern] = None,
    mobile: Optional[bool] = None,
) -> Generator[Pageviews, None, None]:
    """Sift through the stream and filter out unwanted entries.

    Args:
        stream (Iterator[Pageviews]): The input stream of Pageviews objects.
        domain_code (Optional[str]): If set, filter the stream to only include
            entries with the specified domain code. If None, no filtering is
            applied.
        page_title (Optional[str]): If set, filter the stream to only include
            entries with the specified page title. If None, no filtering is
            applied.
        min_views (Optional[int]): If set, filter the stream to only include
            entries with a view count greater than or equal to this value. If
            None, no filtering is applied.
        max_views (Optional[int]): If set, filter the stream to only include
            entries with a view count less than or equal to this value. If
            None, no filtering is applied.
        language (Optional[str]): If set, filter the stream to only include
            entries with the specified language. If None, no filtering is
            applied.
        project (Optional[str]): If set, filter the stream to only include
            entries with the specified project. If None, no filtering is
            applied.
        mobile (Optional[bool]): If set, filter the stream to only include
            mobile or non-mobile entries. If None, no filtering is applied.

    Yields:
        Pageviews: Subset of the stream matching all filters.
    """
    comparators = []

    if domain_code is not None:
        comparators.append(lambda pv: compare_str(domain_code, pv.domain_code))
    if page_title is not None:
        comparators.append(lambda pv: compare_str(page_title, pv.page_title))
    if min_views is not None or max_views is not None:
        comparators.append(lambda pv: compare_int(min_views, max_views, pv.count_views))
    if language is not None:
        comparators.append(lambda pv: compare_str(language, pv.language))
    if project is not None:
        comparators.append(lambda pv: compare_str(project, pv.project))
    if mobile is not None:
        comparators.append(lambda pv: compare_bool(mobile, pv.mobile))

    for pageviews in stream:
        if not comparators or all(f(pageviews) for f in comparators):
            yield pageviews


def compare_str(filter_value: str | re.Pattern, target: str) -> bool:
    """Filter a string value against a target.

    Args:
        value (Optional[str | re.Pattern]): The value to filter. If the filter
            value is a `str`, exact match is needed. For `re.Pattern`, a regex
            match is checked. If None, it is considered a match.
        target (str): The target string to match against.

    Returns:
        bool: True if the value matches the target or if the value is None.
    """
    if isinstance(filter_value, re.Pattern):
        return bool(filter_value.match(target))

    return filter_value == target


def compare_int(
    filter_min: Optional[int], filter_max: Optional[int], target: int
) -> bool:
    """Filter an integer value against a range.

    Args:
        filter_min (Optional[int]): The minimum value of the range. If None,
            it is considered a match.
        filter_max (Optional[int]): The maximum value of the range. If None,
            it is considered a match.
        target (int): The target integer to match against.

    Returns:
        bool: True if the value is within the range or if the value is None.
    """
    if filter_min is not None and target < filter_min:
        return False

    if filter_max is not None and target > filter_max:
        return False

    return True


def compare_bool(value: bool, target: bool) -> bool:
    """Filter a boolean value against a target.

    Args:
        value (Optional[bool]): The value to filter. If None, it is considered
            a match.
        target (bool): The target value to match against.

    Returns:
        bool: True if the value matches the target or if the value is None.
    """
    return value == target
