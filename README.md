# pvcreek

`pvcreek` is a pure python project allowing you to stream download, parse,
and filter data from Wikimedia's exported pageviews dumps.

> [!WARNING]
> This project was abandoned in favor of
> [pvstream](https://github.com/vegardege/pvstream), a Rust library with python
> bindings, giving a 5x performance boost (or more) for my use cases. The code
> for this library is shared because sharing is nice and in case someone may
> want to fork it or use parts of it for a project of their own. This project
> will not be maintained or expanded.

## Usage

The main entry point of the library is the function `stream_from_server`. Enter
a filename or a `datetime` object to stream download, potentially cache, and
parse a file:

```python
from pvcreek import stream_from_server

for row in stream_from_server(
    datetime(2024, 8, 18, 8, 0, 0),
    cache_path=Path(".cache/pvcreek"),
):
    if row.language == "no" and row.mobile:
        print(row)
```

> [!CAUTION]
> This code will stream the file directly from Wikimedia's servers. Please be
> kind to the servers and cache if you plan to read the same file many times.
> Consider using a mirror closer to you by setting `base_url` to one of the
> mirrors listed on [wikimedia.org](https://dumps.wikimedia.org/mirrors.html).

### Function Signature

```python
def stream_from_server(
    filename: str | datetime,
    base_url: str = BASE_URL,
    cache_path: Optional[str | Path] = None,
    line_startswith: Optional[str] = None,
    line_contains: Optional[str] = None,
    line_regex: Optional[str] = None,
    line_custom_filter: Optional[Callable[[str], bool]] = None,
) -> Generator[Pageviews, None, None]:
```

### Arguments

| Argument             | Type                              | Description                                                                                                                                      |
| -------------------- | --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `filename`           | `str` \| `datetime`               | Name or date of the pageviews file (or a datetime, which will be mapped to the right filename and directory).                                    |
| `base_url`           | `str`                             | Base URL to retrieve remote files. Defaults to Wikimedia's dump servers, but it's recommended to look for a closer mirror.                       |
| `cache_path`         | `Optional[str \| Path]`           | If set, the function will first look in the cache directory, and download the file if it's not already downloaded. This prevents re-downloading. |
| `line_startswith`    | `Optional[str]`                   | Only include lines that start with this string.                                                                                                  |
| `line_contains`      | `Optional[str]`                   | Only include lines that contain this substring.                                                                                                  |
| `line_regex`         | `Optional[str]`                   | Only include lines that matches this regular expression.                                                                                         |
| `line_custom_filter` | `Optional[Callable[[str], bool]]` | Custom filtering function. If it returns `True`, the row is parsed and yielded, if `False` the line is ignored.                                  |

### Results

The function yields parsed and filtered `Pageviews` objects. Each object
contains both raw values from the file and parsed domain code values:

```python
@dataclass
class Pageviews:
    domain_code: str
    page_title: str
    count_views: int

    # Derived fields
    language: str
    project: str
    mobile: bool
```

See [Wikimedia's documentation](https://wikitech.wikimedia.org/wiki/Data_Platform/Data_Lake/Traffic/Pageviews)
for more information about the different fields.

## Testing

```bash
pytest tests/ --cov=src

---------- coverage: platform darwin, python 3.10.1-final-0 ----------
Name                      Stmts   Miss  Cover
---------------------------------------------
src/pvcreek/__init__.py      13      0   100%
src/pvcreek/filter.py        15      0   100%
src/pvcreek/parse.py         39      0   100%
src/pvcreek/stream.py        58      0   100%
---------------------------------------------
TOTAL                       125      0   100%
```
