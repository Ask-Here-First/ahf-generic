import os
from collections.abc import Iterable, Callable, Sequence
from typing import TextIO
from logging import info

from .lib import find_in_ancestor, get_caller_info, warn
from .typing import get_type_name
from .guards import is_list_like
from ._basic import FridReplace
from ._loads import FridParseError, scan_frid_str

PathLike = str|os.PathLike[str]

def parse_quoted_line(s: str, line_num: int) -> tuple[str,None]|tuple[None,str|None]:
    try:
        (value, index) = scan_frid_str(s, 0)
    except FridParseError:
        return (None, s[0])
    rest = s[index:].lstrip()
    if rest and rest[0] != '#':
        warn(f"Extravagant content at the end of line: {rest}")
        return (None, None)
    if not isinstance(value, str):
        warn(f"The value at line number {line_num} is not a string: {get_type_name(value)}")
        return (None, None)
    return (value, None)

def _str_gen(func: Callable):
    while (line := func()):
        yield line

def read_dotenv(data: Iterable[str]) -> dict[str,str|None]:
    replace = FridReplace()
    env = dict(os.environ)
    state: tuple[str,str,str]|None = None
    out: dict[str,str|None] = {}
    for line_num, line in enumerate(data, 1):
        if not isinstance(line, str):
            raise ValueError(f"Invalid data type {get_type_name(line)}")
        if state is None:
            line = line.lstrip()
            if not line or line.startswith('#'):
                continue
            split_line = line.split('=', 1)
            if len(split_line) == 1:
                name = line.strip()
                if name.isidentifier():
                    out[name] = None
                else:
                    stripped = line.removeprefix('\n')
                    warn(f"Invalid line: '{stripped}' at line {line_num}")
                continue
            (name, value) = split_line
            name = name.rstrip()
            if not name.isidentifier():
                warn(f"Invalid environment variable name {name} at line {line_num}")
                continue
            value = value.lstrip()
            if not value:
                out[name] = ''
                continue
            if value[0] not in "'`\"":
                if (index := value.find('#') >= 0):
                    value = value[:index]
                env[name] = out[name] = str(replace(value.rstrip(), env))
                continue
            buffer = value
        else:
            (name, quote, buffer) = state
            state = None
            buffer += line if line.endswith('\n') else line + '\n'
            if quote not in line:
                state = (name, quote, buffer)
                continue
        (value, quote) = parse_quoted_line(buffer, line_num)
        if value is not None:
            env[name] = out[name] = str(replace(value, env))
        elif quote is not None:
            state = (name, quote, buffer)
    if state is not None:
        (name, quote, _) = state
        warn(f"Unfinished multi-line entry with name={name} and quoted by ({quote}).")
    return out

def _dotenv_start_path(name: str, /, *, start: PathLike|None=None) -> PathLike|None:
    """Find the dot-env fule name as given by name.
    - The difference from `find_in_ancestor()` is that the default start
      directory is not the current directory. There are three ways to
      determine the starting directory:
        + The current directory: use explicit `start=os.getcwd()`.
        + The sys.path[0] directory: use explicit `start=sys.path[0]`.
        + Use the caller's parent directory: use the default `start=None`.
    - If `strict` is true, search only the given directory.
    """
    if start is not None:
        return start
    caller = get_caller_info(squash_file=True)
    if caller is None:
        return None
    return os.path.dirname(caller[0])

def find_dotenv(name: str, /, **kwargs) -> str|None:
    (path, _) = os.path.split(name)
    if path:
        # This name has a path component; handled as a fixed file path
        if os.path.isfile(name):
            return name
        if os.path.exists(name):
            warn("The path exists but is not a file: " + name)
            return None
        warn("The file does not exist is not a bare file name: " + path)
        return None
    # Only search file if the file does not contain separator
    start = _dotenv_start_path(name, **kwargs)
    if start is None:
        info("Cannot find the start point to search for the dotenv " + name)
        return None
    path = find_in_ancestor(name, start)
    if path is None:
        info(f"Cannot find the dotenv {name} from the directory or ancesters of {start}")
        return None
    info(f"Searching for {name} starting at {start}, found {path}")
    return path

def load_dotenv(file: str|os.PathLike[str]|Sequence[str]|TextIO=".env",
                /, *, force: bool=False, echo: bool=False, **kwargs) -> int:
    if isinstance(file, str):
        source = find_dotenv(file, **kwargs)
        if source is None:
            return 0
        with open(source, 'rt') as fp:
            data = read_dotenv(fp)
    elif isinstance(file, os.PathLike):
        if not os.path.isfile(file):
            info(f"The file path {file} does not exist or is not a file.")
            return 0
        source = file
        with open(source, 'rt') as fp:
            data = read_dotenv(fp)
    elif is_list_like(file, str):
        data = read_dotenv(file)
        source = f"[a list of {len(file)} object]"
    elif hasattr(file, 'readline') and callable(f := getattr(file, 'readline')):
        data = read_dotenv(_str_gen(f))
        source = "[a file object]"
    else:
        raise ValueError(f"Invalid first argument type: {type(file)}")
    count = 0
    for k, v in data.items():
        if v is not None and (force or os.getenv(k) is None):
            os.environ[k] = v  # Note: do not use os.putenv() which only affects subprocesses
            assert os.getenv(k) == v, f"Name {k} cannot be saved in environment"
            count += 1
            if echo:
                print(f"{k} = {v}")
        elif echo:
            print(f"(Skipped) {k} = {v}")
    info(f"Loaded {len(data)} and updated {count} envvars from {source}")
    return count

if __name__ == '__main__':
    import sys
    from .lib import set_root_logging
    set_root_logging("info")
    # Since this file is the caller and main module, we have to use getcwd.
    start = os.getcwd()
    name = None
    info("Usage: python -m frid.dotenv [NAME [DIR-DIR]]")
    if len(sys.argv) > 1:
        name = sys.argv[1]
        if len(sys.argv) > 2:
            start = sys.argv[2]
    load_dotenv(echo=True, start=start)
