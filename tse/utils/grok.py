try:
    import regex as re
except ImportError as e:
    import re

import datetime
import functools
from typing import Callable, Dict, Iterable, Any, Tuple

# https://github.com/garyelephant/pygrok
class GrokProcessor:
    Converter = Callable[[str], Any]

    class Matcher():
        regex: re.Pattern
        type_map: dict[str, str]

        def __init__(self, regex: re.Pattern, type_map: dict[str, str]):
            self.regex = regex
            self.type_map = type_map

    # https://github.com/garyelephant/pygrok/blob/master/pygrok/patterns/grok-patterns
    _base_predefined: dict[str, str] = {
        "INT": r"(?:[+-]?(?:[0-9]+))",
        "BASE10NUM": r"(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\.[0-9]+)?)|(?:\.[0-9]+)))",
        "BASE16NUM": r"(?<![0-9A-Fa-f])(?:[+-]?(?:0x)?(?:[0-9A-Fa-f]+))",
        "NUMBER": r"(?:%{BASE10NUM})",
        "POSINT": r"\b(?:[1-9][0-9]*)\b",
        "NONNEGINT": r"\b(?:[0-9]+)\b",
        "WORD": r"\b\w+\b",
        "NOTSPACE": r"\S+",
        "SPACE": r"\s*",
        "DATA": r".*?",
        "GREEDYDATA": r".*",
        "YEAR": r"(?>\d\d){1,2}",
        "MONTHNUM": r"(?:0?[1-9]|1[0-2])",
        "MONTHDAY": r"(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])",
        "HOUR": r"(?:2[0123]|[01]?[0-9])",
        "MINUTE": r"(?:[0-5][0-9])",
        "SECOND": r"(?:[0-5][0-9])",
        "TIME": r"(?!<[0-9])%{HOUR}:%{MINUTE}(?::%{SECOND})(?![0-9])",
        "DATE_BR": r"%{MONTHDAY}[./-]%{MONTHNUM}[./-]%{YEAR}",
        "VERSION4": r"(?:(\d+)\.)?(?:(\d+)\.)(?:(\d+)\.)?(\*|\d+)",
        "PATH": r"(?:%{UNIXPATH}|%{WINPATH})",
        "UNIXPATH": r"(/([\w_%!$@:.,~-]+|\\.)*)+",
        "WINPATH": r"(?>[A-Za-z]+:|\\)(?:\\[^\\?*]*)+",
    }

    _predefined: dict[str, str]
    _type_converters = dict[str, Converter]
    _matchers: list[Matcher]

    _base_type_converters : dict[str, Converter] = {
        "int": lambda x: int(x),
        "base16int": lambda x: int(x, 16),
        "float": lambda x: float(x),
        "bool": lambda x: bool(x),
        "datebr": lambda x: datetime.datetime.strptime(x, "%d/%m/%Y").date(),
        "time": lambda x: datetime.datetime.strptime(x, "%H:%M:%S").time(),
    }

    def __init__(self, predefined: dict[str, str] = {}, type_converters: dict[str, Converter] = {}):
        self._predefined = self._base_predefined | predefined
        self._type_converters = self._base_type_converters | type_converters
        self._matchers = []
        self._matcher_format_cache = {}

    def _build_grok(self, pattern: str, flags: re.RegexFlag = 0) -> Matcher:
        iterations = 100

        type_map = {}
        pattern = re.sub(r"%\\{([:\w]+)\\}", r"%{\1}", re.escape(pattern))

        while True:
            if iterations == 0:
                raise RecursionError()

            iterations -= 1

            type_map.update({n[1]: n[2] for n in re.findall(r'%{(\w+):(\w+):(\w+)}', pattern)})

            # Named group
            pattern = re.sub(r'%{(\w+):(\w+)(?::\w+)?}',
                lambda m: "(?P<" + m.group(2) + ">" + self._predefined[m.group(1)] + ")",
                pattern)

            # Unnamed group
            pattern = re.sub(r'%{(\w+)}',
                lambda m: "(" + self._predefined[m.group(1)] + ")",
                pattern)
            
            if re.search('%{\w+(:\w+)?(:\w+)?}', pattern) is None:
                return GrokProcessor.Matcher(re.compile(pattern, flags), type_map)

    def add_matchers(self, matchers: Iterable[str], flags: re.RegexFlag = 0):
        self._matchers.extend((self._build_grok(p, flags) for p in matchers))

    def load_matchers_from_file(self, file: str, flags: re.RegexFlag = 0):
        with open(file, 'r', encoding='utf-8') as file:
            for line in (l.strip() for l in file):
                if line == '' or line.startswith('#'):
                    continue

                self._matchers.append(self._build_grok(line, flags))

    @functools.lru_cache(maxsize=512, typed=True)
    def _string_lru_cache(self, string: str) -> str:
        return string

    def match(self, text: str, *, fullmatch=True, positional=False) -> Tuple[str, dict]:
        for i in range(0, len(self._matchers)):
            matcher = self._matchers[i]

            match = matcher.regex.fullmatch(text) if fullmatch else matcher.regex.match(text)
            if match:
                params = match.groupdict()

                split = []
                lbound = 0
                for key, value, l, r in [(key, value, *match.span(key)) for key, value in params.items()]:
                    if key == "__del__":
                        del params[key]
                        split.append(text[lbound:l])
                        lbound = r
                        continue

                    try:
                        type = matcher.type_map[key]
                        converter = self._base_type_converters[type]
                        params[key] = converter(params[key])
                    except KeyError:
                        if len(value) < 32:
                            params[key] = self._string_lru_cache(value)

                    split.append(text[lbound:l])
                    lbound = r

                    if positional:
                        split.append("%s")
                    else:
                        split.append(f"%({key})s")

                split.append(text[lbound:])
                format = "".join(split)

                # Optimization: Bubble up the matched one so most common goes first in the list
                if i > 0:
                    self._matchers[i - 1], self._matchers[i] = self._matchers[i], self._matchers[i - 1]

                return (format, params if not positional else list(params.values()))

        return (text, None)

