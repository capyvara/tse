from __future__ import annotations

try:
    import regex as re
except ImportError as e:
    import re

import datetime
import functools
from typing import Callable, Dict, Iterable, Any, Tuple, Union

from .lru import LRU

# https://github.com/garyelephant/pygrok
class GrokProcessor:
    Converter = Callable[[str], Any]

    class Matcher:
        regex: re.Pattern
        type_map: dict[str, str]
        can_cache_format: bool

        def __init__(self, regex: re.Pattern, type_map: dict[str, str], can_cache_format):
            self.regex = regex
            self.type_map = type_map
            self.can_cache_format = can_cache_format

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
    _matcher_format_cache: dict[(Matcher, bool), str]
    _noMatchCache: set
    _matchCache: LRU[str, Matcher]

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
        self._string_lru_cache = functools.lru_cache(512)(self.__string_lru_cache)
        self._invalidate_caches()

    def _invalidate_caches(self):
        self._matcher_format_cache = {}
        self._noMatchCache = set()
        self._matchCache = LRU(1024)
        self._string_lru_cache.cache_clear()

    def add_matchers(self, matchers: Iterable[str], flags: re.RegexFlag = 0) -> GrokProcessor:
        self._matchers.extend((self._build_matcher(p, flags) for p in matchers))
        self._invalidate_caches()
        return self

    def load_matchers_from_file(self, file: str, flags: re.RegexFlag = 0) -> GrokProcessor:
        with open(file, 'r', encoding='utf-8') as file:
            for line in (l.strip() for l in file):
                if line == '':
                    continue

                can_cache_format = True

                if line.startswith(r"\\"):
                    line = line[1:]
                    can_cache_format = False
                else:
                    line = re.sub(r"%\\{([:\w]+)\\}", r"%{\1}", re.escape(line, literal_spaces=True))

                self._matchers.append(self._build_matcher(line, can_cache_format, flags))

        self._invalidate_caches()
        return self

    def _build_matcher(self, pattern: str, can_cache_format = False, flags: re.RegexFlag = 0) -> Matcher:
        iterations = 100

        type_map = {}

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
            pattern, num_subs = re.subn(r'%{(\w+)}',
                lambda m: "(" + self._predefined[m.group(1)] + ")",
                pattern)

            if num_subs > 0:
                can_cache_format = False
            
            if re.search('%{\w+(:\w+)?(:\w+)?}', pattern) is None:
                return GrokProcessor.Matcher(re.compile(pattern, flags), type_map, can_cache_format)

    # Reuse common strings for field names
    def __string_lru_cache(self, string: str) -> str:
        return string

    def match(self, text: str, *, fullmatch=True, pos_msg_params=False) -> Tuple[str, Union[dict, list]]:
        if text in self._noMatchCache:
            return (text, None)

        def try_matcher(matcher: GrokProcessor.Matcher):
            match = matcher.regex.fullmatch(text, concurrent=True) if fullmatch else matcher.regex.match(text, concurrent=True)
            if not match:
                return (False, (text, None))

            matcherkey = (matcher, pos_msg_params)
            params = match.groupdict()
            format = None

            def process_param(key, value):
                if not value:
                    return
                try:
                    type = matcher.type_map[key]
                    converter = self._type_converters[type]
                    params[key] = converter(value)
                except KeyError:
                    if len(value) < 32:
                        params[key] = self._string_lru_cache(value)

            if matcher.can_cache_format and matcherkey in self._matcher_format_cache:
                format = self._matcher_format_cache[matcherkey]
                for key, value in list(params.items()):
                    if key == "__del__":
                        del params[key]
                        continue

                    process_param(key, value)
            else:
                split = []
                lbound = 0
                for key, value, l, r in [(key, value, *match.span(key)) for key, value in params.items()]:                        
                    if key == "__del__":
                        del params[key]
                        if l > 0:
                            split.append(text[lbound:l])
                            lbound = r
                        continue

                    process_param(key, value)

                    if l > 0:
                        split.append(text[lbound:l])
                        lbound = r

                        if pos_msg_params:
                            split.append("%s")
                        else:
                            split.append(f"%({key})s")

                split.append(text[lbound:])
                format = "".join(split)

                if matcher.can_cache_format:
                    self._matcher_format_cache[matcherkey] = format

            return (True, (format, params if not pos_msg_params else list(params.values())))

        cachedMatcher = None
        try:
            cachedMatcher = self._matchCache[text]
            res, ret = try_matcher(cachedMatcher)
            if res:
                return ret
        except KeyError:
            pass

        for i in range(0, len(self._matchers)):
            matcher = self._matchers[i]
            if matcher == cachedMatcher:
                continue

            res, ret = try_matcher(self._matchers[i])
            if res:
                # Optimization: Bubble up the matched one so most common goes first in the list
                if i > 0:
                    self._matchers[i - 1], self._matchers[i] = self._matchers[i], self._matchers[i - 1]

                self._matchCache[text] = matcher
                return ret            

        self._noMatchCache.add(text)
        return (text, None)
