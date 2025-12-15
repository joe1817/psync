# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import os
import sys
import re
import glob
from dataclasses import dataclass
from collections import namedtuple

from .types import _AbstractPath
from .errors import StateError
from .log import logger

class Filter:
	'''Abstract base class for all file filtering objects supplied to `Sync`.'''

	def __init__(self, default:bool = False):
		'''Initialize a `Filter` object.'''

		self.default = default

	def filter(self, relpath:str, *, root:_AbstractPath|str|None = None, default:bool|None = None) -> bool:
		'''(Abstract method) Filter path.'''

		raise NotImplementedError()

class PathFilter(Filter):
	'''Filter that allows or rejects based on file path string.'''

	if os.sep == "\\":
		seps = r"\\/" # regex pattern is deliberate
	else:
		seps = "/"

	@dataclass
	class _Segment:
		'''The building blocks of a `PathFilter`, built from a pattern string and action. Each file path will be compared to a list of these, and the first one that matches will decide whether the file is allowed or rejected.'''

		glob_pattern : str  # The pattern string is converted to a glob string and stored here
		action       : bool # Whether to allow (True) or reject (False)
		matcher      : re.Pattern # Compiled regular expression will do the matching
		is_relative  : bool # segment pattern originally began with "./"
		is_implicit  : bool # segment was automatically generated

		def __repr__(self):
			return f"({self.glob_pattern}, act={self.action}, rel={self.is_relative}, imp={self.is_implicit})"

		def __str__(self):
			return self.glob_pattern

	@classmethod
	def _tokenize(cls, s:str, *, is_glob:bool, glob_is_escaped:bool):
		'''Yield tokens from a filter string, usually coming from the command line.'''

		escape   :bool = False # used to turn backslash, glob chars, whitespace, quotes into literals; backslash is treated literally for invalid escape sequences
		s_quotes :bool = False # treat everything within single quotes as literal
		d_quotes :bool = False # double quotes treat whitespace and single quotes as literal only
		token    :str  = ""
		tokstart :int = 0   # used to tell if a - or + token were surrounded by quotes

		if "\0" in s:
			raise ValueError("Invalid null character in filter string")
		s += "\0"

		for i, char in enumerate(s):

			if char.strip() == "":
				if escape:
					if d_quotes:
						# mimic bash, treat backslash literally
						token += "\\"
					token += char
					escape = False
				elif s_quotes:
					token += char
				elif d_quotes:
					token += char
				elif token:
					if token == "+":
						yield True if i == tokstart+1 else "+"
					elif token == "-":
						yield False if i == tokstart+1 else "-"
					else:
						yield token
					token = ""
					tokstart = i+1
			elif char == "\0":
				if escape:
					raise ValueError("Unterminated escape sequence in filter string")
				elif s_quotes:
					raise ValueError("Unclosed quotes in filter string")
				elif d_quotes:
					raise ValueError("Unclosed quotes in filter string")
				elif token:
					if token == "+":
						yield True if i == tokstart+1 else "+"
					elif token == "-":
						yield False if i == tokstart+1 else "-"
					else:
						yield token
					token = ""
					tokstart = i+1
			elif char == "\\":
				if escape:
					token += "\\"
					escape = False
				elif s_quotes:
					token += "\\"
				else:
					escape = True
			elif char in ["*", "?", "["]:
				if escape:
					if is_glob:
						if glob_is_escaped:
							token += glob.escape(char)
						else:
							token += "\\"
							token += char
					else:
						token += "\\"
						token += char
					escape = False
				elif s_quotes:
					token += glob.escape(char)
				else:
					token += char
			elif char == "'":
				if escape:
					if d_quotes:
						# mimic bash, treat backslash literally
						token += "\\"
					token += "'"
					escape = False
				elif s_quotes:
					s_quotes = False
				elif d_quotes:
					token += "'"
				else:
					s_quotes = True
			elif char == "\"":
				if escape:
					assert not s_quotes
					token += "\""
					escape = False
				elif d_quotes:
					d_quotes = False
				elif s_quotes:
					token += "\""
				else:
					d_quotes = True
			else:
				if escape:
					# mimic bash, treat backslash literally
					token += "\\"
					escape = False
				token += char

	@classmethod
	def _parse_pattern(cls, action:bool, pattern:str, *, ignore_hidden:bool, ignore_case:bool, is_glob:bool, glob_is_escaped:bool, is_dir:bool|None):
		'''Create a new `Segment` from the pattern string.'''

		is_relative = False
		if pattern[:2] == f".{os.sep}" or pattern[:2] == "./":
			is_relative = True
			if glob_is_escaped:
				# consider anything other than a single backslash next to a glob char to be repeated path separators
				pattern = re.sub(rf"^([{PathFilter.seps}](?![*?[]))|([{PathFilter.seps}]{{2,}})", "", pattern[2:])
			else:
				pattern = pattern[2:].lstrip(PathFilter.seps)
		if pattern:
			# don't allow escaped lone - or +
			# \- and \+ are valid filenames in Linux
			# and Windows shouldn't have a unique escape sequence if it can be avoided
			if pattern == r"\-" or pattern == r"\+":
				raise ValueError(f"Pattern {pattern} is invalid. You probably meant: ./{pattern[1]}")
			# unescape glob chars
			glob_pattern = PathFilter._convert_to_glob_string(pattern, is_glob=is_glob, glob_is_escaped=glob_is_escaped)
			# don't allow . or .. segments
			if glob_pattern == ".." or re.search(rf"^\.\.?[{PathFilter.seps}]", glob_pattern) or re.search(rf"[{PathFilter.seps}]\.\.?[{PathFilter.seps}]", glob_pattern) or re.search(rf"[{PathFilter.seps}]\.\.?$", glob_pattern):
				raise ValueError(f". and .. path references are not supported (except for paths starting with ./): {pattern}")
			# collapse repeated slashes
			glob_pattern = re.sub(rf"[{PathFilter.seps}]+", "/", glob_pattern)
			# don't allow absolute paths
			if os.path.isabs(glob_pattern) or glob_pattern[0] in PathFilter.seps:
				# just assume anything starting with a path separator is an absolute path
				raise ValueError(f"Absolute paths are not supported in filter: {pattern}")
			# handle trailing slash
			if is_dir is not None:
				count = len(glob_pattern) - len(glob_pattern.rstrip(PathFilter.seps))
				if is_dir and count%2 == 0:
					glob_pattern += "/"
				elif not is_dir and count%2:
					glob_pattern = glob_pattern[:-1]
			# convert to regex
			regex = glob.translate(glob_pattern, recursive=True, include_hidden=(not ignore_hidden))
			matcher = re.compile(regex, flags=re.IGNORECASE if ignore_case else 0)
			return PathFilter._Segment(glob_pattern=glob_pattern, action=action, matcher=matcher, is_relative=is_relative, is_implicit=False)
		else:
			return None

	@classmethod
	def _convert_to_glob_string(cls, pattern:str, *, is_glob:bool, glob_is_escaped:bool):
		'''Convert a pattern string, which may or may not include glob characters, into a glob string with appropraite glob characters escaped.'''

		if not is_glob and glob_is_escaped:
			raise ValueError(f"Incompatible arguments: is_glob==False, glob_is_escaped==True")
		glob_string = ""
		escape = False
		for c in pattern:
			if c == "\\":
				if glob_is_escaped:
					if escape:
						glob_string += "\\"
						escape = False
					else:
						escape = True
				else:
					glob_string += "\\"
			elif is_glob and c in ["*", "?", "["]:
				if escape:
					glob_string += glob.translate(c)
				else:
					glob_string += c
			else:
				if escape:
					glob_string += "\\"
					escape = False
				glob_string += c
		if escape:
			glob_string += "\\"
		return glob_string

	def __init__(self, filter_string:str = "**", *, ignore_hidden:bool = False, ignore_case:bool = (os.name=="nt"), is_glob:bool = True, glob_is_escaped:bool = False):
		'''
		Initialize a Filter object.

		Args
			filter_string      (str) : The filter string that allows/rejects file system entries. Similar to rsync, the format of the filter string is one of more repetitions of: (+ or -), followed by a list of one of more relative path patterns. Allowing (+) or rejecting (-) of file system entries is determined by the preceding symbol of the first matching pattern. Included files will be copied over as part of the backup, while included directories will be searched. Each pattern ending with "/" will apply to directories only. Otherise the pattern will apply only to files. Patterns that start with "./" will be appended to previous directory patterns in the same "allow" group. For example, "+ a/ - b/ + c/ d/ ./1" is equivalent to "+ a/ - b/ + c/ d/ c/1 d/1".
			ignore_hidden     (bool) : Whether to ignore hidden files by default in glob patterns. If `True`, then wildcards in glob patterns will not match file system entries beginning with a dot. However, globs containing a dot (e.g., "**/.*") will still match these file system entries. (Defaults to `False`.)
			ignore_case       (bool) : Whether to ignore case when comparing files to the filter string. (Defaults to `False`.)
			is_glob           (bool) : Whether the filter string is a glob string. (Defaults to `True`.)
			glob_is_escaped   (bool) : Whether glob characters in the filter string are escaped (with "\\"), indicating that they should be interpretted as literals.
			default           (bool) : The default value for non-matching files and directories. (Defaults to `False`.)
		'''
		super().__init__()

		self.ignore_hidden   = ignore_hidden
		self.ignore_case     = ignore_case
		self.is_glob         = is_glob
		self.glob_is_escaped = glob_is_escaped

		self._segments : list[PathFilter._Segment] = []
		self._tmp_allowed : set[str] = set() # directories implied when allowing an entry with multiple path segments

		if filter_string:
			action = True
			for token in PathFilter._tokenize(filter_string, is_glob=is_glob, glob_is_escaped=glob_is_escaped):
				if token == True:
					action = True
				elif token == False:
					action = False
				elif action:
					self.allow(token)
				else:
					self.reject(token)

	def allow(self, *patterns, ignore_hidden:bool|None = None, ignore_case:bool|None = None, is_glob:bool|None = None, glob_is_escaped:bool|None = None, is_dir:bool|None = None) -> "PathFilter":
		'''Add an allow segment to the `Filter`.'''

		for pattern in patterns:
			for segment in self._get_segments(True, pattern,
				ignore_hidden = ignore_hidden if ignore_hidden is not None else self.ignore_hidden,
				ignore_case = ignore_case if ignore_case is not None else self.ignore_case,
				is_glob = is_glob if is_glob is not None else self.is_glob,
				glob_is_escaped = glob_is_escaped if glob_is_escaped is not None else self.glob_is_escaped,
				is_dir = is_dir,
			):
				self._tmp_allowed.add(segment.glob_pattern)
				self._segments.append(segment)
		return self

	def reject(self, *patterns, ignore_hidden:bool|None = None, ignore_case:bool|None = None, is_glob:bool|None = None, glob_is_escaped:bool|None = None, is_dir:bool|None = None) -> "PathFilter":
		'''Add a rejection segment to the `Filter`.'''

		self._tmp_allowed = set()
		for pattern in patterns:
			for segment in self._get_segments(False, pattern,
				ignore_hidden = ignore_hidden if ignore_hidden is not None else self.ignore_hidden,
				ignore_case = ignore_case if ignore_case is not None else self.ignore_case,
				is_glob = is_glob if is_glob is not None else self.is_glob,
				glob_is_escaped = glob_is_escaped if glob_is_escaped is not None else self.glob_is_escaped,
				is_dir = is_dir,
			):
				self._segments.append(segment)
		return self

	def _get_segments(self, action:bool, pattern:str, *, ignore_hidden:bool, ignore_case:bool, is_glob:bool, glob_is_escaped:bool, is_dir:bool|None):
		'''Get all segments from this pattern, including implicit ones.'''

		segment = PathFilter._parse_pattern(action, pattern, ignore_hidden=ignore_hidden, ignore_case=ignore_case, is_glob=is_glob, glob_is_escaped=glob_is_escaped, is_dir=is_dir)
		if segment:
			if segment.is_relative and not segment.action:
				raise ValueError("Relative patterns can only be used with a + action")

			if not segment.is_relative:
				yield segment

				# Include implicit parent directories.
				if segment.action:
					pattern = pattern.rstrip("/").rstrip(os.sep)
					while True:
						pattern = os.path.dirname(pattern)
						if pattern == "" or pattern == os.sep or pattern == "/":
							break
						segment = PathFilter._parse_pattern(True, pattern,
							ignore_hidden = ignore_hidden if ignore_hidden is not None else self.ignore_hidden,
							ignore_case = ignore_case if ignore_case is not None else self.ignore_case,
							is_glob = is_glob if is_glob is not None else self.is_glob,
							glob_is_escaped = glob_is_escaped if glob_is_escaped is not None else self.glob_is_escaped,
							is_dir = True,
						)
						assert segment.glob_pattern.endswith("/") or segment.glob_pattern.endswith("\\")
						if segment.glob_pattern in self._tmp_allowed:
							break
						if segment:
							assert not segment.is_relative
							segment.is_implicit = True
							yield segment
						else:
							break
			else:
				# Combine relative segment with parent dirs.
				# If there are no parent dirs then assume relative to root dir. Don't throw away the segment.
				segment_handled = False
				for parent in reversed(self._segments):
					if not parent.action:
						break
					if not parent.is_implicit and parent.glob_pattern.endswith("/"):
						segment_handled = True
						pattern = parent.glob_pattern + segment.glob_pattern
						for new_segment in self._get_segments(True, pattern,
							ignore_hidden = ignore_hidden,
							ignore_case = ignore_case,
							is_glob = True,
							glob_is_escaped = False,
							is_dir = is_dir,
						):
							new_segment.is_implicit = True
							yield new_segment
				if not segment_handled:
					segment.is_implicit = True
					yield segment

	def filter(self, relpath:str, *, root:_AbstractPath|str|None = None, default:bool|None = None) -> bool:
		'''Filter paths by comparing them against the filter string.'''

		for segment in self._segments:
			if segment.matcher.match(relpath):
				return segment.action
		return default if default is not None else self.default

	def __str__(self) -> str:
		_str = ""
		current_action: bool|None = None
		for seg in self._segments:
			if _str:
				_str += " "
			if seg.action != current_action:
				_str += "+ " if seg.action else "- "
				current_action = seg.action
			_str += seg.glob_pattern
		return _str

	def __repr__(self) -> str:
		return "[" + ", ".join(repr(seg) for seg in self._segments) + "]"

class AllFilter(Filter):
	'''Filter that allows or rejects based on child Filters.'''

	def __init__(self, *filters:Filter):
		'''Initialize an `AllFilter` object.'''

		super().__init__()

		if not filters:
			raise ValueError("Missing required argument: filters")
		self.filters = filters

	def filter(self, relpath:str, *, root:_AbstractPath|str|None = None, default:bool|None = None) -> bool:
		'''Filter paths by accepting only those that pass all constiuent `Filters`.'''

		if all(f.filter(relpath, root=root) for f in self.filters):
			return True
		else:
			return default if default is not None else self.default
