import os
import sys
import re

if sys.version_info >= (3, 13):
	import glob
else:
	import glob2 as glob

class Filter:
	'''Object that holds a parsed filter string for quicker file filtering.'''

	patterns : list[tuple[bool, str|re.Pattern]]

	@classmethod
	def _tokenize(cls, s:str, *, is_glob:bool, glob_is_escaped:bool):
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
	def _convert_to_glob_string(cls, pattern:str, *, is_glob:bool, glob_is_escaped:bool):
		glob_string = ""
		escape = False
		for c in pattern:
			if c == "\\":
				escape = True
			elif c in ["*", "?", "["]:
				if is_glob:
					if escape:
						if glob_is_escaped:
							glob_string += glob.translate(c)
						else:
							glob_string += "\\"
							glob_string += c
						escape = False
					else:
						glob_string += c
				else:
					if escape:
						glob_string += "\\"
						escape = False
					glob_string += c
			else:
				if escape:
					glob_string += "\\"
					escape = False
				glob_string += c
		return glob_string

	@classmethod
	def _parse_pattern(cls, action:bool, pattern:str, *, ignore_hidden:bool, ignore_case:bool, is_glob:bool, glob_is_escaped:bool):
		sep = f"{os.sep}/"
		if pattern[:2] == f".{os.sep}" or pattern[:2] == "./":
			pattern = pattern[2:].lstrip(sep)
		if pattern:
			# don't allow escaped lone - and +
			# \- and \+ are valid filenames in Linux
			# and Windows shouldn't have a unique escape sequence if it can be avoided
			if pattern == r"\-" or pattern == r"\+":
				raise ValueError(f"Pattern {pattern} is invalid. You probably meant: ./{pattern[1]}")
			if pattern == ".." or re.search(rf"^\.\.[{sep}]", pattern) or re.search(rf"[{sep}]\.\.[{sep}]", pattern) or re.search(rf"[{sep}]\.\.$", pattern):
				raise ValueError(f"Parent directories ('..') are not supported in filter: {pattern}")
			glob_pattern = Filter._convert_to_glob_string(pattern, is_glob=is_glob, glob_is_escaped=glob_is_escaped)
			if os.path.isabs(glob_pattern) or glob_pattern[0] in sep:
				# just assume anything starting with a path separator is an absolute path
				raise ValueError(f"Absolute paths are not supported in filter: {pattern}")
			regex = glob.translate(glob_pattern, recursive=True, include_hidden=(not ignore_hidden))
			matcher = re.compile(regex, flags=re.IGNORECASE if ignore_case else 0)
			return (action, matcher)
		else:
			return None

	def __init__(self, filter_string:str = "", *, ignore_hidden:bool = False, ignore_case:bool = False, is_glob:bool = True, glob_is_escaped:bool = False, default:bool = False):
		'''
		Args
			filter_string      (str) : The filter string that includes/excludes file system entries. Similar to rsync, the format of the filter string is one of more repetitions of: (+ or -), followed by a list of one of more relative path patterns. Including (+) or excluding (-) of file system entries is determined by the preceding symbol of the first matching pattern. Included files will be copied over as part of the backup, while included directories will be searched. Each pattern ending with "/" will apply to directories only. Otherise the pattern will apply only to files.
			ignore_hidden     (bool) : Whether to ignore hidden files by default in glob patterns. If `True`, then wildcards in glob patterns will not match file system entries beginning with a dot. However, globs containing a dot (e.g., "**/.*") will still match these file system entries. (Defaults to `False`.)
			ignore_case       (bool) : Whether to ignore case when comparing files to the filter string. (Defaults to `False`.)
			is_glob           (bool) : Whether the filter string is a glob string. (Defaults to `True`.)
			glob_is_escaped   (bool) : Whether glob characters in the filter string are escaped (with "\\"), indicating that they should be interpretted as literals.
			default           (bool) : The default value for non-matching files and directories. (Defaults to `False`.)
		'''
		self.ignore_hidden   = ignore_hidden
		self.ignore_case     = ignore_case
		self.is_glob         = is_glob
		self.glob_is_escaped = glob_is_escaped
		self.default         = default

		self.implicit_dirs: set[str] = set()
		self.filters : list[tuple[bool, re.Pattern]] = []

		if filter_string:
			action = True
			for token in PathFilter._tokenize(filter_string, is_glob=is_glob, glob_is_escaped=glob_is_escaped):
				if token == True:
					action = True
				elif token == False:
					action = False
					# self.implicit_dirs = set()
				elif action:
					self.allow(token)
				else:
					self.reject(token)

	def allow(self, *patterns, ignore_hidden:bool|None = None, ignore_case:bool|None = None, is_glob:bool|None = None, glob_is_escaped:bool|None = None) -> "PathFilter":
		for pattern in patterns:
			filter = Filter._parse_pattern(True, pattern,
				ignore_hidden = ignore_hidden if ignore_hidden is not None else self.ignore_hidden,
				ignore_case = ignore_case if ignore_case is not None else self.ignore_case,
				is_glob = is_glob if is_glob is not None else self.is_glob,
				glob_is_escaped = glob_is_escaped if glob_is_escaped is not None else self.glob_is_escaped,
			)
			if filter:
				self.filters.append(filter)

				pattern = pattern.rstrip("/").rstrip(os.sep)
				while True:
					pattern = os.path.dirname(pattern)
					if pattern == "" or pattern == os.sep or pattern == "/":
						break
					if pattern in self.implicit_dirs:
						break
					self.implicit_dirs.add(pattern)
					filter = Filter._parse_pattern(True, pattern + "/",
						ignore_hidden = ignore_hidden if ignore_hidden is not None else self.ignore_hidden,
						ignore_case = ignore_case if ignore_case is not None else self.ignore_case,
						is_glob = is_glob if is_glob is not None else self.is_glob,
						glob_is_escaped = glob_is_escaped if glob_is_escaped is not None else self.glob_is_escaped,
					)
					if filter:
						self.filters.append(filter)
					else:
						break
		return self

	def reject(self, *patterns, ignore_hidden:bool|None = None, ignore_case:bool|None = None, is_glob:bool|None = None, glob_is_escaped:bool|None = None) -> "Filter":
		for pattern in patterns:
			filter = Filter._parse_pattern(False, pattern,
				ignore_hidden = ignore_hidden if ignore_hidden is not None else self.ignore_hidden,
				ignore_case = ignore_case if ignore_case is not None else self.ignore_case,
				is_glob = is_glob if is_glob is not None else self.is_glob,
				glob_is_escaped = glob_is_escaped if glob_is_escaped is not None else self.glob_is_escaped,
			)
			if filter:
				self.filters.append(filter)
				self.implicit_dirs = set()
		return self

	def filter(self, relpath:str, default:bool|None = None) -> bool:
		'''Compare the file path against the filter string.'''

		for action, matcher in self.filters:
			if matcher.match(relpath):
				return action
		return default if default is not None else self.default