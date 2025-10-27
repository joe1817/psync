# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import sys
import os
import re
import stat
import shutil
import logging
import tempfile
from pathlib import Path
from itertools import islice
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Literal, Final, Iterator

if sys.version_info >= (3, 13):
	import glob
else:
	import glob2 as glob

from .sftp import RemotePath, _RemotePathScanner

logger = logging.getLogger("psync")

class _DebugInfoFilter(logging.Filter):
	'''Logging filter that only allows DEBUG and INFO records to pass.'''
	def filter(self, record):
		return logging.DEBUG <= record.levelno <= logging.INFO

class _NonEmptyFilter(logging.Filter):
    '''Logging filter that only allows non-empty messages.'''
    def filter(self, record):
        return bool(str(record.msg).strip())

class _LogFileFormatter(logging.Formatter):
	#BASE_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
	BASE_FORMAT = "%(message)s"

	def __init__(self, fmt=BASE_FORMAT, datefmt=None, style="%"):
		super().__init__(fmt, datefmt, style)

	def format(self, record):
		original_message = record.msg
		if record.levelno == logging.DEBUG:
			record.msg = f"\t{original_message}"
		elif record.levelno == logging.INFO:
			pass
		elif record.levelno == logging.WARNING:
			record.msg = f"WARN: {original_message}"
		elif record.levelno == logging.ERROR:
			record.msg = f"ERROR: {original_message}"
		elif record.levelno == logging.CRITICAL:
			record.msg = f"\n*** {original_message} ***\n"
		formatted_message = super().format(record)
		return formatted_message

class _InputError(ValueError):
	''' Error that indicates the root cause was due to bad input and that a stack trace does not need to be logged. '''
	pass

class _Filter:
	'''Object that holds a parsed filter string for quicker file filtering.'''

	patterns : list[tuple[bool, re.Pattern]]

	@classmethod
	def tokenize(cls, s:str):
		escape   :bool = False # used to turn backslash, glob chars, whitespace, quotes into literals; backslash is treated literally for invalid escape sequences
		s_quotes :bool = False # treat everything within single quotes as literal
		d_quotes :bool = False # double quotes treat whitespace and single quotes as literal only
		token    :str  = ""
		tokstart :int = 0   # used to tell if a - or + token were surrounded by quotes

		if "\0" in s:
			raise _InputError("Invalid null character in filter string")
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
					raise _InputError("Unterminated escape sequence in filter string")
				elif s_quotes:
					raise _InputError("Unclosed quotes in filter string")
				elif d_quotes:
					raise _InputError("Unclosed quotes in filter string")
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
					token += glob.escape(char)
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
					#raise _InputError(f"Unrecognized esacpe sequence: \\{char}")
				token += char

	def __init__(self, filter_string:str, *, ignore_hidden:bool = False, ignore_case:bool = False):
		self.patterns = []
		implicit_dirs: set[str] = set()

		action = True

		for token in _Filter.tokenize(filter_string):
			if token == True:
				action = True
			elif token == False:
				action = False
				implicit_dirs = set()
			else:
				if token[:2] == ".\\" or token[:2] == "./":
					token = token[2:]
				if token:
					if token == ".." or re.search(r"^\.\.[\\/]", token) or re.search(r"[\\/]\.\.[\\/]", token) or re.search(r"[\\/]\.\.$", token):
						raise _InputError(f"Parent directories ('..') are not supported in pattern arguments to include/exclude: {token}")
					if os.path.isabs(token):
						raise _InputError(f"Absolute paths are not supported as arguments to include/exclude: {token}")
					regex = glob.translate(token, recursive=True, include_hidden=(not ignore_hidden))
					reobj = re.compile(regex, flags=re.IGNORECASE if ignore_case else 0)
					self.patterns.append((action, reobj))

					# include parent dirs for each include pattern
					if action:
						if token.endswith("\\") or token.endswith("/"):
							token = token[:-1]
						while True:
							token = os.path.dirname(token)
							if token == "":
								break
							if token in implicit_dirs:
								break
							implicit_dirs.add(token)
							regex = glob.translate(token + "/", recursive=True, include_hidden=(not ignore_hidden))
							reobj = re.compile(regex)
							self.patterns.append((action, reobj))

	def filter(self, relpath:str, default:bool = False) -> bool:
		'''Compare the file path against the filter string.'''

		for action, reobj in self.patterns:
			if reobj.match(relpath):
				return action
		return default

@dataclass(frozen=True)
class _Metadata:
	'''File metadata that will be used to find probable duplicates.'''

	size  : int
	mtime : float

@dataclass(frozen=True)
class _ScandirEntry:
	'''Filesystem entries yielded by `_scandir()`.'''

	class Category:
		EMPTY_DIR : Final[str] = "Empty Dir"
		FILE      : Final[str] = "File"

	# mypy gives an error for some reason
	#category : Literal[Category.EMPTY_DIR, Category.FILE]
	category : str
	normpath : str # normcased and replaced \\ -> /
	path     : str
	meta     : _Metadata # TODO: _Metadata | None

@dataclass(frozen=True)
class _Operation:
	'''Filesystem operation yielded by `_operations()`.'''

	class Category:
		CREATE_FILE : Final[str] = "+"
		UPDATE_FILE : Final[str] = "U"
		RENAME_FILE : Final[str] = "R"
		DELETE_FILE : Final[str] = "-"
		TRASH_FILE  : Final[str] = "~"

		CREATE_DIR : Final[str] = "D+"
		DELETE_DIR : Final[str] = "D-"

	# mypy gives an error for some reason
	#category  : Literal[Category.CREATE_FILE, Category.UPDATE_FILE, Category.RENAME_FILE, Category.DELETE_FILE, Category.TRASH_FILE, Category.CREATE_DIR, Category.DELETE_DIR]
	category  : str
	src       : Path | RemotePath | None
	dst       : Path | RemotePath | None
	byte_diff : int
	summary   : str

@dataclass
class Results:
	'''Various statistics and other information returned by `sync()`.'''

	class Status:
		PENDING              : Final[str] = "Pending"
		COMPLETED            : Final[str] = "Completed"
		INPUT_ERROR          : Final[str] = "Input Error"
		CONNECTION_ERROR     : Final[str] = "Connection Error"
		INTERRUPTED_BY_USER  : Final[str] = "Interrupted by User"
		INTERRUPTED_BY_ERROR : Final[str] = "Interrupted by Error"

	trash_root : Path | RemotePath | None = None
	log_file   : Path | RemotePath | None = None

	# mypy gives an error for some reason
	#status     : Literal[Status.PENDING, Status.COMPLETED, Status.INPUT_ERROR, Status.CONNECTION_ERROR, Status.INTERRUPTED_BY_USER, Status.INTERRUPTED_BY_ERROR] = Status.PENDING
	status     : str = Status.PENDING
	errors     : list[str] = field(default_factory=list)

	rename_success : int = 0
	delete_success : int = 0
	trash_success  : int = 0
	create_success : int = 0
	update_success : int = 0

	rename_error : int   = 0
	delete_error : int   = 0
	trash_error  : int   = 0
	create_error : int   = 0
	update_error : int   = 0

	byte_diff : int      = 0

	dir_delete_success : int = 0
	dir_create_success : int = 0

	dir_delete_error : int   = 0
	dir_create_error : int   = 0

	@property
	def err_count(self) -> int:
		return self.rename_error + self.delete_error + self.trash_error + self.create_error + self.update_error + self.dir_delete_error + self.dir_create_error

def sync(
		src              : str | os.PathLike[str],
		dst              : str | os.PathLike[str],
		*,

		filter           : str  = "+ **/*/ **/*",
		ignore_hidden    : bool = False,
		ignore_case      : bool = False,
		ignore_symlinks  : bool = False,
		follow_symlinks  : bool = False,

		trash            : str | os.PathLike[str] | None = None,
		delete_files     : bool = False,
		force_update     : bool = False,
		metadata_only    : bool = False,
		rename_threshold : int | None  = 10000,

		dry_run          : bool = False,

		log              : str | os.PathLike[str] | None = None,
		debug            : bool = False,
		quiet            : bool = False,
		veryquiet        : bool = False,
	) -> Results:
	'''
	Copies new and updated files from `src` to `dst`, and optionally "deletes" files from `dst` if they are not present in `src` (they will be moved into `trash`, preserving directory structure). Furthermore, files that exist in `dst` but as a different name in `src` may be renamed in `dst` to match. Candidates for rename are discovered by searching for files with an identical metadata signature, consisting of file size and modification time. These candidates must be above a minimum size threshold (`rename_threshold`) and have an unambiguously unique metadata signature within their respective root directories. The user is asked to confirm these renames before they are committed.

	Args
		src    (str or PathLike) : The path of the root directory to copy files from. Can be a symlink to a directory.
		dst    (str or PathLike) : The path of the root directory to copy files to. Can be a symlink to a directory.

		filter             (str) : The filter string that includes/excludes file system entries from the `src` and `dst` directories. Similar to rsync, the format of the filter string is one of more repetitions of: (+ or -), followed by a list of one of more relative path patterns. Including (+) or excluding (-) of file system entries is determined by the preceding symbol of the first matching pattern. Included files will be copied over as part of the backup, while included directories will be searched. Each pattern ending with "/" will apply to directories only. Otherise the pattern will apply only to files. Note that it is still possible for excluded files in `dst` to be overwritten. (Defaults to "+ **/*/ **/*", which searches all directories and copies all files.)
		ignore_hidden     (bool) : Whether to ignore hidden files by default in glob patterns. If `True`, then wildcards in glob patterns will not match file system entries beginning with a dot. However, globs containing a dot (e.g., "**/.*") will still match these file system entries. (Defaults to `False`.)
		ignore_case       (bool) : Whether to ignore case when comparing files to the filter string. (Defaults to `False`.)
		ignore_symlinks   (bool) : Whether to ignore symbolic links under `src` and `dst`. Note that `src` and `dst` themselves will be followed regardless of this argument. Mutually exclusive with `follow_symlinks`. (Defaults to `False`.)
		follow_symlinks   (bool) : Whether to follow symbolic links under `src` and `dst`. Note that `src` and `dst` themselves will be followed regardless of this argument. Mutually exclusive with `ignore_symlinks`. (Defaults to `False`.)

		trash  (str or PathLike) : The path of the root directory to move "extra" files to. ("Extra" files are those that are in `dst` but not `src`.) Must be on the same file system as `dst`. If set to "auto", then a directory will automatically be made next to `dst`. "Extra" files will not be moved if this argument is `None`. Mutually exclusive with `delete_files`. (Defaults to `None`.)
		delete_files      (bool) : Whether to permanently delete 'extra' files (those that are in `dst_root` but not `src_root`). Mutually exclusive with `trash`. (Defaults to `False`.)
		force_update      (bool) : Whether to replace any newer files in `dst` with older copies in `src`. (Defaults to `False`.)
		metadata_only     (bool) : Whether to use only metadata in determining which files in `dst` are the result of a rename. If `False`, the backup process will also compare the last 1kb of files. (Defaults to `False`.)
		rename_threshold   (int) : The minimum size in bytes needed to consider renaming files in `dst` that were renamed in `src`. Renamed files below this threshold will be simply deleted in `dst` and their replacements created. A value of `None` will mean no files in `dst` will be eligible for renaming. (Defaults to `10000`.)

		dry_run           (bool) : Whether to hold off performing any operation that would make a file system change. Changes that would have occurred will still be printed to console. (Defaults to `False`.)

		log    (str or PathLike) : The path of the log file to use. It will be created if it does not exist. A value of "auto" means a tempfile will be used for the log, and it will be copied to the user's home directory after the backup is done. A value of `None` will skip logging to a file. (Defaults to `None`.)
		debug             (bool) : Whether to log debug messages. (Default to `False`.)
		quiet             (bool) : Whether to forgo printing to stdout.
		veryquiet         (bool) : Whether to forgo printing to stdout and stderr.

	Example Console Output
		   path/to/src
		-> path/to/dst
		--------------
		- empty-dir-in-dst/
		R old-name.txt -> new-name.txt
		- not-in-src-and-getting-deleted.txt
		~ not-in-src-and-getting-sent-to-trash.txt
		+ not-in-dst.txt
		U updated.txt
		+ empty-dir-in-src/

		Summary
		-------
		Status: Completed
		Create Success: 1
		Update Success: 1
		Rename Success: 1
		Delete Success: 1
		»Trash Success: 1
		Net Change: 0 bytes

		Log file: path/to/log/psync_20251015_201523.log

	Returns
		A `Results` object containing various statistics.
	'''
	results = Results()

	logger.setLevel(logging.DEBUG)
	if logger.handlers:
		for handler in list(logger.handlers):
			logger.removeHandler(handler)

	log_file       = None
	handler_stdout = None
	handler_stderr = None
	handler_file   = None

	if veryquiet:
		quiet = True

	if not quiet:
		handler_stdout = logging.StreamHandler(sys.stdout)
		handler_stdout.setFormatter(logging.Formatter("%(message)s"))
		handler_stdout.addFilter(_DebugInfoFilter())
		if debug:
			handler_stdout.setLevel(logging.DEBUG)
		else:
			handler_stdout.setLevel(logging.INFO)
		logger.addHandler(handler_stdout)

	if not veryquiet:
		handler_stderr = logging.StreamHandler(sys.stderr)
		handler_stderr.setFormatter(logging.Formatter("%(message)s"))
		handler_stderr.setLevel(logging.WARNING)
		logger.addHandler(handler_stderr)

	try:
		if not isinstance(src, (str, os.PathLike)):
			raise TypeError(f"Bad type for arg 'src' (expected str or PathLike): {src}")
		if not isinstance(dst, (str, os.PathLike)):
			raise TypeError(f"Bad type for arg 'dst' (expected str or PathLike): {dst}")
		if trash is not None and not isinstance(trash, (str, os.PathLike)):
			raise TypeError(f"Bad type for arg 'trash' (expected str or PathLike): {trash}")
		if not isinstance(delete_files, bool):
			raise TypeError(f"Bad type for arg 'delete_files' (expected bool): {trash}")
		if not isinstance(filter, str):
			raise TypeError(f"Bad type for arg 'filter' (expected str): {filter}")
		if not isinstance(ignore_hidden, bool):
			raise TypeError(f"Bad type for arg 'ignore_hidden' (expected bool): {ignore_hidden}")
		if rename_threshold is not None and not isinstance(rename_threshold, int):
			raise TypeError(f"Bad type for arg 'rename_threshold' (expected int): {rename_threshold}")
		if not isinstance(metadata_only, bool):
			raise TypeError(f"Bad type for arg 'metadata_only' (expected bool): {metadata_only}")
		if not isinstance(dry_run, bool):
			raise TypeError(f"Bad type for arg 'dry_run' (expected bool): {dry_run}")
		if log is not None and not isinstance(log, (str, os.PathLike)):
			raise TypeError(f"Bad type for arg 'log' (expected str or PathLike): {log}")
		if not isinstance(quiet, bool):
			raise TypeError(f"Bad type for arg 'quiet' (expected bool): {quiet}")
		if not isinstance(veryquiet, bool):
			raise TypeError(f"Bad type for arg 'veryquiet' (expected bool): {veryquiet}")

		timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

		if log is None:
			log_file = None
		elif log == "auto":
			log_file = Path.home() / f"psync_{timestamp}.log"
		else:
			log_file = Path(log)
		results.log_file = log_file

		tmp_log_file = None
		if log_file is not None:
			if log_file.exists():
				if not log_file.is_file():
					raise _InputError(f"'log' is not a file: {log_file}")
				tmp_log_file = log_file
			else:
				with tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8", delete=False) as tmp_log:
					tmp_log_file = Path(tmp_log.name)
			#formatter = logging.Formatter("%(levelname)s: %(message)s")
			handler_file = logging.FileHandler(tmp_log_file, encoding="utf-8")
			handler_file.setFormatter(_LogFileFormatter())
			handler_file.addFilter(_NonEmptyFilter())
			#if debug:
			handler_file.setLevel(logging.DEBUG)
			#else:
			#	handler_file.setLevel(logging.INFO)
			logger.addHandler(handler_file)

		width = max(len(str(src)), len(str(dst))) + 3
		logger.info("   " + str(src))
		logger.info("-> " + str(dst))
		logger.info("-" * width)

		src = str(src)
		dst = str(dst)
		trash = str(trash) if trash else None

		src_root   : Path|RemotePath
		dst_root   : Path|RemotePath
		trash_root : Path|RemotePath|None = None

		if "@" in src:
			try:
				src_root = RemotePath.create(str(src))
			except (ValueError, ImportError) as e:
				raise _InputError(str(e)) from e
		else:
			src = os.path.expanduser(src)
			src = os.path.expandvars(src)
			src_root = Path(src)
		if "@" in dst:
			try:
				dst_root = RemotePath.create(str(dst))
			except (ValueError, ImportError) as e:
				raise _InputError(str(e)) from e
		else:
			dst = os.path.expanduser(dst)
			dst = os.path.expandvars(dst)
			dst_root = Path(dst)

		if trash is None:
			trash_root = None
		elif trash == "auto":
			trash_root = dst_root.parent / f"Trash_{timestamp}"
		elif "@" in trash:
			try:
				trash_root = RemotePath.create(str(trash))
			except (ValueError, ImportError) as e:
				raise _InputError(str(e)) from e
		else:
			trash = os.path.expanduser(trash)
			trash = os.path.expandvars(trash)
			trash_root = Path(trash) / timestamp
		results.trash_root = trash_root

		# st_dev is not available over SFTP
		if trash_root is not None and not isinstance(dst_root, RemotePath) and not isinstance(trash_root, RemotePath) and trash_root.exists():
			if os.stat(trash_root).st_dev != os.stat(dst_root).st_dev:
				raise _InputError(f"'trash_root' is not on the same file system as 'dst_root': {trash_root}")

		if src_root.exists() and not src_root.is_dir():
			raise _InputError(f"'src' is not a directory: {src_root}")
		if dst_root.exists() and not dst_root.is_dir():
			raise _InputError(f"'dst' is not a directory: {dst_root}")
		# This isn't a problem because dirs are walked in their entirety before operations are performed
		# If this changes in the furture, should also check that src or dst isn't nested in the other
		#if src_root.resolve() == dst_root.resolve():
		#	raise _InputError(f"'src' and 'dst' point to the same directory")
		if trash_root is not None and trash_root.exists() and not trash_root.is_dir():
			raise _InputError(f"'trash_root' is not a directory: {trash_root}")
		if trash_root and delete_files:
			raise _InputError("Mutually exclusive arguments: 'trash_root' and 'delete_files'")

		if ignore_symlinks and follow_symlinks:
			raise _InputError("Mutually exclusive arguments: 'ignore_symlinks' and 'follow_symlinks'")

		if rename_threshold is not None and rename_threshold < 0:
			raise _InputError(f"'rename_threshold' must be non-negative: {rename_threshold}")

		sftp_compat = isinstance(src_root, RemotePath) or isinstance(dst_root, RemotePath)
		filter = filter.replace("\\", "/") if sftp_compat and os.sep == "\\" else filter

		logger.debug("")
		logger.debug(f"Starting backup: {src_root=} {dst_root=} {filter=} {ignore_hidden=} {ignore_case=} {ignore_symlinks=} {follow_symlinks=} {trash_root=} {trash_root=} {delete_files=} {force_update=} {metadata_only=} {rename_threshold=} {dry_run=} {log_file=} {debug=} {quiet=} {veryquiet=} {sftp_compat=}")
		logger.debug("")

		#width = max(len(str(src_root)), len(str(dst_root))) + 3
		#logger.info("   " + str(src_root))
		#logger.info("-> " + str(dst_root))
		#logger.info("-" * width)

		src_files = _scandir(src_root, filter=filter, ignore_hidden=ignore_hidden, ignore_case=ignore_case, ignore_symlinks=ignore_symlinks, follow_symlinks=follow_symlinks, sftp_compat=sftp_compat)
		dst_files = _scandir(dst_root, filter=filter, ignore_hidden=ignore_hidden, ignore_case=ignore_case, ignore_symlinks=ignore_symlinks, follow_symlinks=follow_symlinks, sftp_compat=sftp_compat)

		for op in _operations(
			src_root         = src_root,
			dst_root         = dst_root,
			src_files        = src_files,
			dst_files        = dst_files,
			trash_root       = trash_root,
			delete_files     = delete_files,
			force_update     = force_update,
			rename_threshold = rename_threshold,
			metadata_only    = metadata_only,
		):
			logger.info(op.summary)

			if not dry_run:
				if op.category == _Operation.Category.RENAME_FILE:
					try:
						_move(op.src, op.dst)
						_delete_empty_dirs(op.src.parent, dst_root)
						results.rename_success += 1
					except OSError as e:
						results.rename_error += 1
						msg = "  " + _error_summary(e)
						logger.error(msg)
						results.errors.append(msg)
				elif op.category == _Operation.Category.DELETE_FILE:
					try:
						op.src.unlink()
						_delete_empty_dirs(op.src.parent, dst_root)
						results.delete_success += 1
						results.byte_diff += op.byte_diff
					except OSError as e:
						results.delete_error += 1
						msg = "  " + _error_summary(e)
						logger.error(msg)
						results.errors.append(msg)
				elif op.category == _Operation.Category.TRASH_FILE:
					try:
						_move(op.src, op.dst)
						_delete_empty_dirs(op.src.parent, dst_root)
						results.delete_success += 1
						results.byte_diff += op.byte_diff
					except OSError as e:
						results.delete_error += 1
						msg = "  " + _error_summary(e)
						logger.error(msg)
						results.errors.append(msg)
				elif op.category == _Operation.Category.CREATE_FILE:
					try:
						_copy(op.src, op.dst, follow_symlinks=follow_symlinks)
						results.create_success += 1
						results.byte_diff += op.byte_diff
					except OSError as e:
						results.create_error += 1
						msg = "  " + _error_summary(e)
						logger.error(msg)
						results.errors.append(msg)
				elif op.category == _Operation.Category.UPDATE_FILE:
					try:
						_copy(op.src, op.dst, follow_symlinks=follow_symlinks)
						results.update_success += 1
						results.byte_diff += op.byte_diff
					except OSError as e:
						results.update_error += 1
						msg = "  " + _error_summary(e)
						logger.error(msg)
						results.errors.append(msg)
				elif op.category == _Operation.Category.CREATE_DIR:
					try:
						op.dst.mkdir(exist_ok=True, parents=True)
						results.dir_create_success += 1
					except OSError as e:
						results.dir_create_error += 1
						msg = "  " + _error_summary(e)
						logger.error(msg)
						results.errors.append(msg)
				elif op.category == _Operation.Category.DELETE_DIR:
					try:
						_delete_empty_dirs(op.src, dst_root)
						results.dir_delete_success += 1
					except OSError as e:
						results.dir_delete_error += 1
						msg = "  " + _error_summary(e)
						logger.error(msg)
						results.errors.append(msg)
				else:
					raise RuntimeError(f"Unrecognized operation: {op}")

		results.status = Results.Status.COMPLETED

	except KeyboardInterrupt:
		results.status = Results.Status.INTERRUPTED_BY_USER
	except _InputError as e:
		logger.info("")
		logger.critical(f"Input Error: {e}", exc_info=debug)
		results.status = Results.Status.INPUT_ERROR
	except ConnectionError as e:
		logger.info("")
		logger.critical(f"Connection Error: {e}", exc_info=debug)
		results.status = Results.Status.CONNECTION_ERROR
	except Exception as e:
		logger.info("")
		logger.critical("An unexpected error occurred.", exc_info=True)
		results.status = Results.Status.INTERRUPTED_BY_ERROR
	finally:
		logger.info("")
		logger.info("Summary")
		logger.info("-------")
		if dry_run:
			logger.info(f"Status: {results.status} (Dry Run)")
		else:
			logger.info(f"Status: {results.status}")
			logger.info(f"Create Success: {results.create_success}" + (f" | Failed: {results.create_error}" if results.create_error else ""))
			logger.info(f"Update Success: {results.update_success}" + (f" | Failed: {results.update_error}" if results.update_error else ""))
			logger.info(f"Rename Success: {results.rename_success}" + (f" | Failed: {results.rename_error}" if results.rename_error else ""))
			logger.info(f"Delete Success: {results.delete_success}" + (f" | Failed: {results.delete_error}" if results.delete_error else ""))
			logger.info(f"»Trash Success: {results.delete_success}" + (f" | Failed: {results.trash_error}"  if results.trash_error  else ""))
			logger.info(f"Net Change: {_human_readable_size(results.byte_diff)}")
			logger.info("")

		if results.err_count:
			logger.info("")
			logger.info(f"There were {results.err_count} errors.")
			if results.err_count <= 10:
				logger.info("Errors are reprinted below for convenience.")
				for error in results.errors:
					logger.info(error)

		if log_file:
			logger.info("")
			logger.info(f"Log file: {log_file}")

		if handler_stdout:
			logger.removeHandler(handler_stdout)

		if handler_stderr:
			logger.removeHandler(handler_stderr)

		if handler_file:
			logger.removeHandler(handler_file)
			handler_file.close()
			assert tmp_log_file is not None
			assert log_file is not None
			tmp_log_file.replace(log_file)

	return results

def _scandir(root:Path|RemotePath, *, filter:str = "+ **/*/ **/*", ignore_hidden:bool = False, ignore_case:bool = False, ignore_symlinks:bool = False, follow_symlinks:bool = False, sftp_compat:bool = False) -> Iterator[_ScandirEntry]:
	'''
	Retrieves file information for all files under `root`, including relative paths, sizes, and mtimes.

    Args
		root (Path|RemotePath) : The directory to search.
		filter           (str) : The filter to include/exclude files and directories. Include file system entries by preceding a space-separated list with "+", and exclude with "-". Included files will be copied, while included directories will be searched. Each pattern ending with a slash will only apply to directories. Otherise the pattern will only apply to files. (Defaults to `+ **/*/ **/*`.)
		ignore_hidden   (bool) : Whether to skip hidden files by default. If `True`, then wildcards in glob patterns will not match file system entries beginning with a dot. However, globs containing a dot (e.g., "**/.*") will still match these file system entries. (Defaults to `False`.)
		ignore_case     (bool) : Whether to ignore case when comparing files to the filter string. (Defaults to `False`.)
		follow_symlinks (bool) : Whether to follow symbolic links under `root`. Note that `root` itself will be followed regardless of this argument. (Defaults to `False`.)
		sftp_compat     (bool) : Whether to work in SFTP compatibility mode, which will truncate milliseconds off the file modification times, treat file names case-sensitively on Windows, and return paths with forward slashes. (Defaults to `False`.)
	'''

	f = _Filter(filter, ignore_hidden=ignore_hidden)

	convert_sep = sftp_compat and os.sep == "\\" and not isinstance(root, RemotePath)
	display_sep = "/" if sftp_compat else os.sep

	for dir, dir_entries, file_entries in _walk(root, ignore_symlinks=ignore_symlinks, follow_symlinks=follow_symlinks):
		logger.debug(f"scanning: {dir}")

		dir_relpath = str(dir.relative_to(root))
		normed_dir_relpath = dir_relpath
		if convert_sep:
			normed_dir_relpath = normed_dir_relpath.replace("\\", "/")
			dir_relpath = dir_relpath.replace("\\", "/")
		if not sftp_compat:
			normed_dir_relpath = os.path.normcase(normed_dir_relpath)

		# empty directory
		if dir_relpath != "." and not file_entries and not dir_entries and f.filter(dir_relpath + display_sep):
			yield _ScandirEntry(
				category = _ScandirEntry.Category.EMPTY_DIR,
				normpath = normed_dir_relpath,
				path     = dir_relpath,
				meta     = _Metadata(size=0, mtime=-1),
			)
			continue

		# prune search tree
		i = 0
		while i < len(dir_entries):
			subdirname = dir_entries[i].name
			subdir_path = dir / subdirname
			subdir_relpath = str(subdir_path.relative_to(root))
			if not f.filter(subdir_relpath + display_sep):
				del dir_entries[i]
				continue
			i += 1

		# prune files
		for entry in file_entries:
			filename = entry.name
			file_path = dir / filename
			file_relpath = str(file_path.relative_to(root))

			normed_file_relpath = file_relpath
			if convert_sep:
				normed_file_relpath = normed_file_relpath.replace("\\", "/")
				file_relpath = file_relpath.replace("\\", "/")
			if not sftp_compat:
				normed_file_relpath = os.path.normcase(normed_file_relpath)

			if (f.filter(file_relpath)):
				stat = entry.stat(follow_symlinks=follow_symlinks)
				mtime = stat.st_mtime
				if sftp_compat:
					mtime = float(int(mtime))

				yield _ScandirEntry(
					category = _ScandirEntry.Category.FILE,
					normpath = normed_file_relpath,
					path     = file_relpath,
					meta     = _Metadata(size=stat.st_size, mtime=mtime),
				)

def _walk(top:Path|RemotePath, *,  ignore_symlinks:bool = False, follow_symlinks:bool = False):
	stack        : list[Any] = [top]
	visited_dirs : set[str]  = set()

	assert not isinstance(top, str)

	while stack:
		top = stack.pop()

		if follow_symlinks:
			if isinstance(top, str):
				visited_dirs.remove(top)
				continue
			d = str(top.resolve())
			if d in visited_dirs:
				logger.warning(f"Symlink circular reference: {top} -> {d}")
				continue
			stack.append(d)
			visited_dirs.add(d)

		assert not isinstance(top, str)

		dirs    = []
		nondirs = []

		try:
			scanner = _RemotePathScanner(top) if isinstance(top, RemotePath) else os.scandir(top)
			with scanner as entries:
				for entry in entries:
					try:
						if ignore_symlinks and entry.is_symlink():
							continue
						if follow_symlinks:
							is_dir = entry.is_dir(follow_symlinks=True) or entry.is_junction()
						else:
							is_dir = entry.is_dir(follow_symlinks=False)
					except OSError as e:
						logger.warning(e)
						continue

					if is_dir:
						dirs.append(entry)
					else:
						nondirs.append(entry)
		except OSError as e:
			logger.warning(e)
			continue

		#dirs.sort(key=lambda x: x.name)
		#nondirs.sort(key=lambda x: x.name)

		yield top, dirs, nondirs

		# Traverse into sub-directories
		for dir in reversed(dirs):
			# in case dir symlink status changed after yield
			new_path = top / dir.name
			if follow_symlinks or not new_path.is_symlink():
				stack.append(new_path)

def _operations(
		*,
		src_root         : Path | RemotePath,
		dst_root         : Path | RemotePath,
		src_files        : Iterator[_ScandirEntry],
		dst_files        : Iterator[_ScandirEntry],
		trash_root       : Path | RemotePath | None,
		delete_files     : bool,
		force_update     : bool,
		metadata_only    : bool,
		rename_threshold : int | None,
	):
	'''Generator of file system operations to perform for this backup.'''

	display_sep = "/" if isinstance(src_root, RemotePath) or isinstance(dst_root, RemotePath) else os.sep

	src_real_names = {}
	dst_real_names = {}

	src_meta = {}
	dst_meta = {}

	src_relpaths = set()
	dst_relpaths = set()

	src_empty_dirs = set()
	dst_empty_dirs = set()

	for src_entry in src_files:
		src_real_names[src_entry.normpath] = src_entry.path
		src_meta[src_entry.normpath] = src_entry.meta
		if src_entry.category == _ScandirEntry.Category.FILE:
			src_relpaths.add(src_entry.normpath)
		elif src_entry.category == _ScandirEntry.Category.EMPTY_DIR:
			src_empty_dirs.add(src_entry.normpath)

	for dst_entry in dst_files:
		dst_real_names[dst_entry.normpath] = dst_entry.path
		dst_meta[dst_entry.normpath] = dst_entry.meta
		if dst_entry.category == _ScandirEntry.Category.FILE:
			dst_relpaths.add(dst_entry.normpath)
		elif dst_entry.category == _ScandirEntry.Category.EMPTY_DIR:
			dst_empty_dirs.add(dst_entry.normpath)

	logger.debug(f"{len(src_relpaths)=}")
	logger.debug(f"{len(dst_relpaths)=}")

	src_only_relpaths = sorted(src_relpaths.difference(dst_relpaths))
	dst_only_relpaths = sorted(dst_relpaths.difference(src_relpaths))
	both_relpaths     = sorted(src_relpaths.intersection(dst_relpaths))

	# Ignore remote files with invalid characters when copying to Windows
	if os.sep == "\\" and isinstance(src_root, RemotePath):
		for path in src_only_relpaths.copy():
			if os.path.isreserved(path) or "\\" in path:
				logger.warning(f"Ignoring incompatible remote file: {path}")
				src_only_relpaths.remove(path)

	logger.debug(f"{len(src_only_relpaths)=}")
	logger.debug(f"{len(dst_only_relpaths)=}")
	logger.debug(f"{len(both_relpaths)=}")

	logger.debug(f"{list(islice(src_only_relpaths, 10))=}")
	logger.debug(f"{list(islice(dst_only_relpaths, 10))=}")
	logger.debug(f"{list(islice(both_relpaths, 10))=}")

	# Delete empty directories now in case any new files needs to take their places
	dst_only_empty_dirs = dst_empty_dirs.difference(src_empty_dirs)
	for relpath in dst_only_empty_dirs:
		dst_relpath_real = dst_real_names[relpath]
		src = dst_root / dst_relpath_real
		assert not any(src.iterdir())
		yield _Operation(
			category = _Operation.Category.DELETE_DIR,
			src = src,
			dst = None,
			byte_diff = 0,
			summary = f"- {dst_relpath_real}{display_sep}"
		)

	# Rename files
	if rename_threshold is not None:
		src_only_relpath_from_meta = _reverse_dict({path:src_meta[path] for path in src_only_relpaths})
		dst_only_relpath_from_meta = _reverse_dict({path:dst_meta[path] for path in dst_only_relpaths})

		for dst_relpath in list(dst_only_relpaths): # dst_only_relpaths is changed inside the loop
			# Ignore small files
			if dst_meta[dst_relpath].size < rename_threshold:
				continue
			try:
				rename_to = src_only_relpath_from_meta[dst_meta[dst_relpath]]
				# Ignore if there are multiple candidates
				if rename_to is None:
					continue

				rename_from = dst_only_relpath_from_meta[dst_meta[dst_relpath]]
				# Ignore if there are multiple candidates
				if rename_from is None:
					continue

				# Ignore if last 1kb do not match
				if not metadata_only:
					on_dst = dst_root / rename_from
					on_src = src_root / rename_to
					if not _last_bytes(on_src) == _last_bytes(on_dst):
						continue

				src_only_relpaths.remove(rename_to)
				dst_only_relpaths.remove(rename_from)

				rename_from = dst_real_names[rename_from]
				rename_to = src_real_names[rename_to]

				src = dst_root / rename_from
				dst = dst_root / rename_to

				yield _Operation(
					category = _Operation.Category.RENAME_FILE,
					src = src,
					dst = dst,
					byte_diff = 0,
					summary = f"R {rename_from} -> {rename_to}"
				)

			except KeyError:
				# dst file not a result of a rename
				continue

	# Delete files
	if delete_files:
		for dst_relpath in dst_only_relpaths:
			dst_relpath_real = dst_real_names[dst_relpath]
			src = dst_root / dst_relpath_real
			byte_diff = -dst_meta[dst_relpath].size
			yield _Operation(
				category = _Operation.Category.DELETE_FILE,
				src = src,
				dst = None,
				byte_diff = byte_diff,
				summary = f"- {dst_relpath_real}"
			)

	# Send files to trash
	elif trash_root is not None:
		for dst_relpath in dst_only_relpaths:
			dst_relpath_real = dst_real_names[dst_relpath]
			src = dst_root / dst_relpath_real
			dst = trash_root     / dst_relpath_real
			#byte_diff = -dst_meta[dst_relpath].size
			byte_diff = 0
			yield _Operation(
				category = _Operation.Category.TRASH_FILE,
				src = src,
				dst = dst,
				byte_diff = byte_diff,
				summary = f"~ {dst_relpath_real}"
			)

	# Create files
	for src_relpath in src_only_relpaths:
		src_relpath_real = src_real_names[src_relpath]
		src = src_root / src_relpath_real
		dst = dst_root / src_relpath_real
		byte_diff = src_meta[src_relpath].size
		yield _Operation(
			category = _Operation.Category.CREATE_FILE,
			src = src,
			dst = dst,
			byte_diff = byte_diff,
			summary = f"+ {src_relpath_real}"
		)

	# Update files that have newer mtimes
	for relpath in both_relpaths:
		src_relpath_real = src_real_names[relpath]
		dst_relpath_real = dst_real_names[relpath]
		src = src_root / src_relpath_real
		dst = dst_root / dst_relpath_real
		byte_diff = src_meta[relpath].size - dst_meta[relpath].size
		src_time = src_meta[relpath].mtime
		dst_time = dst_meta[relpath].mtime
		if src_time > dst_time:
			yield _Operation(
				category = _Operation.Category.UPDATE_FILE,
				src = src,
				dst = dst,
				byte_diff = byte_diff,
				summary = f"U {dst_relpath_real}"
			)
		elif src_time < dst_time:
			if force_update:
				yield _Operation(
					category = _Operation.Category.UPDATE_FILE,
					src = src,
					dst = dst,
					byte_diff = byte_diff,
					summary = f"U {dst_relpath_real}"
				)
			else:
				logger.warning(f"'src' file is older than 'dst' file, skipping update: {relpath}")

	# Create empty directories
	src_only_empty_dirs = src_empty_dirs.difference(dst_empty_dirs)#.difference(dst_files.nonempty_dirs)
	for relpath in src_only_empty_dirs:
		src_relpath_real = src_real_names[relpath]
		dst = dst_root / src_relpath_real
		yield _Operation(
			category = _Operation.Category.CREATE_DIR,
			src = None,
			dst = dst,
			byte_diff = 0,
			summary = f"+ {src_relpath_real}{display_sep}"
		)

def _copy(src:Path|RemotePath, dst:Path|RemotePath, *, exist_ok:bool = True, follow_symlinks:bool = False) -> None:
	'''Copy file from `src` to `dst`, keeping timestamp metadata. Existing files will be overwritten if `exist_ok` is `True`. Otherwise this method will raise a `FileExistsError`.'''

	if dst.exists():
		if not exist_ok:
			raise FileExistsError(f"Cannot copy, dst exists: {src} -> {dst}")
		elif not dst.is_file():
			raise FileExistsError(f"Cannot copy, dst is not a file: {src} -> {dst}")

	dst_tmp = dst.with_name(dst.name + ".tempcopy")
	try:
		# Copy into a temp file, with metadata
		dir = dst.parent
		dir.mkdir(parents=True, exist_ok=True)
		if isinstance(src, Path) and isinstance(dst_tmp, Path):
			shutil.copy2(src, dst_tmp, follow_symlinks=follow_symlinks)
		else:
			RemotePath.copy_file(src, dst_tmp, follow_symlinks=follow_symlinks)

		try:
			# Replace the dst file with the tmp file
			dst_tmp.replace(dst)
		except PermissionError as e:
			# Remove read-only flag and try again
			make_readonly = False
			try:
				dst_stat = dst.stat()
				if dst_stat.st_mode is None or not (dst_stat.st_mode & stat.S_IREAD):
					raise e
				dst.chmod(stat.S_IWRITE)
				make_readonly = True
				dst_tmp.replace(dst)
			finally:
				if make_readonly:
					dst.chmod(stat.S_IREAD)
	finally:
		dst_tmp.unlink(missing_ok=True)

def _move(src:Path|RemotePath, dst:Path|RemotePath, *, exist_ok:bool = False) -> None:
	'''
	Move file from `src` to `dst`. Existing files will be overwritten if `exist_ok` is `True`. Otherwise this method will raise a `FileExistsError`.

	If `delete_empty_dirs_under` is supplied, then any empty directories created during this file move (and under this root directory) will be deleted.
	'''

	if dst.exists():
		if not exist_ok:
			raise FileExistsError(f"Cannot move, dst exists: {src} -> {dst}")
		elif not dst.is_file():
			raise FileExistsError(f"Cannot move, dst is not a file: {src} -> {dst}")

	if isinstance(src, Path) and isinstance(dst, Path):
		pass
	elif isinstance(src, Path):
		raise ValueError("Cannot move 'src' given by a Path to location given by RemotePath.")
	elif isinstance(dst, Path):
		raise ValueError("Cannot move 'src' given by a RemotePath to location given by Path.")
	else:
		pass

	# move the file
	dir = dst.parent
	dir.mkdir(exist_ok=True, parents=True)
	src.replace(dst)

def _delete_empty_dirs(dir:Path|RemotePath, dir_stop:Path|RemotePath) -> None:
	'''Iteratively delete empty directories, starting with `dir` and moving up to (but not including) `dir_stop`.'''

	if not dir.is_dir():
		raise ValueError(f"Expected a dir: {dir}")
	if isinstance(dir, Path) and isinstance(dir_stop, Path):
		if not dir.is_relative_to(dir_stop):
			raise ValueError(f"Chosen root ({dir_stop}) is not an ancestor of dir ({dir})")
	elif isinstance(dir, RemotePath) and isinstance(dir_stop, RemotePath):
		if not dir.is_relative_to(dir_stop):
			raise ValueError(f"Chosen root ({dir_stop}) is not an ancestor of dir ({dir})")
	else:
		raise ValueError(f"'dir' and 'dir_stop' must be the same type.")
	#if any(dir.iterdir()):
	#	raise ValueError(f"Dir is not empty: {dir}")
	try:
		while dir != dir_stop and not any(dir.iterdir()):
			logger.debug(f"deleting empty dir: {dir}")
			dir.rmdir()
			dir = dir.parent
	except OSError as e:
		logger.warning(str(e))

def _last_bytes(file_path:Path, n:int = 1024) -> bytes:
	'''Reads and returns the last `n` bytes of a file.'''

	file_size = file_path.stat().st_size
	bytes_to_read = file_size if n > file_size else n
	with file_path.open("rb") as f:
		f.seek(-bytes_to_read, os.SEEK_END)
		return f.read()

def _reverse_dict(old_dict:dict[Any, Any]) -> dict[Any, Any]:
	'''
	Reverses a `dict` by swapping keys and values. If a value in `old_dict` appears more than once, then the corresponding key in the reversed `dict` will point to a `None`.

	>>> _reverse_dict({"a":1, "b":2, "c":2})[1]
	'a'
	>>> _reverse_dict({"a":1, "b":2, "c":2})[2] is None
	True
	'''

	reversed:dict[Any, Any] = {}
	for key, val in old_dict.items():
		if val in reversed:
			reversed[val] = None
		else:
			reversed[val] = key
	return reversed

def _human_readable_size(n:int) -> str:
	'''
	Translates `n` bytes into a human-readable size.

	>>> _human_readable_size(1023)
	'+1023 bytes'
	>>> _human_readable_size(-1024)
	'-1 KB'
	>>> _human_readable_size(2.1 * 1024 * 1024)
	'+2 MB'
	'''

	if n < 0:
		sign = "-"
	elif n > 0:
		sign = "+"
	else:
		sign = ""

	n = abs(n)
	units = ["bytes", "KB", "MB", "GB", "TB", "PB"]
	i = 0
	while n >= 1024 and i < len(units) - 1:
		n //= 1024
		i += 1
	return f"{sign}{round(n)} {units[i]}"

# TODO remove this?
def _error_summary(e):
	'''Get a one-line summary of an Error.'''

	if isinstance(e, OSError):
		error_type = type(e).__name__
		affected_file = getattr(e, "filename", "N/A")
		msg = f"{error_type}: {affected_file}"
	else:
		error_type = type(e).__name__
		error_message = getattr(e, "strerror", "Unknown error")
		msg = f"{error_type}: {error_message}"
	return msg
