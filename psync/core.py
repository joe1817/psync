# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import sys
import os
import stat
import logging
import tempfile
from enum import Enum
from pathlib import Path
from dataclasses import fields
from typing import Literal, Final, Counter as CounterType
from collections import Counter, namedtuple

from .config import _SyncConfig
from .operations import _get_operations, _replace
from .operations import * # Operations
from .filter import Filter, PathFilter
from .helpers import _UniqueIDGenerator, _human_readable_size
from .sftp import RemotePath
from .watch import _LocalWatcher
from .types import _AbstractPath
from .errors import StateError, UnsupportedOperationError, FilesystemErrorLimitError
from .log import logger, _RecordTag, _DebugInfoFilter, _NonEmptyFilter, _TagFilter, _ConsoleFormatter, _LogFileFormatter, _exc_summary

class Results:
	'''Various statistics and other information returned by `sync()`.'''

	Counts = namedtuple("Counts", ["success", "failure"])

	class Status(Enum):
		UNKNOWN              = -1
		COMPLETED            = 0
		CONNECTION_ERROR     = 1
		INTERRUPTED_BY_USER  = 2
		INTERRUPTED_BY_ERROR = 3
		FS_ERROR_LIMIT       = 4

	def __init__(self, config: _SyncConfig):
		self.config         : _SyncConfig = config
		self.status         : Results.Status = Results.Status.UNKNOWN
		self.error          : BaseException|None = None # any error that prevented or halted sync operation
		self.sync_errors    : list[tuple[Operation, Exception]] = []
		self.success_counts : CounterType[type[Operation]] = Counter()
		self.failure_counts : CounterType[type[Operation]] = Counter()
		self.byte_diff      : int = 0

	def tally_success(self, op:Operation):
		self.success_counts[type(op)] += 1
		self.byte_diff += op.byte_diff

	def tally_failure(self, op:Operation, e:Exception):
		self.failure_counts[type(op)] += 1
		self.sync_errors.append((op, e))

	@property
	def failure_count(self) -> int:
		return sum(self.failure_counts.values())

	@property
	def success_count(self) -> int:
		return sum(self.success_counts.values())

	@property
	def total_count(self) -> int:
		return self.success_count + self.failure_count

	def __getitem__(self, key):
		return Results.Counts(success=self.success_counts[key], failure=self.failure_counts[key])

	def summary(self):
		status = self.status.name.replace("_", " ").title()
		lines = []
		if self.config.dry_run:
			lines.append(f"Status: {status} (Dry Run)")
			lines.append(f"Net Change: {_human_readable_size(self.byte_diff)} (Estimated)")
		else:
			lines.append(f"Status: {status}")
			# keys = self.success_counts.keys()|self.failure_counts.keys()
			keys = {
				"Create": [CreateFileOperation, CreateSymlinkOperation, CreateDirOperation],
				"Update": [UpdateFileOperation],
				"Rename": [RenameFileOperation, RenameDirOperation],
				"Delete": [DeleteFileOperation, DeleteDirOperation],
				" Trash": [TrashFileOperation, TrashDirOperation],
			}
			for key, types in keys.items():
				total_success = sum(self[t].success for t in types)
				total_failure = sum(self[t].failure for t in types)
				lines.append(f"{key} Success: {total_success}" + (f" | Failed: {total_failure}" if total_failure else ""))
			lines.append(f"Net Change: {_human_readable_size(self.byte_diff)}")
		if self.config.log_file:
			lines.append(f"Log File: {self.config.log_file}")
		key_length = max(line.find(":") for line in lines)
		for line in lines:
			yield f"{line:>{len(line) + key_length - line.find(":")}}"

class Sync:
	'''
	`Sync` performs the file sync operation in accordance to several optional arguments, including those related to filtering, matching, deleting, and logging.

	A default sync operation entails copying new and updated files from `src` to `dst`, recursively and maintaining directory structure. Files in `dst` with the same relative paths as those in `src` are assumed to be related. Files are not deleted by default. Files in `dst` may be renamed to match those in `src`. Candidates for rename are discovered by searching for files with an identical metadata signature, consisting of file size and modification time. These candidates must be above a minimum size threshold (`rename_threshold`) and have an unambiguously unique metadata signature within their respective root directories.

	Files can be optionally "recycled" from `dst` if they are not present in `src` (they will be moved into `trash`, preserving directory structure) or they can be deleted.

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
		L symlink -> /symlink/target/path
		+ empty-dir-in-src/
		  -------
		          Status: Completed
		  Create Success: 2
		  Update Success: 1
		  Rename Success: 1
		  Delete Success: 1
		   Trash Success: 1
		      Net Change: 0 bytes
		        Log File: path/to/log/psync_20251015_201523.log
	'''

	_AUTO_LOGFILE   : Final[object] = object()
	_AUTO_TRASH_DIR : Final[object] = object()

	# debug flags
	RAISE_UNKNOWN_ERRORS : Final[int] = 2
	RAISE_FS_ERRORS      : Final[int] = 4

	class _SyncState(Enum):
		INVALID    = 0
		READY      = 1
		RUNNING    = 2
		TERMINATED = 3

	def __init__(self, src:_AbstractPath|str, dst:_AbstractPath|str, **kwargs):
		'''
		Collects and validates arguments for a sync operation.

		Args
			src    (str or PathLike) : The path of the root directory to copy files from. Can be a symlink to a directory.
			dst    (str or PathLike) : The path of the root directory to copy files to. Can be a symlink to a directory.

			filter   (str or Filter) : The filter string that includes/excludes file system entries from the `src` and `dst` directories. Similar to rsync, the format of the filter string is one of more repetitions of: (+ or -), followed by a list of one of more relative path patterns. Including (+) or excluding (-) of file system entries is determined by the preceding symbol of the first matching pattern. Included files will be copied over as part of the backup, while included directories will be searched. Each pattern ending with "/" will apply to directories only. Otherise the pattern will apply only to files. (Defaults to "+ **/*", which searches all directories and copies all files.)
			translate_symlinks (bool) : Whether to copy symbolic links literally, without translation to the dst system. (Defaults to `True`.)
			ignore_symlinks   (bool) : Whether to ignore symbolic links under `src` and `dst`. Note that `src` and `dst` themselves will be followed regardless of this argument. Mutually exclusive with `follow_symlinks`. (Defaults to `False`.)
			follow_symlinks   (bool) : Whether to follow symbolic links under `src` and `dst`. Note that `src` and `dst` themselves will be followed regardless of this argument. Mutually exclusive with `ignore_symlinks`. (Defaults to `False`.)

			create_files      (bool) : Whether to create files in `dst`. (Defaults to `True`.)
			create_dir_tree   (bool) : Whether to recreate the directory tree from `src` in `dst`. (Defaults to `False`.)
			renames           (bool) : Whether to rename files and directories in `dst` to match those in `src'. (Defaults to `True`.)
			delete_files      (bool) : Whether to delete files that are in `dst` but not `src`. If `trash` is set, then files will be moved into it instead of deleted. (Defaults to `False`.)
			delete_empty_dirs (bool) : Whether to delete empty directories that are in `dst` but not `src`. If `trash` is set, then empty directories will be moved into it instead of deleted. (Defaults to `False`.)
			trash  (str or PathLike) : The path of the root directory to move "extra" files to. ("Extra" files are those that are in `dst` but not `src`.) Must be on the same file system as `dst`. If set to "auto", then a directory will automatically be made next to `dst`. "Extra" files will not be moved if this argument is `None`. Mutually exclusive with `delete_entries`. (Defaults to `None`.)

			force_update      (bool) : Whether to force `dst` to match `src`. This will allow replacement of any newer files in `dst` with older copies in `src`. (Defaults to `False`.)
			force_replace     (bool) : Whether to allow files to replace dirs (or vice versa) where their names match. (Defaults to `False`.)
			global_renames    (bool) : Whether to search for renamed files between directories. If `False`, the search will stay within each directory. (Defaults to `False`.)
			content_match     (bool) : Whether to read the last 1kb of files when finding renamed files in `dst`. If `False`, the backup process will rely solely on file metadata. (Defaults to `False`.)
			rename_threshold   (int) : The minimum size in bytes needed to consider renaming files in `dst` that were renamed in `src`. Renamed files below this threshold will be simply deleted in `dst` and their replacements created. (Defaults to `10000`.)
			mirror            (bool) : Equivalent to setting create_dir_tree, delete_files, force_update, and force_replace to `True`. (Defaults to `False`.)

			shutdown_src      (bool) : Shutdown the src system when done. (Defaults to `False`.)
			shutdown_dst      (bool) : Shutdown the dst system when done. (Defaults to `False`.)
			err_limit          (int) : Quit after this many filesystem errors. A value of `-1` means no limit. (Defaults to `-1`.)
			dry_run           (bool) : Whether to hold off performing any operation that would make a file system change. Changes that would have occurred will still be printed to console. (Defaults to `False`.)

			log_file (Path|bool|str) : The path of the log file to use. It will be created if it does not exist. A value of `True` or "auto" means a tempfile will be used for the log, and it will be moved to the user's home directory after the backup is done. A value of `None` will skip logging to a file. (Defaults to `None`.)
			file_level         (int) : Log level for logging to file. (Default to `logging.DEBUG`.)
			print_level        (int) : Log level for printing to console. (Default to `logging.INFO`.)
			debug         (bool|int) : Sets console and file debugging levels to DEBUG. If an integer, the masks Sync.RAISE_UNKNOWN_ERRORS and Sync.RAISE_FS_ERRORS can be used to halt on their respective error types. (Defaults to `False`.)

			title              (str) : A strng to be printed in the header.
			no_header         (bool) : Whether to skip logging header information. (Defaults to `False`.)
			no_footer         (bool) : Whether to skip logging footer information. (Defaults to `False`.)

		Returns
			A `Results` object containing various statistics.
		'''

		self._state = Sync._SyncState.INVALID

		self._src : _AbstractPath
		self._dst : _AbstractPath

		self.src = src
		self.dst = dst

		self._filter             : Filter = PathFilter("+ **/*")
		self._translate_symlinks : bool = True
		self._ignore_symlinks    : bool = False
		self._follow_symlinks    : bool = False

		self._create_files       : bool = True
		self._create_dir_tree    : bool = False
		self._renames            : bool = True
		self._delete_files       : bool = False
		self._delete_empty_dirs  : bool = False
		self._trash              : _AbstractPath|Literal[_AUTO_TRASH_DIR]|None = None

		self._force_update       : bool = False
		self._force_replace      : bool = False
		self._global_renames     : bool = False
		self._content_match      : bool = False
		self._rename_threshold   : int  = 10000
		self._mirror             : bool = False

		self._shutdown_src       : bool = False
		self._shutdown_dst       : bool = False
		self._err_limit          : int  = -1
		self._dry_run            : bool = False

		self._log_file           : Path|Literal[_AUTO_LOGFILE]|None = None # TODO implement RemotePath
		self._tmp_log_file       : Path|None = None
		self._file_level         : int = logging.INFO # log file level
		self._print_level        : int = logging.INFO
		self._debug              : bool|int = False
		self._title              : str|None = None

		# no properties for these
		self._handler_file       : logging.FileHandler|None = None
		self._tags_to_hide       : set[_RecordTag] = set()
		self._show_root_names    : bool = True

		for key in kwargs:
			if not hasattr(self, key):
				raise AttributeError(f"Sync object has no '{key}' attribute.")
			setattr(self, key, kwargs[key])

		self._state = Sync._SyncState.READY

	# -------------------------------------------------------------------------
	# Instance methods

	def setup_trash(self, timestamp: str) -> None:
		if self.trash is Sync._AUTO_TRASH_DIR:
			trash_name = f"Trash.{timestamp}"
			trash_path = self.dst.parent / trash_name
			self.trash = trash_path

	def setup_logging(self, timestamp: str) -> None:
		logger_name = f"psync.{timestamp}"
		assert logger_name not in logging.Logger.manager.loggerDict

		print_level = self.print_level
		file_level = self.file_level

		# self.logger will handle records related to a sync operation
		self.logger = logging.getLogger(logger_name)
		self.logger.propagate = False
		self.logger.setLevel(logging.DEBUG)

		handler_stdout = logging.StreamHandler(sys.stdout)
		handler_stderr = logging.StreamHandler(sys.stderr)
		handler_file : logging.FileHandler|None = None

		handler_stdout.setLevel(print_level)
		handler_stderr.setLevel(max(print_level, logging.WARNING))

		handler_stdout.addFilter(_DebugInfoFilter())
		self.filter_tag = _TagFilter()
		for k in self._tags_to_hide:
			self.filter_tag[k] = True
		handler_stdout.addFilter(self.filter_tag)

		handler_stdout.setFormatter(_ConsoleFormatter())
		handler_stderr.setFormatter(_ConsoleFormatter())

		self.logger.addHandler(handler_stdout)
		self.logger.addHandler(handler_stderr)

		log_file : Path|None = None
		tmp_log_file : Path|None = None
		if self.log_file is Sync._AUTO_LOGFILE:
			log_file = Path.home() / f"{self.logger.name}.log"
			with tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8", delete=False) as tmp_log:
				tmp_log_file = Path(tmp_log.name)
			handler_file = logging.FileHandler(tmp_log_file, encoding="utf-8")
		elif self.log_file:
			handler_file = logging.FileHandler(self.log_file, encoding="utf-8")

		if log_file:
			self.log_file = log_file
		if tmp_log_file:
			self._tmp_log_file = tmp_log_file
		if handler_file:
			handler_file.setLevel(file_level)

			handler_file.setFormatter(_LogFileFormatter())
			handler_file.addFilter(_NonEmptyFilter())

			self._handler_file = handler_file
			self.logger.addHandler(handler_file)

	def run(self) -> Results:
		'''Runs the sync operation. `run()` does not raise errors. If an error occurs, it will be available in the returned `Results` object.'''
		return SyncRunner.run(self)

	def watch(self) -> None:
		if isinstance(self.src, RemotePath):
			raise UnsupportedOperationError("Can only watch local directories.")
		else:
			_LocalWatcher(self).watch()

	def close_file_handler(self) -> None:
		if self._handler_file:
			assert isinstance(self.log_file, Path)
			self.logger.removeHandler(self._handler_file)
			self._handler_file.close()
			if self._tmp_log_file is not None and self._tmp_log_file != self.log_file:
				_replace(self._tmp_log_file, self.log_file)

	# -------------------------------------------------------------------------
	# Collected & validated arguments

	@property
	def src(self) -> _AbstractPath:
		return self._src

	@src.setter
	def src(self, val:_AbstractPath|str) -> None:
		if not isinstance(val, _AbstractPath|str):
			raise TypeError(f"Bad type for property 'src' (expected {_AbstractPath|str}): {val}")

		src: _AbstractPath
		if isinstance(val, _AbstractPath):
			src = val
		else:
			assert isinstance(val, str)
			if "@" in val:
				src = RemotePath.create(val)
			else:
				val = os.path.expanduser(val)
				val = os.path.expandvars(val)
				src = Path(val)

		assert isinstance(src, _AbstractPath)
		if src.exists() and not src.is_dir():
			raise ValueError(f"'src' is not a directory: {val}")
		if not src.exists():
			raise ValueError(f"'src' does not exist: {val}")

		src = src.resolve()

		if hasattr(self, "_dst") and self.dst and type(src) == type(self.dst):
			err = None
			if src == self.dst:
				err = "'src' and 'dst' connot be the same directory"
			else:
				try:
					if src.is_relative_to(self.dst):
						err = "'src' cannot be a child of 'dst'"
				except Exception:
					pass
				try:
					if self.dst.is_relative_to(src):
						err = "'dst' cannot be a child of 'src'"
				except Exception:
					pass
				if err:
					raise ValueError(err)

		self._src = src

	@property
	def dst(self) -> _AbstractPath:
		return self._dst

	@dst.setter
	def dst(self, val:_AbstractPath|str) -> None:
		if not isinstance(val, _AbstractPath|str):
			raise TypeError(f"Bad type for property 'dst' (expected {_AbstractPath|str}): {val}")

		dst: _AbstractPath
		if isinstance(val, _AbstractPath):
			dst = val
		else:
			assert isinstance(val, str)
			if "@" in val:
				dst = RemotePath.create(val)
			else:
				val = os.path.expanduser(val)
				val = os.path.expandvars(val)
				dst = Path(val)

		assert isinstance(dst, _AbstractPath)
		if dst.exists() and not dst.is_dir():
			raise ValueError(f"'dst' is not a directory: {val}")

		if hasattr(self, "_trash") and self.trash:
			if isinstance(dst, RemotePath) and not isinstance(self.trash, RemotePath):
				raise ValueError(f"'trash' is not on the same file system as 'dst': {self.trash}")

			if not isinstance(dst, RemotePath) and isinstance(self.trash, RemotePath):
				raise ValueError(f"'trash' is not on the same file system as 'dst': {self.trash}")

			# st_dev is not available over SFTP
			# TODO check also when dst doesn't exist by making a temp dir
			if isinstance(dst, Path) and dst.exists():
				assert isinstance(self.trash, Path)
				if os.stat(self.trash).st_dev != os.stat(dst).st_dev:
					raise ValueError(f"'trash' is not on the same file system as 'dst': {self.trash}")

		assert isinstance(dst, _AbstractPath)
		dst = dst.resolve()

		if hasattr(self, "_src") and self.src and type(self.src) == type(dst):
			err = None
			if self.src == dst:
				err = "'self.src' and 'dst' connot be the same directory"
			else:
				try:
					if self.src.is_relative_to(dst):
						err = "'self.src' cannot be a child of 'dst'"
				except Exception:
					pass
				try:
					if dst.is_relative_to(self.src):
						err = "'dst' cannot be a child of 'self.src'"
				except Exception:
					pass
				if err:
					raise ValueError(err)

		self._dst = dst

	@property
	def filter(self) -> Filter:
		return self._filter

	@filter.setter
	def filter(self, val:str|Filter) -> None:
		if not isinstance(val, str|Filter):
			raise TypeError(f"Bad type for property 'filter' (expected str|Filter): {val}")

		filter: Filter
		if isinstance(val, str):
			# TODO? convert sep?
			filter = PathFilter(val)
		else:
			filter = val

		assert isinstance(filter, Filter)
		self._filter = filter

	@property
	def create_files(self) -> bool:
		return self._create_files

	@create_files.setter
	def create_files(self, val:bool) -> None:
		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'create_files' (expected bool): {val}")
		self._create_files = val

	@property
	def create_dir_tree(self) -> bool:
		return self._mirror or self._create_dir_tree

	@create_dir_tree.setter
	def create_dir_tree(self, val:bool) -> None:
		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'create_dir_tree' (expected bool): {val}")
		self._create_dir_tree = val

	@property
	def renames(self) -> bool:
		return self._renames

	@renames.setter
	def renames(self, val:bool) -> None:
		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'renames' (expected bool): {val}")
		self._renames = val

	@property
	def delete_files(self) -> bool:
		return self._delete_files or self._mirror or bool(self._trash and not self._delete_empty_dirs)

	@delete_files.setter
	def delete_files(self, val:bool) -> None:
		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'delete_files' (expected bool): {val}")
		self._delete_files = val

	@property
	def delete_empty_dirs(self) -> bool:
		return self._delete_empty_dirs

	@delete_empty_dirs.setter
	def delete_empty_dirs(self, val:bool) -> None:
		logger.warning("'delete_empty_dirs' is not yet implemented.")
		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'delete_empty_dirs' (expected bool): {val}")
		self._delete_empty_dirs = val

	@property
	def trash(self) -> _AbstractPath|Literal[_AUTO_TRASH_DIR]|None:
		return self._trash

	@trash.setter
	def trash(self, val:_AbstractPath|str|bool|None) -> None:
		if not isinstance(val, _AbstractPath|str|bool|None):
			raise TypeError(f"Bad type for property 'trash' (expected {_AbstractPath|str|bool|None}): {val}")

		if val is None or val is False:
			self._trash = None
			return

		if val == "auto" or val is True:
			self._trash = Sync._AUTO_TRASH_DIR # will be finalized in setup_trash
			return

		trash: _AbstractPath
		if isinstance(val, _AbstractPath):
			trash = val
		else:
			assert isinstance(val, str)
			if "@" in val:
				#try:
				trash = RemotePath.create(val)
				#except (ValueError, ImportError) as e:
				#	raise ValueError(str(e)) from e
			else:
				val = os.path.expanduser(val)
				val = os.path.expandvars(val)
				trash = Path(val)

		assert isinstance(trash, _AbstractPath)
		if trash.exists() and not trash.is_dir():
			raise ValueError(f"'trash' is not a directory: {val}")

		# st_dev is not available over SFTP
		try:
			if isinstance(self.dst, Path) and self.dst.exists():
				assert isinstance(trash, Path)
				if os.stat(trash).st_dev != os.stat(self.dst).st_dev:
					raise ValueError(f"'trash' is not on the same file system as 'dst': {self.trash}")
		except FileNotFoundError:
			pass

		self._trash = trash

	@property
	def force_update(self) -> bool:
		return self._mirror or self._force_update

	@force_update.setter
	def force_update(self, val:bool) -> None:
		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'force_update' (expected bool): {val}")
		self._force_update = val

	@property
	def force_replace(self) -> bool:
		return self._mirror or self._force_replace

	@force_replace.setter
	def force_replace(self, val:bool) -> None:
		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'force_replace' (expected bool): {val}")
		self._force_replace = val

	@property
	def global_renames(self) -> bool:
		return self._global_renames

	@global_renames.setter
	def global_renames(self, val:bool) -> None:
		if not isinstance(val, bool):
			raise TypeError(f"Bad type for arg 'global_renames' (expected bool): {val}")
		self._global_renames = val

	@property
	def content_match(self) -> bool:
		return self._content_match

	@content_match.setter
	def content_match(self, val:bool) -> None:
		if not isinstance(val, bool):
			raise TypeError(f"Bad type for arg 'content_match' (expected bool): {val}")
		self._content_match = val

	@property
	def rename_threshold(self) -> int:
		return self._rename_threshold

	@rename_threshold.setter
	def rename_threshold(self, val:int) -> None:
		if not isinstance(val, int):
			raise TypeError(f"Bad type for arg 'rename_threshold' (expected int): {val}")
		if val < 0:
			raise ValueError(f"'rename_threshold' must be non-negative.")
		self._rename_threshold = val

	@property
	def translate_symlinks(self) -> bool:
		return self._translate_symlinks

	@translate_symlinks.setter
	def translate_symlinks(self, val:bool) -> None:
		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'translate_symlinks' (expected bool): {val}")
		self._translate_symlinks = val

	@property
	def ignore_symlinks(self) -> bool:
		return self._ignore_symlinks

	@ignore_symlinks.setter
	def ignore_symlinks(self, val:bool) -> None:
		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'ignore_symlinks' (expected bool): {val}")
		if val and self.follow_symlinks:
			raise StateError("Mutually exclusive properties: 'follow_symlinks' and 'ignore_symlinks'")
		self._ignore_symlinks = val

	@property
	def follow_symlinks(self) -> bool:
		return self._follow_symlinks

	@follow_symlinks.setter
	def follow_symlinks(self, val:bool) -> None:
		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'follow_symlinks' (expected bool): {val}")
		if val and self.ignore_symlinks:
			raise StateError("Mutually exclusive properties: 'ignore_symlinks' and 'follow_symlinks'")
		self._follow_symlinks = val

	@property
	def shutdown_src(self) -> bool:
		return self._shutdown_src

	@shutdown_src.setter
	def shutdown_src(self, val:bool) -> None:
		if not isinstance(val, bool):
			raise TypeError(f"Bad type for arg 'shutdown_src' (expected bool): {val}")
		self._shutdown_src = val

	@property
	def shutdown_dst(self) -> bool:
		return self._shutdown_dst

	@shutdown_dst.setter
	def shutdown_dst(self, val:bool) -> None:
		if not isinstance(val, bool):
			raise TypeError(f"Bad type for arg 'shutdown_dst' (expected bool): {val}")
		self._shutdown_dst = val

	@property
	def err_limit(self) -> int:
		return self._err_limit

	@err_limit.setter
	def err_limit(self, val:int) -> None:
		if not isinstance(val, int):
			raise TypeError(f"Bad type for arg 'err_limit' (expected int): {val}")
		self._err_limit = val

	@property
	def dry_run(self) -> bool:
		return self._dry_run

	@dry_run.setter
	def dry_run(self, val:bool) -> None:
		if not isinstance(val, bool):
			raise TypeError(f"Bad type for arg 'dry_run' (expected bool): {val}")
		self._dry_run = val

	@property
	def log_file(self) -> Path|Literal[_AUTO_LOGFILE]|None:
		return self._log_file

	@log_file.setter
	def log_file(self, val:Path|str|bool|None) -> None:
		if isinstance(val, RemotePath):
			raise NotImplementedError() # TODO

		if not isinstance(val, Path|str|bool|None):
			raise TypeError(f"Bad type for property 'log_file' (expected {Path|str|bool|None}): {val}")

		if val is None or val is False:
			self._log_file = None
			return
		if val == "auto" or val is True:
			self._log_file = Sync._AUTO_LOGFILE # will be finalized in setup_logging
			return

		log_file : Path
		if isinstance(val, str):
			log_file = Path(val)
		else:
			assert isinstance(val, Path)
			log_file = val

		if log_file.exists() and not log_file.is_file():
			raise ValueError(f"'log' is not a file: {log_file}")

		self._log_file = log_file

	@property
	def debug(self) -> bool|int:
		return self._debug

	@debug.setter
	def debug(self, val:bool|int) -> None:
		if not isinstance(val, bool|int):
			raise TypeError(f"Bad type for arg 'debug' (expected bool|int): {val}")
		self._debug = val

	@property
	def title(self) -> str|None:
		return self._title

	@title.setter
	def title(self, val:str) -> None:
		if not isinstance(val, str|None):
			raise TypeError(f"Bad type for arg 'title' (expected str|None): {val}")
		self._title = val

	# -------------------------------------------------------------------------
	# Derived properties

	@property
	def src_sys(self):
		return RemotePath.os_name(self.src.hostname) if isinstance(self.src, RemotePath) else os.name

	@property
	def dst_sys(self):
		return RemotePath.os_name(self.dst.hostname) if isinstance(self.dst, RemotePath) else os.name

	@property
	def src_sep(self):
		return "/" if isinstance(self.src, RemotePath) else os.sep

	@property
	def dst_sep(self):
		return "/" if isinstance(self.dst, RemotePath) else os.sep

	@property
	def src_name(self):
		return (self.src.name + self.src_sep) if self._show_root_names else ""

	@property
	def dst_name(self):
		return (self.dst.name + self.dst_sep) if self._show_root_names else ""

	@property
	def trash_name(self):
		return (self.trash.name + self.dst_sep) if self.trash and self._show_root_names else ""

	@property
	def sftp_compat(self):
		return isinstance(self.src, RemotePath) or isinstance(self.dst, RemotePath)

	# -------------------------------------------------------------------------
	# Not passed to _SyncConfig

	@property
	def mirror(self) -> bool:
		return self._mirror

	@mirror.setter
	def mirror(self, val:bool) -> None:
		if not isinstance(val, int):
			raise TypeError(f"Bad type for property 'mirror' (expected bool): {val}")
		self._mirror = val

	@property
	def file_level(self) -> int:
		return logging.DEBUG if self.debug else self._file_level

	@file_level.setter
	def file_level(self, val:int) -> None:
		if not isinstance(val, int):
			raise TypeError(f"Bad type for property 'file_level' (expected int): {val}")
		self._file_level = val

	@property
	def print_level(self) -> int:
		return logging.DEBUG if self.debug else self._print_level

	@print_level.setter
	def print_level(self, val:int) -> None:
		if not isinstance(val, int):
			raise TypeError(f"Bad type for property 'print_level' (expected int): {val}")
		self._print_level = val

	@property
	def no_header(self):
		return _RecordTag.HEADER in self._tags_to_hide

	@no_header.setter
	def no_header(self, val:bool) -> None:
		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'no_header' (expected bool): {val}")
		if val:
			self._tags_to_hide.add(_RecordTag.HEADER)
		else:
			try:
				self._tags_to_hide.remove(_RecordTag.HEADER)
			except KeyError:
				pass

	@property
	def no_footer(self):
		return _RecordTag.FOOTER in self._tags_to_hide

	@no_footer.setter
	def no_footer(self, val:bool) -> None:
		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'no_footer' (expected bool): {val}")
		if val:
			self._tags_to_hide.add(_RecordTag.FOOTER)
		else:
			try:
				self._tags_to_hide.remove(_RecordTag.FOOTER)
			except KeyError:
				pass

class SyncRunner:

	@classmethod
	def shutdown_local(cls) -> None:
		platform = sys.platform
		if platform.startswith("win"):
			command = "shutdown /s /f /t 0"
		elif platform.startswith("linux") or platform == "darwin":
			# Mac will require sudo
			command = "shutdown -h now"
		else:
			raise UnsupportedOperationError("Shutdown command not available on this system.")
		try:
			os.system(command)
		except Exception as e:
			logger.error(f"Error executing shutdown command: {e}")

	@classmethod
	def get_config(cls, sync: Sync) -> _SyncConfig:
		names = {f.name for f in fields(_SyncConfig)}
		options = {name: getattr(sync, name) for name in names}
		config = _SyncConfig(**options)
		return config

	@classmethod
	def run(cls, sync: Sync) -> Results:
		if sync._state != Sync._SyncState.READY:
			raise StateError("Sync object state is not READY.")

		timestamp = _UniqueIDGenerator.get_timestamp()
		sync.setup_trash(timestamp)
		sync.setup_logging(timestamp)
		config  = cls.get_config(sync)
		results = Results(config)

		try:
			HEADER  = _RecordTag.HEADER.dict()
			FOOTER  = _RecordTag.FOOTER.dict()
			SYNC_OP = _RecordTag.SYNC_OP.dict()

			config.logger.debug(repr(config))
			config.logger.debug("")

			width = max(len(str(config.src)), len(str(config.dst)), 7) + 3
			if config.title:
				config.logger.info(config.title, extra=HEADER)
			config.logger.info("   " + str(config.src), extra=HEADER)
			config.logger.info("-> " + str(config.dst), extra=HEADER)
			config.logger.info("-" * width, extra=HEADER)

			for op in _get_operations(config):
				if any(op.depends_on(failed_op) for failed_op, _ in results.sync_errors):
					config.logger.debug(f"Chain failure: {op.summary}")
					continue

				config.logger.info(op.summary, extra=SYNC_OP)

				if not config.dry_run:
					try:
						op.perform()
						results.tally_success(op)
					except OSError as e:
						results.tally_failure(op, e)
						if config.debug & Sync.RAISE_FS_ERRORS:
							raise e
						elif config.err_limit > 0 and results.failure_count >= config.err_limit:
							raise FilesystemErrorLimitError()
						else:
							config.logger.error(_exc_summary(e))

			results.status = Results.Status.COMPLETED
		except KeyboardInterrupt as e:
			results.status = Results.Status.INTERRUPTED_BY_USER
			results.error = e
			raise e
		except ConnectionError as e:
			config.logger.info("")
			config.logger.critical(f"Connection Error: {e}", exc_info=False)
			results.status = Results.Status.CONNECTION_ERROR
			results.error = e
		except FilesystemErrorLimitError as e:
			config.logger.info("")
			config.logger.critical(f"Filesystem error limit reached.", exc_info=False)
			results.status = Results.Status.FS_ERROR_LIMIT
			results.error = e
		except Exception as e:
			config.logger.info("")
			config.logger.critical("An unexpected error occurred.", exc_info=True)
			results.status = Results.Status.INTERRUPTED_BY_ERROR
			results.error = e
			if config.debug & Sync.RAISE_UNKNOWN_ERRORS:
				raise e
		finally:
			config.logger.info("-" * width, extra=FOOTER)
			for line in results.summary():
				config.logger.info(line, extra=FOOTER)

			if results.failure_count:
				config.logger.info("", extra=FOOTER)
				config.logger.info(f"There were {results.failure_count} errors.", extra=FOOTER)
				if results.failure_count <= 10 and results.total_count >= 50:
					config.logger.info("Errors are reprinted below for convenience.", extra=FOOTER)
					for op, exc in results.sync_errors:
						config.logger.info(_exc_summary(exc), extra=FOOTER)

			config.logger.info("", extra=FOOTER)

			if sync.log_file:
				sync.close_file_handler()

			# shutdown all remote hosts before local
			# send shutdown signal at most once per host
			if results.status == Results.Status.COMPLETED:
				do_shutdown_local = False
				shutdown_remote: set[str] = set()
				if config.shutdown_src:
					if isinstance(config.src, RemotePath):
						shutdown_remote.add(config.src.hostname)
					else:
						do_shutdown_local = True
				if config.shutdown_dst:
					if isinstance(config.dst, RemotePath):
						shutdown_remote.add(config.dst.hostname)
					else:
						do_shutdown_local = True
				for hostname in shutdown_remote:
					RemotePath.shutdown(hostname)
				if do_shutdown_local:
					SyncRunner.shutdown_local()

		sync._state = Sync._SyncState.READY
		return results
