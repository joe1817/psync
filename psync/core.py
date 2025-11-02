# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import sys
import os
import stat
import shutil
import logging
import tempfile
from enum import Enum
from pathlib import Path
from itertools import islice
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Literal, Iterator, Counter as CounterType, cast
from collections import Counter

from .filter import Filter, PathFilter
from .helpers import _reverse_dict, _human_readable_size
from .sftp import RemotePath, _RemotePathScanner
from .watch import _LocalWatcher
from .types import PathType, PathLikeType
from .errors import MetadataUpdateError, DirDeleteError, StateError, ImmutableObjectError
from .log import logger, _RecordTag, _DebugInfoFilter, _NonEmptyFilter, _TagFilter, _ConsoleFormatter, _LogFileFormatter, _exc_summary

@dataclass(frozen=True)
class _Metadata:
	'''File metadata that will be used to find probable duplicates.'''

	size  : int
	mtime : float

@dataclass(frozen=True)
class _Entry:
	'''Filesystem entries yielded by `_scandir()`.'''

	path         : PathType
	relpath      : str
	norm_relpath : str # normcased and replaced \\ -> /

@dataclass(frozen=True)
class _File(_Entry):
	meta     : _Metadata

@dataclass(frozen=True)
class _Dir(_Entry):
	num_files    : int
	num_dirs     : int

@dataclass(frozen=True)
class Operation:
	'''Filesystem operation yielded by `_operations()`.'''

	name  = ""

	summary   : str
	dst       : PathType
	src       : PathType | None = None
	target    : PathType | None = None
	byte_diff : int = 0

	def perform(self, sync:"Sync"):
		raise NotImplementedError()

	def depends_on(self, op:"Operation"):
		'''Returns `True` if this Operation would fail if the Operation `op` were to fail beforehand or not to occur.'''

		in_path  = None
		out_path = None

		if isinstance(self, CreateFileOperation):
			in_path = self.dst
		elif isinstance(self, RenameFileOperation):
			in_path = self.target
		#elif isinstance(self, TrashFileOperation):
		#	in_path = self.target
		elif isinstance(self, CreateDirOperation):
			in_path = self.dst

		if isinstance(op, RenameFileOperation):
			out_path = op.dst
		elif isinstance(op, DeleteFileOperation):
			out_path = op.dst
		elif isinstance(op, TrashFileOperation):
			out_path = op.dst
		elif isinstance(op, CreateDirOperation):
			out_path = op.dst
		elif isinstance(op, DeleteDirOperation):
			out_path = op.dst

		if in_path is None or out_path is None:
			return False
		else:
			if isinstance(in_path, Path):
				assert isinstance(out_path, Path)
				return in_path.is_relative_to(out_path)
			else:
				assert isinstance(in_path, Path)
				assert isinstance(out_path, Path)
				return in_path.is_relative_to(out_path)

	def __str__(self):
		return self.summary

@dataclass(frozen=True)
class CreateFileOperation(Operation):
	name = "Create"

	def __post_init__(self):
		assert self.src is not None
		assert self.target is None

	def perform(self, sync:"Sync"):
		assert self.src is not None
		_copy(self.src, self.dst, follow_symlinks=sync.follow_symlinks)

@dataclass(frozen=True)
class UpdateFileOperation(Operation):
	name = "Update"

	def __post_init__(self):
		assert self.src is not None
		assert self.target is None

	def perform(self, sync:"Sync"):
		assert self.src is not None
		_copy(self.src, self.dst, follow_symlinks=sync.follow_symlinks)

@dataclass(frozen=True)
class RenameFileOperation(Operation):
	name = "Rename"

	def __post_init__(self):
		assert self.src is None
		assert self.target is not None

	def perform(self, sync:"Sync"):
		assert self.target is not None
		_move(self.dst, self.target)

@dataclass(frozen=True)
class DeleteFileOperation(Operation):
	name = "Delete"

	def __post_init__(self):
		assert self.src is None
		assert self.target is None

	def perform(self, sync:"Sync"):
		self.dst.unlink()

@dataclass(frozen=True)
class TrashFileOperation(Operation):
	name = "Trash"

	def __post_init__(self):
		assert self.src is None
		assert self.target is not None

	def perform(self, sync:"Sync"):
		assert self.target is not None
		_move(self.dst, self.target)

@dataclass(frozen=True)
class CreateDirOperation(Operation):
	name = "Create Dir"

	def __post_init__(self):
		assert self.src is None
		assert self.target is None

	def perform(self, sync:"Sync"):
		self.dst.mkdir(exist_ok=True, parents=True)

@dataclass(frozen=True)
class DeleteDirOperation(Operation):
	name = "Delete Dir"

	def __post_init__(self):
		assert self.src is None
		assert self.target is None

	def perform(self, sync:"Sync"):
		try:
			self.dst.rmdir()
		except OSError as e:
			raise DirDeleteError(_exc_summary(e)) from e

class Results:
	'''Various statistics and other information returned by `sync()`.'''

	class Status(Enum):
		UNKNOWN              = -1
		COMPLETED            = 0
		CONNECTION_ERROR     = 1
		INTERRUPTED_BY_USER  = 2
		INTERRUPTED_BY_ERROR = 3

	def __init__(self, sync:"Sync"):
		self.sync           : "Sync" = sync # needed to reference trash, log, and dry_run
		self.status         : Results.Status = Results.Status.UNKNOWN
		self.error          : BaseException|None = None # any error that prevented or halted sync operation
		self.sync_errors    : list[tuple[Operation, Exception]] = []
		self.success_counts : CounterType[str] = Counter()
		self.failure_counts : CounterType[str] = Counter()
		self.byte_diff      : int = 0

	def tally_success(self, op:Operation):
		self.success_counts[op.name] += 1
		self.byte_diff += op.byte_diff

	def tally_failure(self, op:Operation, e:Exception):
		self.failure_counts[op.name] += 1
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
		if isinstance(key, type) and issubclass(key, Operation):
			return (self.success_counts[key.name], self.failure_counts[key.name])
		else:
			return self.counts[key]

	def summary(self):
		status = self.status.name.replace("_", " ").title()
		lines = []
		if self.sync.dry_run:
			lines.append(f"Status: {status} (Dry Run)")
		else:
			lines.append(f"Status: {status}")
			# keys = self.success_counts.keys()|self.failure_counts.keys()
			keys = ["Create", "Update", "Rename", "Delete", "Trash"]
			for key in keys:
				lines.append(f"{key} Success: {self.success_counts[key]}" + (f" | Failed: {self.failure_counts[key]}" if self.failure_counts[key] else ""))
			lines.append(f"Net Change: {_human_readable_size(self.byte_diff)}")
		if self.sync.log_file:
			lines.append(f"Log File: {self.sync.log_file}")
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
		+ empty-dir-in-src/
		  -------
		          Status: Completed
		  Create Success: 1
		  Update Success: 1
		  Rename Success: 1
		  Delete Success: 1
		   Trash Success: 1
		      Net Change: 0 bytes
		        Log File: path/to/log/psync_20251015_201523.log
	'''

	class _SyncState(Enum):
		INVALID    = 0
		READY      = 1
		RUNNING    = 2
		TERMINATED = 3

	valid_kwargs = [
		"filter",
		"trash",
		"delete_files",
		"no_create",
		"force_update",
		"metadata_only",
		"rename_threshold",
		"ignore_symlinks",
		"follow_symlinks",
		"dry_run",
		"log_file",
		"log_level",
		"print_level",
		"no_header",
		"no_footer",
	]

	def __init__(self, src:PathLikeType, dst:PathLikeType, **kwargs):
		'''
		Initialize a Sync object.

		Args
			src    (str or PathLike) : The path of the root directory to copy files from. Can be a symlink to a directory.
			dst    (str or PathLike) : The path of the root directory to copy files to. Can be a symlink to a directory.

			filter   (str or Filter) : The filter string that includes/excludes file system entries from the `src` and `dst` directories. Similar to rsync, the format of the filter string is one of more repetitions of: (+ or -), followed by a list of one of more relative path patterns. Including (+) or excluding (-) of file system entries is determined by the preceding symbol of the first matching pattern. Included files will be copied over as part of the backup, while included directories will be searched. Each pattern ending with "/" will apply to directories only. Otherise the pattern will apply only to files. (Defaults to "+ **/*", which searches all directories and copies all files.)
			ignore_symlinks   (bool) : Whether to ignore symbolic links under `src` and `dst`. Note that `src` and `dst` themselves will be followed regardless of this argument. Mutually exclusive with `follow_symlinks`. (Defaults to `False`.)
			follow_symlinks   (bool) : Whether to follow symbolic links under `src` and `dst`. Note that `src` and `dst` themselves will be followed regardless of this argument. Mutually exclusive with `ignore_symlinks`. (Defaults to `False`.)

			trash  (str or PathLike) : The path of the root directory to move "extra" files to. ("Extra" files are those that are in `dst` but not `src`.) Must be on the same file system as `dst`. If set to "auto", then a directory will automatically be made next to `dst`. "Extra" files will not be moved if this argument is `None`. Mutually exclusive with `delete_files`. (Defaults to `None`.)
			delete_files      (bool) : Whether to permanently delete 'extra' files (those that are in `dst` but not `src`). Mutually exclusive with `trash`. (Defaults to `False`.)
			no_create         (bool) : Whether to prevent the creation of any files or directories in `dst`. (Defaults to `False`.)
			force_update      (bool) : Whether to allow replacement of any newer files in `dst` with older copies in `src`. (Defaults to `False`.)
			metadata_only     (bool) : Whether to use only metadata in determining which files in `dst` are the result of a rename. If `False`, the backup process will also compare the last 1kb of files. (Defaults to `False`.)
			rename_threshold   (int) : The minimum size in bytes needed to consider renaming files in `dst` that were renamed in `src`. Renamed files below this threshold will be simply deleted in `dst` and their replacements created. A value of `None` will mean no files in `dst` will be eligible for renaming. (Defaults to `10000`.)

			dry_run           (bool) : Whether to hold off performing any operation that would make a file system change. Changes that would have occurred will still be printed to console. (Defaults to `False`.)

			log    (str or PathLike) : The path of the log file to use. It will be created if it does not exist. A value of "auto" means a tempfile will be used for the log, and it will be copied to the user's home directory after the backup is done. A value of `None` will skip logging to a file. (Defaults to `None`.)
			log_level          (int) : Log level for logging to file. (Default to `logging.DEBUG`.)
			print_level        (int) : Log level for printing to console. (Default to `logging.INFO`.)
			no_header         (bool) : Whether to skip logging header information. (Defaults to `False`.)
			no_footer         (bool) : Whether to skip logging footer information. (Defaults to `False`.)

		Returns
			A `Results` object containing various statistics.
		'''
		self._state = Sync._SyncState.INVALID

		self._src : PathType
		self._dst : PathType

		self.src = src
		self.dst = dst

		self._filter           : Filter = PathFilter("+ **/*")
		self._trash            : PathType|None = None
		self._delete_files     : bool = False
		self._no_create        : bool = False
		self._force_update     : bool = False
		self._metadata_only    : bool = False
		self._rename_threshold : int|None = 10000
		self._ignore_symlinks  : bool = False
		self._follow_symlinks  : bool = False
		self._dry_run          : bool = False
		self._log_file         : PathType|None = None
		self._log_level        : int = logging.DEBUG

		self._tmp_log_file     : PathType|None = None

		# This isn't a problem because dirs are walked in their entirety before operations are performed
		# If this changes in the furture, should also check that src or dst isn't nested in the other
		#if src.resolve() == dst.resolve():
		#	raise ValueError(f"'src' and 'dst' point to the same directory")

		self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
		self.sftp_compat = isinstance(self.src, RemotePath) or isinstance(self.dst, RemotePath)
		self.results = Results(self)

		# self.logger will handle records related to a sync operation
		logger_name = f"psync.{self.timestamp}"
		if logger_name in logging.Logger.manager.loggerDict:
			logger_name += f"_{id(self)}"

		self.logger = logging.getLogger(logger_name)
		self.logger.propagate = False
		self.logger.setLevel(logging.DEBUG)
		self.handler_stdout = logging.StreamHandler(sys.stdout)
		self.handler_stderr = logging.StreamHandler(sys.stderr)
		self.handler_file : logging.FileHandler|None = None
		self.handler_stdout.addFilter(_DebugInfoFilter())
		self.handler_stdout.setLevel(logging.INFO)
		self.handler_stderr.setLevel(logging.WARNING)
		self.handler_stdout.setFormatter(_ConsoleFormatter())
		self.handler_stderr.setFormatter(_ConsoleFormatter())
		self.filter_tag = _TagFilter()
		self.handler_stdout.addFilter(self.filter_tag)
		self.logger.addHandler(self.handler_stdout)
		self.logger.addHandler(self.handler_stderr)

		for key in kwargs:
			if key not in Sync.valid_kwargs:
				raise AttributeError(f"'{key}' is not a valid parameter for a Sync object.")
			setattr(self, key, kwargs[key])

		self._state = Sync._SyncState.READY

	def reset(self):
		'''Discards results and allows this `Sync` object to be run again.'''

		self.results = Results(self)
		if self._state != Sync._SyncState.INVALID:
			self._state = Sync._SyncState.READY


	@property
	def src(self) -> PathType:
		return self._src

	@src.setter
	def src(self, val:PathLikeType) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		self._state = Sync._SyncState.INVALID

		if not isinstance(val, PathLikeType):
			raise TypeError(f"Bad type for property 'src' (expected {PathLikeType}): {val}")

		src: PathType
		if isinstance(val, PathType):
			src = val
		else:
			assert isinstance(val, str)
			if "@" in val:
				src = RemotePath.create(val)
			else:
				val = os.path.expanduser(val)
				val = os.path.expandvars(val)
				src = Path(val)

		assert isinstance(src, PathType)
		if src.exists() and not src.is_dir():
			raise ValueError(f"'src' is not a directory: {val}")
		if not src.exists():
			raise ValueError(f"'src' does not exist: {val}")

		if hasattr(self, "_dst") and self.dst:
			self._state = Sync._SyncState.READY

		self._src = src

	@property
	def dst(self) -> PathType:
		return self._dst

	@dst.setter
	def dst(self, val:PathLikeType) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		self._state = Sync._SyncState.INVALID

		if not isinstance(val, PathLikeType):
			raise TypeError(f"Bad type for property 'dst' (expected {PathLikeType}): {val}")

		dst: PathType
		if isinstance(val, PathType):
			dst = val
		else:
			assert isinstance(val, str)
			if "@" in val:
				dst = RemotePath.create(val)
			else:
				val = os.path.expanduser(val)
				val = os.path.expandvars(val)
				dst = Path(val)

		assert isinstance(dst, PathType)
		if dst.exists() and not dst.is_dir():
			raise ValueError(f"'dst' is not a directory: {val}")

		if hasattr(self, "_trash") and self.trash:
			if isinstance(dst, RemotePath) and not isinstance(self.trash, RemotePath):
				raise ValueError(f"'trash' is not on the same file system as 'dst': {self.trash}")

			if not isinstance(dst, RemotePath) and isinstance(self.trash, RemotePath):
				raise ValueError(f"'trash' is not on the same file system as 'dst': {self.trash}")

			# st_dev is not available over SFTP
			# TODO check also when dst doesn't exist by making a temp dir
			if not isinstance(dst, RemotePath) and dst.exists():
				if os.stat(self.trash).st_dev != os.stat(self.dst).st_dev:
					raise ValueError(f"'trash' is not on the same file system as 'dst': {self.trash}")

		if hasattr(self, "_src") and self.src:
			self._state = Sync._SyncState.READY

		self._dst = dst

	@property
	def filter(self) -> Filter:
		return self._filter

	@filter.setter
	def filter(self, val:str|Filter) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		if not isinstance(val, str|Filter):
			raise TypeError(f"Bad type for property 'filter' (expected str|Filter): {val}")

		filter: Filter
		if isinstance(val, str):
			val  = val.replace("\\", "/") if self.sftp_compat and os.name == "nt" else val
			filter = PathFilter(val)
		else:
			filter = val

		assert isinstance(filter, Filter)
		self._filter = filter

	@property
	def trash(self) -> PathType|None:
		return self._trash

	@trash.setter
	def trash(self, val:PathLikeType|None) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		if val is None:
			self._trash = None
			return

		if not isinstance(val, PathLikeType):
			raise TypeError(f"Bad type for property 'trash' (expected {PathLikeType}): {val}")
		if self.delete_files:
			raise RuntimeError("Mutually exclusive properties: 'trash' and 'delete_files'")

		trash: PathType
		if isinstance(val, PathType):
			trash = val
		else:
			assert isinstance(val, str)
			if val == "auto":
				trash = self.dst.parent / f"Trash_{self.timestamp}"
			elif "@" in val:
				#try:
				trash = RemotePath.create(val)
				#except (ValueError, ImportError) as e:
				#	raise ValueError(str(e)) from e
			else:
				val = os.path.expanduser(val)
				val = os.path.expandvars(val)
				trash = Path(val)

		assert isinstance(trash, PathType)
		if trash.exists() and not trash.is_dir():
			raise ValueError(f"'trash' is not a directory: {val}")

		# st_dev is not available over SFTP
		if not self.sftp_compat and trash.exists():
			if os.stat(trash).st_dev != os.stat(self.dst).st_dev:
				raise ValueError(f"'trash' is not on the same file system as 'dst': {trash}")

		self._trash = trash

	@property
	def delete_files(self) -> bool:
		return self._delete_files

	@delete_files.setter
	def delete_files(self, val:bool) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'delete_files' (expected bool): {val}")
		if val and self.trash:
			raise RuntimeError("Mutually exclusive properties: 'trash' and 'delete_files'")
		self._delete_files = val

	@property
	def no_create(self) -> bool:
		return self._no_create

	@no_create.setter
	def no_create(self, val:bool) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'no_create' (expected bool): {val}")
		self._no_create = val

	@property
	def force_update(self) -> bool:
		return self._force_update

	@force_update.setter
	def force_update(self, val:bool) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'force_update' (expected bool): {val}")
		self._force_update = val

	@property
	def metadata_only(self) -> bool:
		return self._metadata_only

	@metadata_only.setter
	def metadata_only(self, val:bool) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		if not isinstance(val, bool):
			raise TypeError(f"Bad type for arg 'metadata_only' (expected bool): {val}")
		self._metadata_only = val

	@property
	def rename_threshold(self) -> int|None:
		return self._rename_threshold

	@rename_threshold.setter
	def rename_threshold(self, val:int|None) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		if val is None:
			self._rename_threshold = None
			return

		if not isinstance(val, int):
			raise TypeError(f"Bad type for arg 'rename_threshold' (expected int): {val}")
		self._rename_threshold = val

	@property
	def ignore_symlinks(self) -> bool:
		return self._ignore_symlinks

	@ignore_symlinks.setter
	def ignore_symlinks(self, val:bool) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'ignore_symlinks' (expected bool): {val}")
		if val and self.follow_symlinks:
			raise RuntimeError("Mutually exclusive properties: 'follow_symlinks' and 'ignore_symlinks'")
		self._ignore_symlinks = val

	@property
	def follow_symlinks(self) -> bool:
		return self._follow_symlinks

	@follow_symlinks.setter
	def follow_symlinks(self, val:bool) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'follow_symlinks' (expected bool): {val}")
		if val and self.ignore_symlinks:
			raise RuntimeError("Mutually exclusive properties: 'ignore_symlinks' and 'follow_symlinks'")
		self._follow_symlinks = val

	@property
	def dry_run(self) -> bool:
		return self._dry_run

	@dry_run.setter
	def dry_run(self, val:bool) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		if not isinstance(val, bool):
			raise TypeError(f"Bad type for arg 'dry_run' (expected bool): {val}")
		self._dry_run = val

	@property
	def log_file(self) -> PathType|None:
		return self._log_file

	@log_file.setter
	def log_file(self, val:PathLikeType) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		if val is None:
			self.close_file_handler()
			self._log_file = None
			self._tmp_log_file = None
			self.handler_file = None
			return

		if not isinstance(val, PathLikeType):
			raise TypeError(f"Bad type for property 'log_file' (expected {PathLikeType}): {val}")

		log_file: PathType
		tmp_log_file: PathType
		if isinstance(val, Path):
			log_file     = val
			tmp_log_file = val
		elif isinstance(val, RemotePath):
			raise NotImplementedError()
		else:
			if val == "auto":
				log_file = Path.home() / f"{self.logger.name}.log"
				with tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8", delete=False) as tmp_log:
					tmp_log_file = Path(tmp_log.name)
			else:
				log_file = Path(val)
				tmp_log_file = log_file

		assert isinstance(log_file, PathType)
		assert isinstance(tmp_log_file, PathType)
		if log_file.exists():
			if not log_file.is_file():
				raise ValueError(f"'log' is not a file: {log_file}")

		handler_file = logging.FileHandler(tmp_log_file, encoding="utf-8")
		handler_file.setFormatter(_LogFileFormatter())
		handler_file.addFilter(_NonEmptyFilter())
		if self.handler_file:
			self._log_level = self.handler_file.level
		handler_file.setLevel(self._log_level)
		self.logger.addHandler(handler_file)

		self.close_file_handler()

		self._log_file = log_file
		self._tmp_log_file = tmp_log_file
		self.handler_file = handler_file

	@property
	def log_level(self) -> int:
		return self._log_level

	@log_level.setter
	def log_level(self, val:int) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		if not isinstance(val, int):
			raise TypeError(f"Bad type for property 'log_level' (expected int): {val}")
		self._log_level = val
		if self.handler_file:
			self.handler_file.level = self._log_level

	@property
	def print_level(self) -> int:
		return self.handler_stdout.level

	@print_level.setter
	def print_level(self, val:int) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		if not isinstance(val, int):
			raise TypeError(f"Bad type for property 'print_level' (expected int): {val}")
		self.handler_stdout.level = val
		self.handler_stderr.level = max(val, logging.WARNING)

	@property
	def no_header(self):
		return self.filter_tag[_RecordTag.HEADER]

	@no_header.setter
	def no_header(self, val:bool) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'no_header' (expected bool): {val}")
		self.filter_tag[_RecordTag.HEADER] = val

	@property
	def no_footer(self):
		return self.filter_tag[_RecordTag.FOOTER]

	@no_footer.setter
	def no_footer(self, val:bool) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'no_footer' (expected bool): {val}")
		self.filter_tag[_RecordTag.FOOTER] = val

	def close_file_handler(self) -> None:
		if self.handler_file:
			self.logger.removeHandler(self.handler_file)
			self.handler_file.close()
			assert self._tmp_log_file
			assert self.log_file
			_replace(self._tmp_log_file, self.log_file)

	def _walk(self, top:PathType) -> Iterator[tuple[PathType, list[os.DirEntry|RemotePath], list[os.DirEntry|RemotePath]]]:
		stack        : list[Any] = [top]
		visited_dirs : set[str]  = set()

		assert not isinstance(top, str)

		while stack:
			top = stack.pop()

			if self.follow_symlinks:
				if isinstance(top, str):
					visited_dirs.remove(top)
					continue
				d = str(top.resolve())
				if d in visited_dirs:
					self.logger.warning(f"Symlink circular reference: {top} -> {d}")
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
							if self.ignore_symlinks and entry.is_symlink():
								continue
							if self.follow_symlinks:
								is_dir = entry.is_dir(follow_symlinks=True) or entry.is_junction()
							else:
								is_dir = entry.is_dir(follow_symlinks=False)
						except OSError as e:
							self.logger.warning(_exc_summary(e))
							continue
						if is_dir:
							dirs.append(entry)
						else:
							nondirs.append(entry)
			except OSError as e:
				# top does not exist or user has no read access
				self.logger.warning(_exc_summary(e))
				continue

			#dirs.sort(key=lambda x: x.name)
			#nondirs.sort(key=lambda x: x.name)

			yield top, dirs, nondirs

			# Traverse into sub-directories
			for dir in reversed(dirs):
				# in case dir symlink status changed after yield
				new_path = top / dir.name
				if self.follow_symlinks or not new_path.is_symlink():
					stack.append(new_path)

	def _scandir(self, root:PathType) -> Iterator[_Entry]:
		'''Retrieves file information for all files under `root`, including relative paths, sizes, and mtimes.'''

		filter = self.filter.filter

		convert_sep = self.sftp_compat and os.name == "nt" and not isinstance(root, RemotePath)
		display_sep = "/" if self.sftp_compat else os.sep

		for dir, dir_entries, file_entries in self._walk(root):
			self.logger.debug(f"scanning: {dir}")

			dir_relpath = str(_relative_to(dir, root))
			normed_dir_relpath = dir_relpath
			if convert_sep:
				normed_dir_relpath = normed_dir_relpath.replace("\\", "/")
				dir_relpath = dir_relpath.replace("\\", "/")
			if not self.sftp_compat:
				normed_dir_relpath = os.path.normcase(normed_dir_relpath)

			# empty directory
			if dir_relpath != "." and filter(root, dir_relpath + display_sep):
				yield _Dir(
					path         = root / dir_relpath,
					relpath      = dir_relpath,
					norm_relpath = normed_dir_relpath,
					num_files    = len(file_entries),
					num_dirs     = len(dir_entries),
				)
				if not file_entries and not dir_entries:
					continue

			# prune search tree
			i = 0
			while i < len(dir_entries):
				subdirname = dir_entries[i].name
				subdir_path = dir / subdirname
				subdir_relpath = str(_relative_to(subdir_path, root))
				if not filter(root, subdir_relpath + display_sep):
					del dir_entries[i]
					continue
				i += 1

			# prune files
			for entry in file_entries:
				# Ignore non-standard files (e.g., sockets, named pipes, block & character devices), except symlinks.
				if self.follow_symlinks:
					if not entry.is_file(follow_symlinks=True):
						continue
				else:
					if not entry.is_file(follow_symlinks=False) and not entry.is_symlink():
						continue

				filename = entry.name
				file_path = dir / filename
				file_relpath = str(_relative_to(file_path, root))

				normed_file_relpath = file_relpath
				if convert_sep:
					normed_file_relpath = normed_file_relpath.replace("\\", "/")
					file_relpath = file_relpath.replace("\\", "/")
				if not self.sftp_compat:
					normed_file_relpath = os.path.normcase(normed_file_relpath)

				if filter(root, file_relpath):
					stat  = entry.stat(follow_symlinks=self.follow_symlinks)
					size  = stat.st_size
					mtime = stat.st_mtime
					if size is None or mtime is None:
						# Ignore files with unknown size or mtime.
						continue
					if self.sftp_compat:
						mtime = float(int(mtime))

					yield _File(
						path         = root / file_relpath,
						relpath      = file_relpath,
						norm_relpath = normed_file_relpath,
						meta         = _Metadata(size=size, mtime=mtime),
					)

	def _operations(
			self,
			*,
			src_entries : Iterator[_Entry],
			dst_entries : Iterator[_Entry],
		) -> Iterator[Operation]:
		'''Generator of file system operations to perform for this sync.'''

		display_sep = "/" if self.sftp_compat else os.sep

		src_relpaths = {}
		dst_relpaths = {}

		src_meta = {}
		dst_meta = {}

		src_norm_relpaths = set()
		dst_norm_relpaths = set()

		src_dirs = set()
		dst_dirs = set()

		src_empty_dirs = set()
		dst_empty_dirs = set()

		dst_dir_size: CounterType[str] = Counter()

		for src_entry in src_entries:
			src_relpaths[src_entry.norm_relpath] = src_entry.relpath
			if isinstance(src_entry, _File):
				src_meta[src_entry.norm_relpath] = src_entry.meta
				src_norm_relpaths.add(src_entry.norm_relpath)
			else:
				src_entry = cast(_Dir, src_entry)
				dir_size = src_entry.num_files + src_entry.num_dirs
				if not dir_size:
					src_empty_dirs.add(src_entry.norm_relpath)
				src_dirs.add(src_entry.norm_relpath)

		for dst_entry in dst_entries:
			dst_relpaths[dst_entry.norm_relpath] = dst_entry.relpath
			if isinstance(dst_entry, _File):
				dst_meta[dst_entry.norm_relpath] = dst_entry.meta
				dst_norm_relpaths.add(dst_entry.norm_relpath)
			else:
				dst_entry = cast(_Dir, dst_entry)
				dir_size = dst_entry.num_files + dst_entry.num_dirs
				if not dir_size:
					dst_empty_dirs.add(dst_entry.norm_relpath)
				dst_dirs.add(dst_entry.norm_relpath)
				dst_dir_size[dst_entry.norm_relpath] = dir_size

		self.logger.debug(f"{len(src_norm_relpaths)=}")
		self.logger.debug(f"{len(dst_norm_relpaths)=}")

		src_only_norm_relpaths = sorted(src_norm_relpaths.difference(dst_norm_relpaths))
		dst_only_norm_relpaths = sorted(dst_norm_relpaths.difference(src_norm_relpaths))
		both_norm_relpaths     = sorted(src_norm_relpaths.intersection(dst_norm_relpaths))

		# Ignore remote files with invalid characters when copying to Windows
		if os.sep == "nt" and isinstance(self.src, RemotePath):
			for path in src_only_norm_relpaths.copy():
				if os.path.isreserved(path) or "\\" in path:
					self.logger.warning(f"Ignoring incompatible remote file: {path}")
					src_only_norm_relpaths.remove(path)

		self.logger.debug(f"{len(src_only_norm_relpaths)=}")
		self.logger.debug(f"{len(dst_only_norm_relpaths)=}")
		self.logger.debug(f"{len(both_norm_relpaths)=}")
		self.logger.debug(f"{len(src_dirs)=}")
		self.logger.debug(f"{len(dst_dirs)=}")
		self.logger.debug(f"{len(src_empty_dirs)=}")
		self.logger.debug(f"{len(dst_empty_dirs)=}")

		self.logger.debug(f"{src_only_norm_relpaths[:10]=}")
		self.logger.debug(f"{dst_only_norm_relpaths[:10]=}")
		self.logger.debug(f"{both_norm_relpaths[:10]=}")
		self.logger.debug(f"{list(islice(src_dirs, 10))=}")
		self.logger.debug(f"{list(islice(dst_dirs, 10))=}")
		self.logger.debug(f"{list(islice(src_empty_dirs, 10))=}")
		self.logger.debug(f"{list(islice(dst_empty_dirs, 10))=}")

		def _automatic_dir_delete_ops(deleted_norm_relpath:str):
			norm_relpath = os.path.dirname(deleted_norm_relpath) # should keep / separators on Windows
			while norm_relpath and norm_relpath not in src_dirs:
				relpath = dst_relpaths[norm_relpath]
				dir = self.dst / relpath
				if dst_dir_size[norm_relpath]:
					break
				yield DeleteDirOperation(
					dst       = dir,
					summary   = f"- {relpath}{display_sep}"
				)
				norm_relpath = os.path.dirname(norm_relpath)
				if norm_relpath:
					dst_dir_size[norm_relpath] -= 1
					assert dst_dir_size[norm_relpath] >= 0

		# Delete empty directories now in case any new files needs to take their places
		dst_only_empty_dirs = dst_empty_dirs.difference(src_dirs)
		for dst_norm_relpath in dst_only_empty_dirs:
			dst_relpath = dst_relpaths[dst_norm_relpath]
			dst = self.dst / dst_relpath
			assert not any(dst.iterdir())
			yield DeleteDirOperation(
				dst       = dst,
				summary   = f"- {dst_relpath}{display_sep}"
			)
			parent_dir = os.path.dirname(dst_norm_relpath)
			if parent_dir:
				dst_dir_size[parent_dir] -= 1
				assert dst_dir_size[parent_dir] >= 0
			yield from _automatic_dir_delete_ops(dst_norm_relpath)

		# Rename files
		if self.rename_threshold is not None:
			src_only_norm_relpath_from_meta = _reverse_dict({norm_relpath:src_meta[norm_relpath] for norm_relpath in src_only_norm_relpaths})
			dst_only_norm_relpath_from_meta = _reverse_dict({norm_relpath:dst_meta[norm_relpath] for norm_relpath in dst_only_norm_relpaths})

			for dst_norm_relpath in list(dst_only_norm_relpaths): # dst_only_norm_relpaths is changed inside the loop
				# Ignore small files
				if dst_meta[dst_norm_relpath].size < self.rename_threshold:
					continue
				try:
					src_norm_relpath = src_only_norm_relpath_from_meta[dst_meta[dst_norm_relpath]]
					# Ignore if there are multiple candidates
					if src_norm_relpath is None:
						continue

					dst_norm_relpath = dst_only_norm_relpath_from_meta[dst_meta[dst_norm_relpath]]
					# Ignore if there are multiple candidates
					if dst_norm_relpath is None:
						continue

					rename_from = dst_relpaths[dst_norm_relpath]
					rename_to = src_relpaths[src_norm_relpath]

					# Ignore if last 1kb do not match
					if not self.metadata_only:
						on_dst = self.dst / rename_from
						on_src = self.src / rename_to
						try:
							if not _last_bytes(on_src) == _last_bytes(on_dst):
								continue
						except OSError as e:
							self.logger.warning(_exc_summary(e))
							continue

					src_only_norm_relpaths.remove(src_norm_relpath)
					dst_only_norm_relpaths.remove(dst_norm_relpath)

					parent_dir = os.path.dirname(src_norm_relpath)
					if parent_dir:
						if dst_dir_size[parent_dir] == 0:
							yield CreateDirOperation(
								dst       = self.dst / dst_relpaths[parent_dir],
								summary   = f"+ {parent_dir}{display_sep}"
							)
							dst_dir_size[parent_dir] += 1
					yield RenameFileOperation(
						dst       = self.dst / rename_from,
						target    = self.dst / rename_to,
						summary   = f"R {rename_from} -> {rename_to}"
					)
					parent_dir = os.path.dirname(dst_norm_relpath)
					if parent_dir:
						dst_dir_size[parent_dir] -= 1
						assert dst_dir_size[parent_dir] >= 0
					yield from _automatic_dir_delete_ops(dst_norm_relpath)

				except KeyError:
					# dst file not a result of a rename
					continue

		# Delete files
		if self.delete_files:
			for dst_norm_relpath in dst_only_norm_relpaths:
				dst_relpath = dst_relpaths[dst_norm_relpath]
				yield DeleteFileOperation(
					dst       = self.dst / dst_relpath,
					byte_diff = -dst_meta[dst_norm_relpath].size,
					summary   = f"- {dst_relpath}"
				)
				parent_dir = os.path.dirname(dst_norm_relpath)
				if parent_dir:
					dst_dir_size[parent_dir] -= 1
					assert dst_dir_size[parent_dir] >= 0
				yield from _automatic_dir_delete_ops(dst_norm_relpath)

		# Send files to trash
		elif self.trash is not None:
			for dst_norm_relpath in dst_only_norm_relpaths:
				dst_relpath = dst_relpaths[dst_norm_relpath]
				# Don't want the summary of this logged liked the others, since it takes place in the trash folder.
				# As a result, this Operation can't be added to a list of failed Operations during the sync.
				#parent_dir = os.path.dirname(dst_norm_relpath)
				#if parent_dir:
				#	if dst_dir_size[parent_dir] == 0:
				#		yield CreateDirOperation(
				#			dst       = self.trash / dst_relpaths[parent_dir],
				#			summary   = f"+ {parent_dir}{display_sep}"
				#		)
				#	dst_dir_size[parent_dir] += 1
				yield TrashFileOperation(
					dst       = self.dst / dst_relpath,
					target    = self.trash / dst_relpath,
					summary   = f"T {dst_relpath}"
				)
				parent_dir = os.path.dirname(dst_norm_relpath)
				if parent_dir:
					dst_dir_size[parent_dir] -= 1
					assert dst_dir_size[parent_dir] >= 0
				yield from _automatic_dir_delete_ops(dst_norm_relpath)

		# Create files
		if not self.no_create:
			for src_norm_relpath in src_only_norm_relpaths:
				src_relpath = src_relpaths[src_norm_relpath]
				parent_dir = os.path.dirname(src_norm_relpath)
				if parent_dir:
					if dst_dir_size[parent_dir] == 0:
						yield CreateDirOperation(
							dst       = self.dst / src_relpaths[parent_dir],
							summary   = f"+ {parent_dir}{display_sep}"
						)
					dst_dir_size[parent_dir] += 1
				yield CreateFileOperation(
					src       = self.src / src_relpath,
					dst       = self.dst / src_relpath,
					byte_diff = src_meta[src_norm_relpath].size,
					summary   = f"+ {src_relpath}"
				)
				# No need to update dir size anymore

		# Update files that have newer mtimes
		for norm_relpath in both_norm_relpaths:
			src_relpath = src_relpaths[norm_relpath]
			dst_relpath = dst_relpaths[norm_relpath]
			src_time = src_meta[norm_relpath].mtime
			dst_time = dst_meta[norm_relpath].mtime
			byte_diff = src_meta[norm_relpath].size - dst_meta[norm_relpath].size
			if src_time > dst_time:
				yield UpdateFileOperation(
					src       = self.src / src_relpath,
					dst       = self.dst / dst_relpath,
					byte_diff = byte_diff,
					summary   = f"U {dst_relpath}"
				)
			elif src_time < dst_time:
				if self.force_update:
					yield UpdateFileOperation(
						src       = self.src / src_relpath,
						dst       = self.dst / dst_relpath,
						byte_diff = byte_diff,
						summary   = f"U {dst_relpath}"
					)
				else:
					self.logger.warning(f"'src' file is older than 'dst' file, skipping update: {norm_relpath}")

		# Create empty directories
		if not self.no_create:
			src_only_empty_dirs = src_empty_dirs.difference(dst_empty_dirs)#.difference(dst_entries.nonempty_dirs)
			for norm_relpath in src_only_empty_dirs:
				if norm_relpath not in dst_dirs:
					src_relpath = src_relpaths[norm_relpath]
					yield CreateDirOperation(
						dst       = self.dst / src_relpath,
						summary   = f"+ {src_relpath}{display_sep}"
					)

	def run(self) -> Results:
		'''Runs the sync operation. `run()` does not raise errors. If an error occurs, it will be available in the returned `Results` object.'''

		if self._state != Sync._SyncState.READY:
			raise StateError("Sync object state is not READY.")

		try:
			HEADER  = _RecordTag.HEADER.dict()
			FOOTER  = _RecordTag.FOOTER.dict()
			SYNC_OP = _RecordTag.SYNC_OP.dict()

			self.logger.debug(f"Starting backup: {self.src=}, {self.dst=}, {self.filter=}, {self.trash=}, {self.delete_files=}, {self.no_create=}, {self.force_update=}, {self.metadata_only=}, {self.rename_threshold=}, {self.ignore_symlinks=}, {self.follow_symlinks=}, {self.dry_run=}, {self.log_file=}, {self.log_level=}, {self.print_level=}, {self.no_header=}, {self.no_footer=}, {self.sftp_compat=}".replace("self.", ""))
			self.logger.debug("")

			width = max(len(str(self.src)), len(str(self.dst)), 7) + 3
			self.logger.info("   " + str(self.src), extra=HEADER)
			self.logger.info("-> " + str(self.dst), extra=HEADER)
			self.logger.info("-" * width, extra=HEADER)

			src_entries = self._scandir(self.src)
			if self.dst.exists():
				dst_entries = self._scandir(self.dst)
			else:
				dst_entries = iter([])

			for op in self._operations(
				src_entries = src_entries,
				dst_entries = dst_entries,
			):
				if any(op.depends_on(failed_op) for failed_op, _ in self.results.sync_errors):
					self.logger.debug(f"Dependent failure: {op.summary}")
					continue

				self.logger.info(op.summary, extra=SYNC_OP)

				if not self.dry_run:
					try:
						op.perform(self)
						self.results.tally_success(op)
					except OSError as e:
						self.logger.error(_exc_summary(e))
						self.results.tally_failure(op, e)

			self.results.status = Results.Status.COMPLETED
		except KeyboardInterrupt as e:
			self.results.status = Results.Status.INTERRUPTED_BY_USER
			self.results.error = e
			raise e
		except ConnectionError as e:
			self.logger.info("")
			self.logger.critical(f"Connection Error: {e}", exc_info=False)
			self.results.status = Results.Status.CONNECTION_ERROR
			self.results.error = e
		except Exception as e:
			self.logger.info("")
			self.logger.critical("An unexpected error occurred.", exc_info=True)
			self.results.status = Results.Status.INTERRUPTED_BY_ERROR
			self.results.error = e
		finally:
			self.logger.info("-" * width, extra=FOOTER)
			for line in self.results.summary():
				self.logger.info(line, extra=FOOTER)

			if self.results.failure_count:
				self.logger.info("", extra=FOOTER)
				self.logger.info(f"There were {self.results.failure_count} errors.", extra=FOOTER)
				if self.results.failure_count <= 10 and self.results.total_count >= 50:
					self.logger.info("Errors are reprinted below for convenience.", extra=FOOTER)
					for op, exc in self.results.sync_errors:
						self.logger.info(_exc_summary(exc), extra=FOOTER)

			if self.log_file:
				self.close_file_handler()

			self.logger.info("", extra=FOOTER)
			self._state = Sync._SyncState.TERMINATED

		return self.results

	def watch(self):
		if isinstance(self.src, RemotePath):
			raise UnsupportedOperationError("Can only watch local directories.")
		else:
			_LocalWatcher(self).watch()

def _copy(src:PathType, dst:PathType, *, exist_ok:bool = True, follow_symlinks:bool = False) -> None:
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
		# repalce the temp file
		_replace(dst_tmp, dst)
	finally:
		dst_tmp.unlink(missing_ok=True)

def _move(src:PathType, dst:PathType, *, exist_ok:bool = False) -> None:
	'''Move file from `src` to `dst`. Existing files will be overwritten if `exist_ok` is `True`. Otherwise this method will raise a `FileExistsError`.'''

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
	_replace(src, dst)

def _replace(src:PathType, dst:PathType) -> None:
	'''Move file from `src` to `dst`. Existing files will be overwritten..'''

	try:
		# Replace the dst file with the tmp file
		if isinstance(src, Path):
			src.replace(cast(Path, dst))
		else:
			src.replace(cast(RemotePath, dst))
	except PermissionError as e:
		# Remove read-only flag and try again
		make_readonly = False
		try:
			dst_stat = dst.stat()
			if dst_stat.st_mode is None or not (dst_stat.st_mode & stat.S_IREAD):
				raise e
			dst.chmod(stat.S_IWRITE, follow_symlinks=False)
			make_readonly = True
			if isinstance(src, Path):
				src.replace(cast(Path, dst))
			else:
				src.replace(cast(RemotePath, dst))
		finally:
			if make_readonly:
				dst.chmod(stat.S_IREAD, follow_symlinks=False)

# this is just needed to stop mypy from complaining
def _relative_to(path:PathType, root:PathType) -> PathType:
	if isinstance(path, Path):
		return path.relative_to(cast(Path, root))
	else:
		return path.relative_to(cast(RemotePath, root))

def _last_bytes(file:PathType, n:int = 1024) -> bytes:
	'''Reads and returns the last `n` bytes of a file.'''

	file_size = file.stat().st_size
	if file_size is None:
		raise OSError(f"Could not get file size: {file}")
	bytes_to_read = file_size if n > file_size else n
	with file.open("rb") as f:
		f.seek(-bytes_to_read, os.SEEK_END)
		return f.read()
