# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import sys
import os
import ntpath
import stat
import shutil
import logging
import tempfile
import uuid
from enum import Enum
from pathlib import Path, PurePath, PureWindowsPath, PurePosixPath
from itertools import islice
from functools import cmp_to_key
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Literal, Iterator, Counter as CounterType, cast
from collections import Counter, namedtuple

from .filter import Filter, PathFilter
from .helpers import _reverse_dict, _human_readable_size, _merge_iters
from .sftp import RemotePath, _RemotePathScanner
from .watch import _LocalWatcher
from .types import PathType, PathLikeType
from .errors import MetadataUpdateError, BrokenSymlinkError, IncompatiblePathError, NewerInDstError, StateError, ImmutableObjectError, UnsupportedOperationError
from .log import logger, _RecordTag, _DebugInfoFilter, _NonEmptyFilter, _TagFilter, _ConsoleFormatter, _LogFileFormatter, _exc_summary

@dataclass(frozen=True)
class _Metadata:
	'''File metadata that will be used to find probable duplicates.'''

	size  : int
	mtime : float

class _Relpath:
	'''Filesystem entries yielded by `_scandir()`.'''

	def __init__(self, relpath, in_sep, out_sys):
		self.relpath = relpath
		self.in_sep = in_sep
		self.out_sys = out_sys

		if out_sys == "nt":
			if (in_sep == "/" and "\\" in relpath) or ntpath.isreserved(relpath):
				raise IncompatiblePathError("Incompatible path for this system", str(path))
			parts = relpath.split(in_sep)
			self.norm = tuple(p.lower() for p in parts)
			self.name = parts[-1]
		else:
			self.norm = tuple(relpath.split(in_sep))
			self.name = self.norm[-1]

		self._hash = hash(self.norm)

		assert ".." not in self.norm

	def __eq__(self, other):
		return self.norm == other.norm

	def __hash__(self):
		return self._hash

	def __bool__(self):
		return bool(self.relpath)

	def __lt__(self, other:"_Relpath"):
		return self.norm < other.norm

	#def __contains__(self, val):
	#	return val in self.relpath

	def __str__(self):
		return self.relpath

	def __repr__(self):
		return self.relpath

	def __add__(self, other):
		return _Relpath(self.relpath + other, self.in_sep, self.out_sys)

	def __rtruediv__(self, other:PathType):
		return other / self.relpath

	def is_relative_to(self, other):
		return all(a==b for a,b in zip(self.norm, other.norm)) or other.relpath == "."

#@dataclass(frozen=True, repr=False, eq=False, unsafe_hash=False)
class _File(_Relpath):
	pass
	#meta     : _Metadata

class _Symlink(_File): # TODO use this to replace is_symlink() check inside _operations
	pass

#@dataclass(frozen=True, repr=False, eq=False, unsafe_hash=False)
class _Dir(_Relpath):
	pass
	#num_files    : int
	#num_dirs     : int

	#def __len__(self):
	#	return self.num_files + self.num_dirs

@dataclass(frozen=True)
class Operation:
	'''Filesystem operation yielded by `_operations()`.'''

	name  = ""

	summary   : str
	dst       : _Relpath
	src       : _Relpath | None = None
	target    : _Relpath | None = None
	byte_diff : int = 0

	def perform(self, sync:"Sync"):
		raise NotImplementedError()

	def depends_on(self, op:"Operation"):
		'''Returns `True` if this Operation would fail if the Operation `op` were to fail beforehand or not to occur.'''

		in_path  = None
		out_path = None

		if isinstance(self, CreateFileOperation):
			in_path = self.dst
		if isinstance(self, CreateSymlinkOperation):
			in_path = self.dst
		elif isinstance(self, RenameFileOperation):
			in_path = self.target
		elif isinstance(self, TrashFileOperation):
			in_path = self.target
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
			return in_path.is_relative_to(out_path)

	def __lt__(self, other):
		return self.dst < other.dst

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
		_copy(sync.src / self.src, sync.dst / self.dst, follow_symlinks=sync.follow_symlinks)

@dataclass(frozen=True)
class UpdateFileOperation(Operation):
	name = "Update"

	def __post_init__(self):
		assert self.src is not None
		assert self.target is None

	def perform(self, sync:"Sync"):
		assert self.src is not None
		_copy(sync.src / self.src, sync.dst / self.dst, follow_symlinks=sync.follow_symlinks)

@dataclass(frozen=True)
class RenameFileOperation(Operation):
	name = "Rename"

	def __post_init__(self):
		assert self.src is None
		assert self.target is not None

	def perform(self, sync:"Sync"):
		assert self.target is not None
		# TODO two-step if Windows and only changing case
		_move(sync.dst / self.dst, sync.dst / self.target)

@dataclass(frozen=True)
class DeleteFileOperation(Operation):
	name = "Delete"

	def __post_init__(self):
		assert self.src is None
		assert self.target is None

	def __lt__(self, other):
		if self.dst.is_relative_to(other.dst):
			return True
		return self.dst < other.dst

	def perform(self, sync:"Sync"):
		(sync.dst / self.dst).unlink()

@dataclass(frozen=True)
class TrashFileOperation(Operation):
	name = "Trash"

	def __post_init__(self):
		assert self.src is None
		assert self.target is None

	def __lt__(self, other):
		if self.dst.is_relative_to(other.dst):
			return True
		return self.dst < other.dst

	def perform(self, sync:"Sync"):
		assert self.target is None
		assert sync.trash is not None
		_move(sync.dst / self.dst, sync.trash / self.dst)

@dataclass(frozen=True)
class TrashDirOperation(Operation):
	name = "Create Dir"

	def __post_init__(self):
		assert self.src is None
		assert self.target is None

	def __lt__(self, other):
		if self.dst.is_relative_to(other.dst):
			return True
		return self.dst < other.dst

	def perform(self, sync:"Sync"):
		assert sync.trash is not None
		(sync.trash / self.dst).mkdir(exist_ok=True, parents=True)
		(sync.dst / self.dst).rmdir()

@dataclass(frozen=True)
class DeleteDirOperation(Operation):
	name = "Delete Dir"

	def __post_init__(self):
		assert self.src is None
		assert self.target is None

	def __lt__(self, other):
		if self.dst.is_relative_to(other.dst):
			return True
		return self.dst < other.dst

	def perform(self, sync:"Sync"):
		(sync.dst / self.dst).rmdir()

@dataclass(frozen=True)
class CreateDirOperation(Operation):
	name = "Create Dir"

	def __post_init__(self):
		assert self.src is None
		assert self.target is None

	def perform(self, sync:"Sync"):
		(sync.dst / self.dst).mkdir(exist_ok=True, parents=True)

@dataclass(frozen=True)
class CreateSymlinkOperation(Operation):
	name = "Create Symlink"

	def __post_init__(self):
		assert self.src is not None # symlink file
		assert self.target is None

	def perform(self, sync:"Sync"):
		assert self.src is not None

		src = sync.src / self.src
		dst = sync.dst / self.dst

		st = src.stat()

		target:str|None
		if isinstance(src, Path):
			target = os.readlink(src)
			if target is None:
				raise BrokenSymlinkError("Broken Symlink", str(src))
			if os.name == "nt" and (target.startswith("\\\\?\\") or target.startswith("\\??\\")):
				target = target[4:]
		else:
			assert isinstance(src, RemotePath)
			target = RemotePath.readlink(src)
			if target is None:
				raise BrokenSymlinkError("Broken Symlink", str(src))

		assert target is not None

		if sync.translate_symlinks:
			src_path    : PurePath
			dst_path    : PurePath
			target_path : PurePath

			# convert target, src_path to sync.in_sep
			if sync.in_sys == "nt":
				target_path = PureWindowsPath(target)
				if sync.in_sep == "\\":
					src_path = PureWindowsPath(_convert_sep(str(sync.src), "\\", "\\"))
				else:
					src_path = PureWindowsPath(_convert_sep(str(sync.src), "/", "\\"))
			else:
				target_path = PurePosixPath(target)
				if sync.in_sep == "\\":
					src_path = PurePosixPath(_convert_sep(str(sync.src), "\\", "/"))
				else:
					src_path = PurePosixPath(_convert_sep(str(sync.src), "/", "/"))

			# convert dst_path to sync.out_sep
			if sync.out_sys == "nt":
				if sync.out_sep == "\\":
					dst_path = PureWindowsPath(_convert_sep(str(sync.dst), "\\", "\\"))
				else:
					dst_path = PureWindowsPath(_convert_sep(str(sync.dst), "/", "\\"))
			else:
				if sync.out_sep == "\\":
					dst_path = PurePosixPath(_convert_sep(str(sync.dst), "\\", "/"))
				else:
					dst_path = PurePosixPath(_convert_sep(str(sync.dst), "/", "/"))

			if sync.in_sys == "linux" and sync.out_sys == "nt":
				if ntpath.isreserved(target_path):
					raise IncompatiblePathError("Incompatible path for dst system", str(target_path))

			if target_path.is_relative_to(src_path):
				# translate absolute paths
				rel_target = target_path.relative_to(src_path)
				rel_target = _convert_sep(str(rel_target), sync.in_sep, sync.out_sep)
				target_path = dst_path / rel_target
			else:
				# translate relative paths
				target_path = _convert_sep(str(target_path), sync.in_sep, sync.out_sep)

		try:
			_create_symlink(dst, target=str(target_path), st=st)
		except UnsupportedOperationError as e:
			sync.logger.warning(_exc_summary(e))

class Results:
	'''Various statistics and other information returned by `sync()`.'''

	Counts = namedtuple("Counts", ["success", "failure"])

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
			return Results.Counts(success=self.success_counts[key.name], failure=self.failure_counts[key.name])
		else:
			return self.counts[key]

	def summary(self):
		status = self.status.name.replace("_", " ").title()
		lines = []
		if self.sync.dry_run:
			lines.append(f"Status: {status} (Dry Run)")
			lines.append(f"Net Change: {_human_readable_size(self.byte_diff)} (Estimated)")
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

	_RAISE_UNKNOWN_ERRORS = False

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
		"force",
		"global_renames",
		"metadata_only",
		"rename_threshold",
		"translate_symlinks",
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
			translate_symlinks (bool) : Whether to copy symbolic links literally, without translation to the dst system. (Defaults to `True`.)
			ignore_symlinks   (bool) : Whether to ignore symbolic links under `src` and `dst`. Note that `src` and `dst` themselves will be followed regardless of this argument. Mutually exclusive with `follow_symlinks`. (Defaults to `False`.)
			follow_symlinks   (bool) : Whether to follow symbolic links under `src` and `dst`. Note that `src` and `dst` themselves will be followed regardless of this argument. Mutually exclusive with `ignore_symlinks`. (Defaults to `False`.)

			trash  (str or PathLike) : The path of the root directory to move "extra" files to. ("Extra" files are those that are in `dst` but not `src`.) Must be on the same file system as `dst`. If set to "auto", then a directory will automatically be made next to `dst`. "Extra" files will not be moved if this argument is `None`. Mutually exclusive with `delete_files`. (Defaults to `None`.)
			delete_files      (bool) : Whether to permanently delete 'extra' files (those that are in `dst` but not `src`). Mutually exclusive with `trash`. (Defaults to `False`.)
			no_create         (bool) : Whether to prevent the creation of any files or directories in `dst`. (Defaults to `False`.)
			force             (bool) : Whether to force `dst` to match `src`. This will allow replacement of any newer files in `dst` with older copies in `src`, and it will allow files to replace dirs (or vice versa) where their names match. (Defaults to `False`.)
			global_renames    (bool) : Whether to search for renamed files between directories. If `False`, the search will stay within each directory. (Defaults to `False`.)
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
		self._force            : bool = False
		self._global_renames   : bool = False
		self._metadata_only    : bool = False
		self._rename_threshold : int|None = 10000
		self._translate_symlinks : bool = True
		self._ignore_symlinks  : bool = False
		self._follow_symlinks  : bool = False
		self._dry_run          : bool = False
		self._log_file         : PathType|None = None
		self._log_level        : int = logging.DEBUG

		self._tmp_log_file     : PathType|None = None

		self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
		self.sftp_compat = isinstance(self.src, RemotePath) or isinstance(self.dst, RemotePath) # TODO check which uses of this should be changed to out_sep
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
		#assert not self.logger.handlers # TODO no idea why this fails, logger should be brand new
		self.logger.handlers = []
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

		src = src.resolve()

		if hasattr(self, "_dst") and self.dst:
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
			self._state = Sync._SyncState.READY

		if isinstance(src, RemotePath):
			self.in_sys = "linux" if RemotePath.sep(src) == "/" else "nt" # TODO RemotePath.os_name, whcih may also return "darwin"
		else:
			self.in_sys = os.name
		self.in_sep = "/" if isinstance(src, RemotePath) else os.sep
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

		dst = dst.resolve()

		if hasattr(self, "_src") and self.src:
			err = None
			if self.src == dst:
				err = "'src' and 'dst' connot be the same directory"
			else:
				try:
					if self.src.is_relative_to(dst):
						err = "'src' cannot be a child of 'dst'"
				except Exception:
					pass
				try:
					if dst.is_relative_to(self.src):
						err = "'dst' cannot be a child of 'src'"
				except Exception:
					pass
				if err:
					raise ValueError(err)
			self._state = Sync._SyncState.READY

		if isinstance(dst, RemotePath):
			self.out_sys = "linux" if RemotePath.sep(dst) == "/" else "nt" # TODO RemotePath.os_name, whcih may also return "darwin"
		else:
			self.out_sys = os.name
		self.out_sep = "/" if isinstance(dst, RemotePath) else os.sep
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
		self._filter = filter # TODO convert filter based on in_sep, out_sep?

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
			raise StateError("Mutually exclusive properties: 'trash' and 'delete_files'")

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
		try:
			if not self.sftp_compat:
				if os.stat(trash).st_dev != os.stat(self.dst).st_dev:
					raise ValueError(f"'trash' is not on the same file system as 'dst': {trash}")
		except FileNotFoundError:
			pass

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
			raise StateError("Mutually exclusive properties: 'trash' and 'delete_files'")
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
	def force(self) -> bool:
		return self._force

	@force.setter
	def force(self, val:bool) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'force' (expected bool): {val}")
		self._force = val

	@property
	def global_renames(self) -> bool:
		return self._global_renames

	@global_renames.setter
	def global_renames(self, val:bool) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		if not isinstance(val, bool):
			raise TypeError(f"Bad type for arg 'global_renames' (expected bool): {val}")
		self._global_renames = val

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
	def translate_symlinks(self) -> bool:
		return self._translate_symlinks

	@translate_symlinks.setter
	def translate_symlinks(self, val:bool) -> None:
		if self._state == Sync._SyncState.RUNNING or self._state == Sync._SyncState.TERMINATED:
			raise ImmutableObjectError("Cannot modify Sync object after calling run().")

		if not isinstance(val, bool):
			raise TypeError(f"Bad type for property 'translate_symlinks' (expected bool): {val}")
		self._translate_symlinks = val

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
			raise StateError("Mutually exclusive properties: 'follow_symlinks' and 'ignore_symlinks'")
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
			raise StateError("Mutually exclusive properties: 'ignore_symlinks' and 'follow_symlinks'")
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

	def _scandir(self, root:PathType) -> Iterator[tuple[_Dir, list[_Dir], list[_File], int, dict[_File, _Metadata], set[int]]]:
		stack        : list[Any] = [root]
		visited_dirs : set[str]  = set()

		assert not isinstance(root, str)

		filter = self.filter.filter
		src_root_name = root.name + self.in_sep if self.trash else ""

		while stack:
			parent = stack.pop()
			self.logger.debug(f"scanning: {parent}")

			if self.follow_symlinks:
				if isinstance(parent, str):
					visited_dirs.remove(parent)
					continue
				d = str(parent.resolve())
				if d in visited_dirs:
					self.logger.warning(f"Symlink circular reference: {parent} -> {d}")
					continue
				stack.append(d)
				visited_dirs.add(d)

			assert not isinstance(parent, str)

			dir_entries    = []
			file_entries = []

			try:
				scanner = _RemotePathScanner(parent) if isinstance(parent, RemotePath) else os.scandir(parent)
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
							dir_entries.append(entry)
						else:
							file_entries.append(entry)
			except OSError as e:
				# parent does not exist or user has no read access
				self.logger.warning(_exc_summary(e))
				continue

			dir_size = len(file_entries) + len(dir_entries)

			dir_entries.sort(key = lambda x: x.name)
			#file_entries.sort(key = lambda x: x.name)

			# prune dirs
			dirs = []
			#dir_metadata = {}
			i = 0
			while i < len(dir_entries):
				entry = dir_entries[i]
				dirname = entry.name
				dir_path = parent / dirname
				dir_relpath = str(_relative_to(dir_path, root))

				if not filter(root, dir_relpath + self.out_sep):
					del dir_entries[i]
					continue

				try:
					d = _Dir(
						relpath = dir_relpath,
						in_sep = self.in_sep,
						out_sys = self.out_sys,
					)
					d._index = i # for ease of removal in later steps
				except IncompatiblePathError:
					self.logger.warning(f"Ignoring incompatible parent: {src_root_name}{dir_relpath}{self.in_sep}")
					continue

				dirs.append(d)
				i += 1

			# prune files
			files = []
			file_metadata = {}
			i = 0
			for entry in file_entries:
				# Ignore non-standard files (e.g., sockets, named pipes, block & character devices), except symlinks.
				if self.follow_symlinks:
					if not entry.is_file(follow_symlinks=True):
						continue
				else:
					if not entry.is_file(follow_symlinks=False) and not entry.is_symlink():
						continue

				filename = entry.name
				file_path = parent / filename
				file_relpath = str(_relative_to(file_path, root))
				try:
					f = _File(
						relpath = file_relpath,
						in_sep = self.in_sep,
						out_sys = self.out_sys,
					)
				except IncompatiblePathError as e:
					self.logger.warning(f"Ignoring incompatible file: {src_root_name}{file_relpath}")
					continue

				if filter(root, file_relpath):
					stat  = entry.stat(follow_symlinks=self.follow_symlinks)
					size  = stat.st_size
					mtime = stat.st_mtime
					if size is None or mtime is None:
						self.logger.warning(f"Ignoring file with unknown metadata: {src_root_name}{file_relpath}")
						continue

					if self.sftp_compat:
						mtime = float(int(mtime))

					files.append(f)
					file_metadata[f] = _Metadata(size=size, mtime=mtime)
					i += 1

			parent_relpath = str(_relative_to(parent, root))
			parent_dir = _Dir(
				relpath = parent_relpath,
				in_sep = self.in_sep,
				out_sys = self.out_sys,
			)

			rejected: set[int] = set()
			yield parent_dir, dirs, files, dir_size, file_metadata, rejected

			# remove dirs that _operations rejected
			num_deleted = 0
			for i in sorted(rejected):
				del dir_entries[i - num_deleted]
				num_deleted += 1

			# Traverse into sub-directories
			for d in reversed(dir_entries):
				# in case dir symlink status changed after yield
				new_path = parent / d.name
				if self.follow_symlinks or not new_path.is_symlink():
					stack.append(new_path)

	def _dir_diff(self, comp, src_entries, dst_entries) -> tuple[
		_Dir,
		dict[_File, _Metadata],
		dict[_File, _Metadata],
		set[_Dir],
		set[_Dir],
		set[_File],
		set[_File],
		dict[_Dir, _Dir],
		dict[_File, _File],
		set[int],
		set[int],
	]: # TODO return namedtuple
		'''Returns a tuple of the differences between of two directories.'''

		src_file_metadata: dict[_File, _Metadata] = {}
		dst_file_metadata: dict[_File, _Metadata] = {}

		if src_entries:
			src_parent, src_dirs, src_files, src_dir_size, src_file_metadata, reject_src_dir = src_entries
			parent_dir = src_parent
		if dst_entries:
			dst_parent, dst_dirs, dst_files, dst_dir_size, dst_file_metadata, reject_dst_dir = dst_entries
			parent_dir = dst_parent

		src_only_dirs: set[_Dir] = set()
		dst_only_dirs: set[_Dir] = set()
		src_only_files: set[_File] = set()
		dst_only_files: set[_File] = set()
		dir_matches: dict[_Dir, _Dir] = {}
		file_matches: dict[_File, _File] = {}
		ignored_src_entries = set()
		ignored_dst_entries = set()

		src_root_name = self.src.name + self.out_sep
		dst_root_name = self.dst.name + self.out_sep

		if comp == -1:
			for s in src_dirs:
				src_only_dirs.add(s)
			for f in src_files:
				src_only_files.add(f)

		elif comp == 0:
			# strong match = file names match exactly
			# weak match = file names match after normalizing case
			# a dir/file can have at most one strong match, and it will always be chosen with priority
			# a dir/file can have multiple weak matches
			# a weak match will be be chosen if there is only one and there is no strong match
			# if there is no chosen match, then the file is ignored, along with all weak matches to it

			existing_dst: dict[_Relpath, list[_Relpath]] = {}
			new_dst: dict[_Relpath, list[_Relpath]] = {}

			for d in dst_dirs:
				existing_dst[d] = []
			for f in dst_files:
				existing_dst[f] = []

			for d in src_dirs:
				try:
					existing_dst[d].append(d)
				except KeyError:
					try:
						new_dst[d].append(d)
					except KeyError:
						new_dst[d] = [d]
			for f in src_files:
				try:
					existing_dst[f].append(f)
				except KeyError:
					try:
						new_dst[f].append(f)
					except KeyError:
						new_dst[f] = [f]

			for dst_entry, matches in existing_dst.items():
				type_entry = type(dst_entry)
				strong_match = None
				weak_match = None
				reject_dst = False
				reject_src = set()
				for match in matches:
					if type_entry != type(match) and not self.force:
						reject_dst = True
						break
					elif dst_entry.name == match.name:
						strong_match = match
						if weak_match:
							reject_src.add(weak_match)
					elif dst_entry.norm[-1] == match.norm[-1]:
						if strong_match:
							reject_src.add(match)
						if weak_match is None:
							weak_match = match
						elif weak_match:
							reject_src.add(match)
							reject_src.add(weak_match)
							weak_match = False
						else:
							reject_src.add(match)

				if weak_match is False and strong_match is None:
					reject_dst = True
				if reject_dst:
					reject_src = matches
				for s in reject_src:
					if type(s) == _Dir:
						reject_src_dir.add(s._index) # stops recursion into this dir
						self.logger.warning(f"Match conflict: {src_root_name}{s}{self.out_sep}")
					else:
						self.logger.warning(f"Match conflict: {src_root_name}{s}")
					ignored_src_entries.add(id(s)) # stops this from being a rename candidate
				if reject_dst:
					if type(dst_entry) == _Dir:
						reject_dst_dir.add(dst_entry._index)
						self.logger.warning(f"Match conflict: {dst_root_name}{dst_entry}{self.out_sep}")
					else:
						self.logger.warning(f"Match conflict: {dst_root_name}{dst_entry}")
					ignored_dst_entries.add(id(dst_entry))

				final_match = strong_match or weak_match
				if final_match:
					if type(final_match) == _Dir and type(dst_entry) == _Dir:
						dir_matches[final_match] = dst_entry
					elif type(final_match) == _File and type(dst_entry) == _File:
						file_matches[final_match] = dst_entry
					elif type(final_match) == _File and type(dst_entry) == _Dir:
						src_only_files.add(final_match)
						dst_only_dirs.add(dst_entry)
					else:
						src_only_dirs.add(final_match)
						dst_only_files.add(dst_entry)

				elif not reject_dst:
					if type_entry == _Dir:
						dst_only_dirs.add(dst_entry)
					else:
						dst_only_files.add(dst_entry)

			for dst_entry, matches in new_dst.items():
				if len(matches) > 1:
					for match in matches:
						if type(match) == _Dir:
							reject_src_dir.add(match._index)
							self.logger.warning(f"Match conflict: {src_root_name}{match}{self.out_sep}")
						else:
							self.logger.warning(f"Match conflict: {src_root_name}{match}")
						ignored_src_entries.add(id(match))
				else:
					match = matches[0]
					if type(dst_entry) == _Dir:
						src_only_dirs.add(dst_entry)
					else:
						src_only_files.add(dst_entry)

		else:
			# dst dir needs to be deleted
			for d in dst_dirs:
				assert d is not None
				dst_only_dirs.add(d)
			for f in dst_files:
				dst_only_files.add(f)

		# TODO
		'''
		self.logger.debug(f"{len(src_only)=}")
		self.logger.debug(f"{len(dst_only)=}")
		#self.logger.debug(f"{len(both)=}")
		self.logger.debug(f"{len(src_dirs)=}")
		self.logger.debug(f"{len(dst_dirs)=}")
		self.logger.debug(f"{len(src_empty_dirs)=}")
		self.logger.debug(f"{len(dst_empty_dirs)=}")

		self.logger.debug(f"{src_only[:10]=}")
		self.logger.debug(f"{dst_only[:10]=}")
		self.logger.debug(f"{list(islice(both(), 10))=}")
		self.logger.debug(f"{list(islice(src_dirs, 10))=}")
		self.logger.debug(f"{list(islice(dst_dirs, 10))=}")
		self.logger.debug(f"{list(islice(src_empty_dirs, 10))=}")
		self.logger.debug(f"{list(islice(dst_empty_dirs, 10))=}")
		'''

		return (
			parent_dir,
			src_file_metadata,
			dst_file_metadata,
			src_only_dirs,
			dst_only_dirs,
			src_only_files,
			dst_only_files,
			dir_matches,
			file_matches,
			ignored_src_entries,
			ignored_dst_entries,
		)

	def _get_rename_map(self, src_file_metadata, dst_file_metadata, ignored_src_entries, ignored_dst_entries) -> dict[_File, _File]:
		rename_map: dict[_File, _File] = {}
		rename_cycles: list[list[list[_File]]] = []

		src_meta_to_relpath = _reverse_dict({k:v for k,v in src_file_metadata.items() if id(k) not in ignored_src_entries})
		dst_meta_to_relpath = _reverse_dict({k:v for k,v in dst_file_metadata.items() if id(k) not in ignored_dst_entries})

		unique_src = set(k for k,v in src_meta_to_relpath.items() if v is not None and k.size >= self.rename_threshold)
		unique_dst = set(k for k,v in dst_meta_to_relpath.items() if v is not None and k.size >= self.rename_threshold)
		matched_meta = unique_src.intersection(unique_dst)

		for meta in matched_meta:
			src_file = src_meta_to_relpath[meta]
			dst_file = dst_meta_to_relpath[meta]

			if src_file == dst_file:
				continue

			assert src_file is not None
			assert dst_file is not None

			# Ignore if last 1kb do not match
			if not self.metadata_only:
				try:
					if not _last_bytes(self.src / src_file) == _last_bytes(self.dst / dst_file):
						continue
				except OSError as e:
					self.logger.warning(_exc_summary(e))
					continue

			rename_map[dst_file] = src_file
		return rename_map

	def _operations(self) -> Iterator[Operation]:
		src_root_name = self.src.name + self.out_sep if self.trash else ""
		dst_root_name = self.dst.name + self.out_sep if self.trash else ""
		trash_root_name = self.trash.name + self.out_sep if self.trash else ""

		src_iter = self._scandir(self.src)
		if self.dst.exists():
			dst_iter = self._scandir(self.dst)
		else:
			dst_iter = iter([])
			dst_file_metadata: dict[_File, _Metadata] = {}

		def _delete_ops(dst_relpath:_Relpath, dst_file_metadata) -> Iterator[Operation]:
			if isinstance(dst_relpath, _File):
				if self.trash:
					yield TrashFileOperation(
						dst       = dst_relpath,
						summary   = f"T {dst_root_name}{dst_relpath}",
					)
				else:
					yield DeleteFileOperation(
						dst       = dst_relpath,
						byte_diff = -dst_file_metadata[dst_relpath].size,
						summary   = f"- {dst_root_name}{dst_relpath}",
					)
			else:
				assert isinstance(dst_relpath, _Dir)
				if self.trash:
					yield TrashDirOperation(
						dst     = dst_relpath,
						summary = f"T {dst_root_name}{dst_relpath}{self.out_sep}",
					)
				else:
					yield DeleteDirOperation(
						dst     = dst_relpath,
						summary = f"- {dst_root_name}{dst_relpath}{self.out_sep}",
					)

		def _create_ops(dst_relpath:_Relpath, src_file_metadata) -> Iterator[Operation]:
			if isinstance(dst_relpath, _File):
				if not self.follow_symlinks and (self.src / dst_relpath).is_symlink():
					yield CreateSymlinkOperation(
						src       = dst_relpath,
						dst       = dst_relpath,
						byte_diff = 0,
						summary   = f"+ {dst_root_name}{dst_relpath}",
					)
				else:
					yield CreateFileOperation(
						src       = dst_relpath,
						dst       = dst_relpath,
						byte_diff = src_file_metadata[dst_relpath].size,
						summary   = f"+ {dst_root_name}{dst_relpath}",
					)
			else:
				assert isinstance(dst_relpath, _Dir)
				yield CreateDirOperation(
					dst     = dst_relpath,
					summary = f"+ {dst_root_name}{dst_relpath}{self.out_sep}",
				)

		def _update_ops(src_relpath:_Relpath, dst_relpath:_Relpath, src_file_metadata, dst_file_metadata) -> Iterator[Operation]:
			src_time = src_file_metadata[src_relpath].mtime
			dst_time = dst_file_metadata[dst_relpath].mtime
			byte_diff = src_file_metadata[src_relpath].size - dst_file_metadata[dst_relpath].size

			do_update = False

			if src_time > dst_time:
				do_update = True
			elif src_time < dst_time:
				if self.force:
					do_update = True
				else:
					self.logger.warning(f"'dst' file is newer than 'src' file: {dst_root_name}{dst_relpath}")

			if do_update:
				yield UpdateFileOperation(
					src       = src_relpath,
					dst       = dst_relpath,
					byte_diff = byte_diff,
					summary   = f"U {dst_root_name}{dst_relpath}",
				)
				if src_relpath != dst_relpath:
					yield RenameFileOperation(
						src       = src_relpath,
						dst       = dst_relpath,
						summary   = f"R {src_root_name}{src_relpath} -> {dst_root_name}{dst_relpath}",
					)

		def _rename_ops(rename_map, src_only_files, file_matches, dst_only_files, dst_only_dirs) -> Iterator[Operation]:
			rename_cycles = []
			handled_renames = set()
			for rename_from in rename_map:
				if rename_from in handled_renames:
					continue
				cycle: list[list[_File]] = []
				first = rename_from

				# get chain/cycle if it exists
				while True:
					handled_renames.add(rename_from)
					if rename_from not in rename_map:
						cycle = []
						break

					rename_to = rename_map[rename_from]
					cycle.append([rename_from, rename_to])

					if rename_to in src_only_files:
						cycle = list(reversed(cycle))
						break
					elif rename_to == first or (self.global_renames and rename_to in dst_only_dirs):
						cycle[-1][1] = rename_to + ".tempcopy"
						cycle = list(reversed(cycle))
						cycle.append([rename_to + ".tempcopy", rename_to])
						break

					rename_from = rename_to

				# remove renamed files from other collections
				if cycle:
					rename_cycles.append(cycle)
					#for cycle in rename_cycles:
					for step in cycle:
						rename_from, rename_to = step[0], step[1]
						try:
							src_only_files.remove(rename_to)
						except KeyError:
							pass
						try:
							del file_matches[rename_from]
						except KeyError:
							pass
						try:
							dst_only_files.remove(rename_from)
						except KeyError:
							pass

			for cycle in rename_cycles:
				for step in cycle:
					rename_from, rename_to = step[0], step[1]
					yield RenameFileOperation(
						dst       = rename_from,
						target    = rename_to,
						summary   = f"R {dst_root_name}{rename_from} -> {dst_root_name}{rename_to}",
					)

		if self.global_renames:

			deletes: list[Operation] = []
			creates: list[Operation] = []
			updates: list[Operation] = []
			renames: list[Operation] = []

			all_src_file_metadata = {}
			all_dst_file_metadata = {}
			all_src_only_dirs = set()
			all_dst_only_dirs = set()
			all_src_only_files = set()
			all_dst_only_files = set()
			all_dir_matches = {}
			all_file_matches = {}
			all_ignored_src_entries = set()
			all_ignored_dst_entries = set()

			for comp, src_entries, dst_entries in _merge_iters(src_iter, dst_iter, key=lambda x: x[0]):
				(
					parent_dir,
					src_file_metadata,
					dst_file_metadata,
					src_only_dirs,
					dst_only_dirs,
					src_only_files,
					dst_only_files,
					dir_matches,
					file_matches,
					ignored_src_entries,
					ignored_dst_entries,
				) = self._dir_diff(comp, src_entries, dst_entries)

				all_src_file_metadata.update(src_file_metadata)
				all_dst_file_metadata.update(dst_file_metadata)
				all_src_only_dirs.update(src_only_dirs)
				all_dst_only_dirs.update(dst_only_dirs)
				all_src_only_files.update(src_only_files)
				all_dst_only_files.update(dst_only_files)
				all_dir_matches.update(dir_matches)
				all_file_matches.update(file_matches)

			rename_map = self._get_rename_map(all_src_file_metadata, all_dst_file_metadata, all_ignored_src_entries, all_ignored_dst_entries)

			# renames
			# TODO currently this is just used to remove entries from other collections
			renames.extend(_rename_ops(rename_map, all_src_only_files, all_file_matches, all_dst_only_files, all_dst_only_dirs))

			'''
			print(f"{all_src_file_metadata=}")
			print(f"{all_dst_file_metadata=}")
			print(f"{all_src_only_dirs=}")
			print(f"{all_dst_only_dirs=}")
			print(f"{all_src_only_files=}")
			print(f"{all_dst_only_files=}")
			print(f"{all_dir_matches=}")
			print(f"{all_file_matches=}")
			'''

			# deletes
			if self.delete_files or self.trash:
				for d in all_dst_only_dirs:
					deletes.extend(_delete_ops(d, None))
				for f in all_dst_only_files:
					deletes.extend(_delete_ops(f, all_dst_file_metadata))

			# updates
			for s, d in all_file_matches.items():
				updates.extend(_update_ops(s, d, all_src_file_metadata, all_dst_file_metadata))

			# creates
			if not self.no_create:
				for s in all_src_only_dirs:
					creates.extend(_create_ops(s, None))
				for s in all_src_only_files:
					creates.extend(_create_ops(s, all_src_file_metadata))

			# TODO combine rename ops into rename dir ops

			# TODO combine delete/trash ops into trash/delete dir ops
			# dir_size would be needed for this

			def deepest_first(x, y):
				if x == y:
					return 0
				elif x.is_relative_to(y) or x < y:
					return -1
				else:
					return 1

			deletes.sort()
			updates.sort()
			creates.sort()

			delete_keys = list(op.dst for op in deletes)
			rename_keys = sorted((entry for entry in rename_map), key=cmp_to_key(deepest_first))

			delete_index = 0
			rename_index = 0
			while delete_index < len(delete_keys) or rename_index < len(rename_keys):
				try:
					d = delete_keys[delete_index]
				except IndexError:
					d = None
				try:
					r = rename_keys[rename_index]
				except IndexError:
					r = None

				yield_d = False
				yield_r = False
				if d is None:
					yield_r = True
				elif r is None:
					yield_d = True
				elif r.is_relative_to(d) or r < d:
					yield_r = True
				else:
					yield_d = True

				if yield_d:
					yield deletes[delete_index]
					delete_index += 1

				if yield_r:
					rename_to = rename_map[r]
					if rename_to.is_relative_to(r) or any(rename_to.is_relative_to(d2) for d2 in delete_keys[delete_index:]): # TODO use priority queue
						old_rename_to = rename_to
						rename_to = type(rename_to)(str(uuid.uuid4()).replace("-","") + ".tempcopy", rename_to.in_sep, rename_to.out_sys)
						rename_map[rename_to] = old_rename_to
						rename_keys.append(rename_to) # TODO use priority queue
					elif rename_to in rename_map:
						old_rename_to = rename_to
						rename_to += ".tempcopy"
						rename_map[rename_to] = old_rename_to
						rename_keys.append(rename_to)
					yield RenameFileOperation(
						dst     = r,
						target  = rename_to,
						summary = f"R {dst_root_name}{r} -> {dst_root_name}{rename_to}",
					)
					rename_index += 1

			yield from updates
			yield from creates

		else:

			deleting_dirs_under = None

			# yield after exiting this directory
			# delete ops always need to be yielded after this dir
			# other ops only need to be yielded after this dir if there are delete ops
			delete_after: list[Operation] = []
			update_after: list[Operation] = []
			create_after: list[Operation] = []
			rename_after: list[Operation] = []

			for comp, src_entries, dst_entries in _merge_iters(src_iter, dst_iter, key=lambda x: x[0]):
				(
					parent_dir,
					src_file_metadata,
					dst_file_metadata,
					src_only_dirs,
					dst_only_dirs,
					src_only_files,
					dst_only_files,
					dir_matches,
					file_matches,
					ignored_src_entries,
					ignored_dst_entries,
				) = self._dir_diff(comp, src_entries, dst_entries)

				rename_map = self._get_rename_map(src_file_metadata, dst_file_metadata, ignored_src_entries, ignored_dst_entries)

				delete_now: list[Operation] = []
				update_now: list[Operation] = []
				create_now: list[Operation] = []
				rename_now: list[Operation] = []

				if deleting_dirs_under and not parent_dir.is_relative_to(deleting_dirs_under):
					yield from sorted(delete_after) # reversed(delete_after) should also work
					yield from sorted(update_after)
					yield from sorted(create_after)
					yield from rename_after
					delete_after = []
					update_after = []
					create_after = []
					rename_after = []
					deleting_dirs_under = None

				if dst_only_dirs and deleting_dirs_under is None:
				#if deleting_dirs_under is None and (any(f in dst_only_dirs for f in src_only_files) or any(d in dst_only_dirs for d in src_only_dirs)):
					deleting_dirs_under = parent_dir

				if deleting_dirs_under:
					deletes = delete_after
					updates = update_after
					creates = create_after
					renames = rename_after
				else:
					deletes = delete_now
					updates = update_now
					creates = create_now
					renames = rename_now

				# renames
				renames.extend(_rename_ops(rename_map, src_only_files, file_matches, dst_only_files, dst_only_dirs))

				# deletes
				if self.delete_files or self.trash:
					for d in dst_only_dirs:
						deletes.extend(_delete_ops(d, None))
					for f in dst_only_files:
						deletes.extend(_delete_ops(f, dst_file_metadata))

				# updates
				for s, d in file_matches.items():
					updates.extend(_update_ops(s, d, src_file_metadata, dst_file_metadata))

				# creates
				if not self.no_create:
					for s in src_only_dirs:
						creates.extend(_create_ops(s, None))
					for s in src_only_files:
						creates.extend(_create_ops(s, src_file_metadata))

				# TODO combine delete/trash ops into trash/delete dir ops
				# dir_size would be needed for this

				yield from sorted(delete_now)
				yield from sorted(update_now)
				yield from sorted(create_now)
				yield from rename_now

			yield from sorted(delete_after) # reversed(delete_after) should also work
			yield from sorted(update_after)
			yield from sorted(create_after)
			yield from rename_after

	def run(self) -> Results:
		'''Runs the sync operation. `run()` does not raise errors. If an error occurs, it will be available in the returned `Results` object.'''

		if self._state != Sync._SyncState.READY:
			raise StateError("Sync object state is not READY.")

		try:
			HEADER  = _RecordTag.HEADER.dict()
			FOOTER  = _RecordTag.FOOTER.dict()
			SYNC_OP = _RecordTag.SYNC_OP.dict()

			self.logger.debug(f"Starting backup: {self.src=}, {self.dst=}, {self.filter=}, {self.trash=}, {self.delete_files=}, {self.no_create=}, {self.force=}, {self.global_renames=}, {self.metadata_only=}, {self.rename_threshold=}, {self.translate_symlinks=}, {self.ignore_symlinks=}, {self.follow_symlinks=}, {self.dry_run=}, {self.log_file=}, {self.log_level=}, {self.print_level=}, {self.no_header=}, {self.no_footer=}, {self.sftp_compat=}".replace("self.", ""))
			self.logger.debug("")

			width = max(len(str(self.src)), len(str(self.dst)), 7) + 3
			self.logger.info("   " + str(self.src), extra=HEADER)
			self.logger.info("-> " + str(self.dst), extra=HEADER)
			self.logger.info("-" * width, extra=HEADER)

			for op in self._operations():
				if any(op.depends_on(failed_op) for failed_op, _ in self.results.sync_errors):
					self.logger.debug(f"Dependent failure: {op.summary}")
					continue

				self.logger.info(op.summary, extra=SYNC_OP)

				if not self.dry_run:
					try:
						op.perform(self)
						self.results.tally_success(op)
					except OSError as e:
						self.logger.error(_exc_summary(e)) # TODO include_root = bool(self.trash)
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
			if Sync._RAISE_UNKNOWN_ERRORS:
				raise e
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
			raise FileExistsError(17, f"Cannot copy {src}, dst exists", str(dst))
		elif not dst.is_file():
			raise FileExistsError(17, f"Cannot copy {src}, dst is not a file", str(dst))

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

def _create_symlink(dst:PathType, *, target:str, st, exist_ok:bool = True) -> None:
	if dst.exists():
		if not exist_ok:
			raise FileExistsError(17, "Cannot create symlink, dst exists", str(dst))
		elif not dst.is_symlink():
			raise FileExistsError(17, "Cannot create symlink, dst is not a symlink", str(dst))

	dst_tmp = dst.with_name(dst.name + ".tempcopy")
	try:
		# Copy into a temp file, with metadata
		dir = dst.parent
		dir.mkdir(parents=True, exist_ok=True)
		if isinstance(dst_tmp, Path):
			os.symlink(target, dst_tmp, target_is_directory=stat.S_ISDIR(st.st_mode))
		else:
			RemotePath.symlink(target, dst_tmp)
		# replace the temp file
		_replace(dst_tmp, dst)
	finally:
		dst_tmp.unlink(missing_ok=True)

	# update time metadata
	if st.st_atime is not None and st.st_mtime is not None:
			if isinstance(dst, Path):
				try:
					os.utime(str(dst), (st.st_atime, st.st_mtime), follow_symlinks=False)
				except NotImplementedError as e:
					raise UnsupportedOperationError("Could not update time metadata", str(dst)) from e
			else:
				RemotePath._utime(dst, st=st, follow_symlinks=False)
	else:
		raise PermissionError(1, "Could not update time metadata", str(dst))

def _move(src:PathType, dst:PathType, *, exist_ok:bool = False) -> None:
	'''Move file from `src` to `dst`. Existing files will be overwritten if `exist_ok` is `True`. Otherwise this method will raise a `FileExistsError`.'''

	if dst.exists():
		if not exist_ok:
			raise FileExistsError(17, f"Cannot move {src}, dst exists", str(dst))
		elif not dst.is_file():
			raise FileExistsError(17, f"Cannot move {src}, dst is not a file", str(dst))

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
	if isinstance(path, RemotePath):
		return path.relative_to(cast(RemotePath, root))
	else:
		return path.relative_to(cast(Path, root))

def _last_bytes(file:PathType, n:int = 1024) -> bytes:
	'''Reads and returns the last `n` bytes of a file.'''

	file_size = file.stat().st_size
	if file_size is None:
		raise PermissionError(1, "Could not get file size", str(file))
	bytes_to_read = file_size if n > file_size else n
	with file.open("rb") as f:
		f.seek(-bytes_to_read, os.SEEK_END)
		return f.read()

def _convert_sep(path:str, in_sep:str, out_sep:str):
	r'''
	Translates `in_sep` (path separators) in  `path` to `out_sep`.

	>>> _convert_sep("\\a/b", "/", "\\")
	Traceback (most recent call last):
	...
	psync.errors.IncompatiblePathError: [Errno 1] Incompatible path for this system: '\\a/b'
	>>> _convert_sep("a/b", "/", "\\")
	'a\\b'
	>>> _convert_sep("\\a/b", "\\", "/")
	'/a/b'
	>>> _convert_sep("\\a/b", "/", "/")
	'\\a/b'
	>>> _convert_sep("\\a/b", "\\", "\\")
	'\\a\\b'
	'''

	if in_sep == out_sep:
		if in_sep == "/":
			return path
		else:
			return path.replace("/", "\\")
	elif in_sep == "\\":
		return path.replace("\\", "/")
	else:
		if "\\" in path:
			raise IncompatiblePathError("Incompatible path for this system", str(path))
		else:
			return path.replace("/", "\\")
