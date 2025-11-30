# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import sys
import os
import ntpath
import stat
import shutil
from pathlib import Path, PurePath, PureWindowsPath, PurePosixPath
from functools import cached_property
from datetime import datetime
from dataclasses import dataclass
from typing import Iterator

from .config import _SyncConfig
from .dual_walk import _Relpath, _File, _Dir, _Diff, _DualWalk
from .helpers import _convert_sep
from .sftp import RemotePath
from .types import _AbstractPath
from .errors import MetadataUpdateError, BrokenSymlinkError, IncompatiblePathError, NewerInDstError, UnsupportedOperationError
from .log import _exc_summary

def _get_operations(config: _SyncConfig) -> Iterator["Operation"]:
	'''
	Get all the `Operation`s necessary to complete the sync operation.

	Args
		config (_SyncConfig): Sync settings object

	Returns
		an Iterator of `Operation`s.
	'''

	# In general, operations are yielded in a particular order: renames, deletes, updates, and creates.

	factory = _OperationFactory(config)

	a: _Relpath
	b: _Relpath

	if config.global_renames:
		# The entire directory tree needs to be read in to find global renames (ie, directory renames or file renames that span between two different directories).

		deletes: list["Operation"] = []
		creates: list["Operation"] = []
		updates: list["Operation"] = []
		renames: list["Operation"] = []

		necessary_dirs = []

		diff = _Diff(config)
		dw   = _DualWalk(config, get_dir_hashes=True) # bottom_up_lone_dst=False,
		for dif in dw:
			diff.update(dif)
			if len(dif.src_only_files) > 0 and dif.dst_parent is None:
				necessary_dirs.append(dif.src_parent)

		# renames
		if config.renames:
			for rename_from, rename_to in diff.get_rename_pairs(dw.src_dir_hash, dw.dst_dir_hash):
				renames.extend(factory.get_rename_ops(rename_from, rename_to))

		# deletes
		if config.delete_files:
			for d in diff.dst_only_dirs:
				deletes.extend(factory.get_delete_ops(d, diff))
		#elif config.delete_empty_dirs:
		#	# TODO not implemented
		#	# In addition to dst_only_dirs, need to remove matched dirs that have no file descendants and did not receive any new file descendants
		#	dst_empty_dirs = ...
		#	for d in dst_empty_dirs:
		#		deletes.extend(factory.get_delete_ops(d, diff))
		if config.delete_files:
			for f in diff.dst_only_files:
				deletes.extend(factory.get_delete_ops(f, diff))

		# updates
		#for a, b in diff.dir_matches.items():
		#	updates.extend(factory.get_update_ops(a, b, diff)) # updating dirs currently does nothing
		for a, b in diff.file_matches.items():
			updates.extend(factory.get_update_ops(a, b, diff))

		# creates
		if config.create_dir_tree:
			for d in diff.src_only_dirs:
				creates.extend(factory.get_create_ops(d, diff))
		elif config.create_files:
			for d in necessary_dirs:
				creates.extend(factory.get_create_ops(d, diff))
		if config.create_files:
			for f in diff.src_only_files:
				creates.extend(factory.get_create_ops(f, diff))

		# renames are already sorted
		deletes.sort()
		updates.sort()
		creates.sort()

		yield from renames
		yield from deletes
		yield from updates
		yield from creates

	else:
		# No need to read in the whole directory tree when global_renames is False. Instead, a folder-by-folder apprach is faster.

		dw = _DualWalk(config)
		for diff in dw:

			# renames
			if config.renames:
				for rename_from, rename_to in diff.get_rename_pairs():
					yield from factory.get_rename_ops(rename_from, rename_to)

			# deletes
			if config.delete_files:
				for f in diff.dst_only_files:
					yield from factory.get_delete_ops(f, diff)
				if diff.src_parent is None:
					yield from factory.get_delete_ops(diff.dst_parent, diff)

			# updates
			#for a, b in diff.dir_matches.items():
			#	updates.extend(factory.get_update_ops(a, b, diff)) # updating dirs currently does nothing
			for a, b in diff.file_matches.items():
				yield from factory.get_update_ops(a, b, diff)

			# creates
			if config.create_dir_tree:
				# create all matched directories, even if they are empty or contain no files
				if diff.dst_parent is None:
					yield from factory.get_create_ops(diff.src_parent, diff)
			elif config.create_files and len(diff.src_only_files) > 0:
				# create directories only when they are needed to hold file children
				if diff.dst_parent is None:
					yield from factory.get_create_ops(diff.src_parent, diff)
			if config.create_files:
				for f in diff.src_only_files:
					yield from factory.get_create_ops(f, diff)

		# TODO not implemented
		#if config.delete_empty_dirs:
		#	dirs_to_delete = ...
		#	delete_ops = []
		#	for d in dirs_to_delete:
		#		delete_ops.extend(factory.get_delete_ops(d, None))
		#	yield from sorted(delete_ops)

@dataclass(frozen=True)
class Operation:
	'''Filesystem operation yielded by `_operations()`.'''

	config    : _SyncConfig
	dst       : _Relpath
	src       : _Relpath | None = None
	target    : _Relpath | None = None
	byte_diff : int = 0

	def perform(self):
		'''Perform the filesystem operation associated with this object.'''

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
		return self.dst.norm < other.dst.norm

	@property
	def summary(self):
		'''A string summary that will be logged when the `Operation` is performed.'''

		raise NotImplementedError()

	def __str__(self):
		return self.summary

@dataclass(frozen=True)
class RenameFileOperation(Operation):
	def __post_init__(self):
		assert self.src is None
		assert self.target is not None

	def perform(self):
		assert self.target is not None
		exist_ok = self.dst.norm == self.target.norm
		_move(self.config.dst / self.dst, self.config.dst / self.target, exist_ok=exist_ok)

	@property
	def summary(self):
		return f"R {self.config.dst_name}{self.dst} -> {self.config.dst_name}{self.target}"

@dataclass(frozen=True)
class RenameDirOperation(Operation):
	def __post_init__(self):
		assert self.src is None
		assert self.target is not None

	def perform(self):
		assert self.target is not None
		exist_ok = self.dst.norm == self.target.norm
		_move(self.config.dst / self.dst, self.config.dst / self.target, exist_ok=exist_ok) # TODO check this

	@property
	def summary(self):
		return f"R {self.config.dst_name}{self.dst}{self.config.dst_sep} -> {self.config.dst_name}{self.target}{self.config.dst_sep}"

@dataclass(frozen=True)
class DeleteFileOperation(Operation):
	def __post_init__(self):
		assert self.src is None
		assert self.target is None

	def __lt__(self, other):
		if other.dst.relpath == ".":
			return True
		if all(a==b for a,b in zip(self.dst.norm, other.dst.norm)):
			return len(self.dst.norm) >= len(other.dst.norm)
		else:
			return self.dst.norm < other.dst.norm

	def perform(self):
		(self.config.dst / self.dst).unlink()

	@property
	def summary(self):
		return f"- {self.config.dst_name}{self.dst}"

@dataclass(frozen=True)
class DeleteDirOperation(Operation): # Empty dirs only
	def __post_init__(self):
		assert self.src is None
		assert self.target is None

	def __lt__(self, other):
		if other.dst.relpath == ".":
			return True
		if all(a==b for a,b in zip(self.dst.norm, other.dst.norm)):
			return len(self.dst.norm) >= len(other.dst.norm)
		else:
			return self.dst.norm < other.dst.norm

	def perform(self):
		(self.config.dst / self.dst).rmdir()

	@property
	def summary(self):
		return f"- {self.config.dst_name}{self.dst}{self.config.dst_sep}"

@dataclass(frozen=True)
class TrashFileOperation(Operation):
	def __post_init__(self):
		assert self.src is None
		assert self.target is None

	def __lt__(self, other):
		if other.dst.relpath == ".":
			return True
		if all(a==b for a,b in zip(self.dst.norm, other.dst.norm)):
			return len(self.dst.norm) >= len(other.dst.norm)
		else:
			return self.dst.norm < other.dst.norm

	def perform(self):
		assert self.target is None
		assert self.config.trash is not None
		_move(self.config.dst / self.dst, self.config.trash / self.dst)

	@property
	def summary(self):
		return f"T {self.config.dst_name}{self.dst}"

@dataclass(frozen=True)
class TrashDirOperation(Operation): # Empty dirs only
	def __post_init__(self):
		assert self.src is None
		assert self.target is None

	def __lt__(self, other):
		if other.dst.relpath == ".":
			return True
		if all(a==b for a,b in zip(self.dst.norm, other.dst.norm)):
			return len(self.dst.norm) >= len(other.dst.norm)
		else:
			return self.dst.norm < other.dst.norm

	def perform(self):
		assert self.config.trash is not None
		(self.config.trash / self.dst).mkdir(exist_ok=True, parents=True)
		(self.config.dst / self.dst).rmdir()

	@property
	def summary(self):
		return f"T {self.config.dst_name}{self.dst}{self.config.dst_sep}"

@dataclass(frozen=True)
class UpdateFileOperation(Operation):
	def __post_init__(self):
		assert self.src is not None
		assert self.target is None

	def perform(self):
		assert self.src is not None
		_copy(self.config.src / self.src, self.config.dst / self.dst, follow_symlinks=self.config.follow_symlinks)

	@property
	def summary(self):
		return f"U {self.config.dst_name}{self.dst}"

@dataclass(frozen=True)
class CreateFileOperation(Operation):
	def __post_init__(self):
		assert self.src is not None
		assert self.target is None

	def perform(self):
		assert self.src is not None
		_copy(self.config.src / self.src, self.config.dst / self.dst, follow_symlinks=self.config.follow_symlinks)

	@property
	def summary(self):
		return f"+ {self.config.dst_name}{self.dst}"

@dataclass(frozen=True)
class CreateSymlinkOperation(Operation):
	def __post_init__(self):
		assert self.src is not None # symlink file
		assert self.target is None

		object.__setattr__(self, "target", self.get_target)

	def perform(self):
		assert self.src is not None

		src = self.config.src / self.src
		dst = self.config.dst / self.dst

		try:
			_create_symlink(dst, target=self.target, st=src.stat(follow_symlinks=False))
		except UnsupportedOperationError as e:
			self.config.logger.warning(_exc_summary(e))


	@cached_property
	def get_target(self) -> str:
		assert self.src is not None

		src = self.config.src / self.src
		dst = self.config.dst / self.dst

		target: str|None
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

		if self.config.translate_symlinks:
			src_path    : PurePath
			dst_path    : PurePath
			target_path : PurePath

			# convert target, src_path to self.config.src_sep
			if self.config.src_sys == "nt":
				target_path = PureWindowsPath(target)
				if self.config.src_sep == "\\":
					src_path = PureWindowsPath(_convert_sep(str(self.config.src), "\\", "\\"))
				else:
					src_path = PureWindowsPath(_convert_sep(str(self.config.src), "/", "\\"))
			else:
				target_path = PurePosixPath(target)
				if self.config.src_sep == "\\":
					src_path = PurePosixPath(_convert_sep(str(self.config.src), "\\", "/"))
				else:
					src_path = PurePosixPath(_convert_sep(str(self.config.src), "/", "/"))

			# convert dst_path to self.config.dst_sep
			if self.config.dst_sys == "nt":
				if self.config.dst_sep == "\\":
					dst_path = PureWindowsPath(_convert_sep(str(self.config.dst), "\\", "\\"))
				else:
					dst_path = PureWindowsPath(_convert_sep(str(self.config.dst), "/", "\\"))
			else:
				if self.config.dst_sep == "\\":
					dst_path = PurePosixPath(_convert_sep(str(self.config.dst), "\\", "/"))
				else:
					dst_path = PurePosixPath(_convert_sep(str(self.config.dst), "/", "/"))

			if self.config.src_sys == "posix" and self.config.dst_sys == "nt":
				if ntpath.isreserved(target_path):
					raise IncompatiblePathError("Incompatible path for dst system", str(target_path))

			if target_path.is_relative_to(src_path):
				# translate absolute paths
				rel_target = target_path.relative_to(src_path)
				rel_target = _convert_sep(str(rel_target), self.config.src_sep, self.config.dst_sep)
				target_path = dst_path / rel_target
				target = str(target_path)
			else:
				# translate relative paths
				target = _convert_sep(str(target_path), self.config.src_sep, self.config.dst_sep)

		return target

	@property
	def summary(self):
		return f"L {self.config.dst_name}{self.dst} -> {self.target}"

@dataclass(frozen=True)
class CreateDirOperation(Operation): # Empty dirs only
	def __post_init__(self):
		assert self.src is None
		assert self.target is None

	def perform(self):
		(self.config.dst / self.dst).mkdir(exist_ok=True, parents=True)

	@property
	def summary(self):
		return f"+ {self.config.dst_name}{self.dst}{self.config.dst_sep}"

class _OperationFactory:
	'''Determines which `Operation`s to yield, in accordance `_SyncConfig` settings and the type of the `_Relpath`s given as arguments.'''

	def __init__(self, config: _SyncConfig):
		self.config = config

	def get_rename_ops(self, dst_relpath:_Relpath, target_relpath:_Relpath) -> Iterator[Operation]:
		if isinstance(dst_relpath, _File):
			yield RenameFileOperation(
				config  = self.config,
				dst     = dst_relpath,
				target  = target_relpath,
			)
		else:
			assert isinstance(dst_relpath, _Dir)
			yield RenameDirOperation(
				config  = self.config,
				dst     = dst_relpath,
				target  = target_relpath,
			)

	def get_delete_ops(self, dst_relpath:_Relpath, diff:_Diff) -> Iterator[Operation]:
		if isinstance(dst_relpath, _File):
			if self.config.trash:
				yield TrashFileOperation(
					config    = self.config,
					dst       = dst_relpath,
				)
			else:
				yield DeleteFileOperation(
					config    = self.config,
					dst       = dst_relpath,
					byte_diff = -diff.dst_file_metadata[dst_relpath].size,
				)
		else:
			assert isinstance(dst_relpath, _Dir)
			if self.config.trash:
				yield TrashDirOperation(
					config  = self.config,
					dst     = dst_relpath,
				)
			else:
				yield DeleteDirOperation(
					config  = self.config,
					dst     = dst_relpath,
				)

	def get_update_ops(self, src_relpath:_Relpath, dst_relpath:_Relpath, diff:_Diff) -> Iterator[Operation]:
		if isinstance(dst_relpath, _File):
			src_time = diff.src_file_metadata[src_relpath].mtime
			dst_time = diff.dst_file_metadata[dst_relpath].mtime
			byte_diff = diff.src_file_metadata[src_relpath].size - diff.dst_file_metadata[dst_relpath].size

			do_update = False

			if src_time > dst_time:
				do_update = True
			elif src_time < dst_time:
				if self.config.force_update:
					do_update = True
				else:
					self.config.logger.warning(f"'dst' file is newer than 'src' file: {self.config.dst_name}{dst_relpath}")
					# TODO? raise NewerInDstError

			if do_update:
				yield UpdateFileOperation(
					config    = self.config,
					src       = src_relpath,
					dst       = dst_relpath,
					byte_diff = byte_diff,
				)
				# TODO updates.sort() may swap the order of these 2 ops, let UpdateFileOp rename
				if src_relpath.name != dst_relpath.name:
					yield RenameFileOperation(
						config  = self.config,
						dst     = dst_relpath,
						target  = src_relpath,
					)
		else:
			# TODO need to update child relpaths
			pass
			#if src_relpath.name != dst_relpath.name:
			#	yield RenameDirOperation(
			#		config  = self.config,
			#		dst     = dst_relpath,
			#		target  = src_relpath,
			#	)

	def get_create_ops(self, dst_relpath:_Relpath, diff:_Diff) -> Iterator[Operation]:
		if isinstance(dst_relpath, _File):
			if not self.config.follow_symlinks and (self.config.src / dst_relpath).is_symlink():
				yield CreateSymlinkOperation(
					config    = self.config,
					src       = dst_relpath,
					dst       = dst_relpath,
					byte_diff = 0,
				)
			else:
				yield CreateFileOperation(
					config    = self.config,
					src       = dst_relpath,
					dst       = dst_relpath,
					byte_diff = diff.src_file_metadata[dst_relpath].size,
				)
		else:
			assert isinstance(dst_relpath, _Dir)
			yield CreateDirOperation(
				config  = self.config,
				dst     = dst_relpath,
			)

def _copy(src:_AbstractPath, dst:_AbstractPath, *, exist_ok:bool = True, follow_symlinks:bool = False) -> None:
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

def _move(src:_AbstractPath, dst:_AbstractPath, *, exist_ok:bool = False) -> None:
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

def _create_symlink(dst:_AbstractPath, *, target:str, st, exist_ok:bool = True) -> None:
	'''Create a symlink pointing to `target`. Modification time is retrieved from the stat object `st`.'''

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
			assert isinstance(dst_tmp, RemotePath)
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
				raise UnsupportedOperationError(f"Cannot update symlink mtime on Windows.") from e
			except Exception as e:
				raise MetadataUpdateError(f"Could not update time metadata", str(dst)) from e
		else:
			assert isinstance(dst, RemotePath)
			RemotePath._utime(dst, st=st, follow_symlinks=False)
	else:
		raise MetadataUpdateError(f"Could not update time metadata", str(dst))

def _replace(src:_AbstractPath, dst:_AbstractPath) -> None:
	'''Move file from `src` to `dst`. Existing files will be overwritten..'''

	try:
		# Replace the dst file with the tmp file
		src.replace(dst)
	except PermissionError as e:
		# Remove read-only flag and try again
		if not src.is_file():
			raise e
		make_readonly = False
		try:
			dst_stat = dst.stat()
			if dst_stat.st_mode is None or not (dst_stat.st_mode & stat.S_IREAD):
				raise e
			dst.chmod(stat.S_IWRITE, follow_symlinks=False)
			make_readonly = True
			src.replace(dst)
		finally:
			if make_readonly:
				dst.chmod(stat.S_IREAD, follow_symlinks=False)
