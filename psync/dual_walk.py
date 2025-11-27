# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import os
import ntpath
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Iterator, cast, ContextManager, TypeVar
from collections import namedtuple

from .config import _SyncConfig
from .types import _AbstractPath
from .sftp import RemotePath, _RemotePathScanner
from .errors import IncompatiblePathError
from .helpers import _merge_iters, _reverse_dict
from .log import _exc_summary

P = TypeVar("P", bound=_AbstractPath) # for dir_list

@dataclass(frozen=True)
class _Metadata:
	'''File metadata that will be used to find probable duplicates.'''

	size  : int
	mtime : float

@dataclass(frozen=True, repr=False, eq=False, order=False, unsafe_hash=False)
class _Relpath:
	'''Filesystem entries yielded by `_scandir()`.'''

	relpath    : str
	sep        : str
	dst_sys    : str
	norm       : tuple[str] = ("",)
	name       : str = ""
	_real_hash : int = 0
	_norm_hash : int = 0

	def __post_init__(self):
		if self.dst_sys == "nt":
			if (self.sep == "/" and "\\" in self.relpath) or ntpath.isreserved(self.relpath):
				raise IncompatiblePathError("Incompatible path for this system", str(self.relpath))
			parts = self.relpath.split(self.sep)
			object.__setattr__(self, "norm", tuple(p.lower() for p in parts))
			object.__setattr__(self, "name", parts[-1])
		else:
			object.__setattr__(self, "norm", tuple(self.relpath.split(self.sep)))
			object.__setattr__(self, "name", self.norm[-1])

		object.__setattr__(self, "_real_hash", hash(self.relpath))
		object.__setattr__(self, "_norm_hash",  hash(self.norm))

		assert self.name != ""
		assert ".." not in self.norm

	@property
	def normed_name(self):
		return self.norm[-1]

	@property
	def parent(self):
		if len(self.norm) == 1:
			return None
		return _Dir(self.sep.join(self.relpath.split(self.sep)[:-1]), self.sep, self.dst_sys)

	def __eq__(self, other):
		return self.relpath == other.relpath

	# Comparing by relpath would make sense, but _Relpaths are only ever sorted by their norms.
	# So, keep this commented-out so it doesn't get used by mistake.
	#def __lt__(self, other):
	#	return self.relpath < other.relpath

	def __hash__(self):
		return self._real_hash

	def __rtruediv__(self, other:_AbstractPath):
		return other.joinpath(*self.relpath.split(self.sep))

	def __add__(self, other):
		return type(self)(self.relpath + other, self.sep, self.dst_sys)

	def __bool__(self):
		return bool(self.relpath)

	def __contains__(self, val):
		return val in self.relpath

	def __str__(self):
		return self.relpath

	def __repr__(self):
		return self.relpath

	def is_relative_to(self, other):
		if other.relpath == ".":
			return True
		if all(a==b for a,b in zip(self.norm, other.norm)):
			return len(self.norm) >= len(other.norm)
		return False

@dataclass(frozen=True, repr=False, eq=False, order=False, unsafe_hash=False)
class _File(_Relpath):
	pass

@dataclass(frozen=True, repr=False, eq=False, order=False, unsafe_hash=False)
class _Symlink(_File): # TODO use this to replace is_symlink() check inside _operations
	pass

@dataclass(frozen=True, repr=False, eq=False, order=False, unsafe_hash=False)
class _Dir(_Relpath):
	pass

class _Normalized:
	def __init__(self, relpath: _Relpath):
		self._wrapped: _Relpath = relpath

	@property
	def unwrapped(self) -> _Relpath:
		return self._wrapped

	def __hash__(self):
		return self._wrapped._norm_hash

	def __eq__(self, other):
		return self._wrapped.norm == other._wrapped.norm

	def __lt__(self, other:"_Normalized"):
		return self._wrapped.norm < other._wrapped.norm

_DirList = namedtuple("_DirList", [
	"parent_dir",
	"dirs",
	"files",
	"dir_size",
	"file_metadata",
	"nonstandard_files",
])

@dataclass(frozen=True)
class _Diff:
	config              : _SyncConfig
	src_parent          : _Dir|None              = None
	dst_parent          : _Dir|None              = None
	src_file_metadata   : dict[_File, _Metadata] = field(default_factory=dict)
	dst_file_metadata   : dict[_File, _Metadata] = field(default_factory=dict)
	src_only_dirs       : dict[_Dir, None]       = field(default_factory=dict) # used as sorted set, Python 3.7+ required
	dst_only_dirs       : dict[_Dir, None]       = field(default_factory=dict) # used as sorted set
	src_only_files      : dict[_File, None]      = field(default_factory=dict) # used as sorted set
	dst_only_files      : dict[_File, None]      = field(default_factory=dict) # used as sorted set
	dir_matches         : dict[_Dir, _Dir]       = field(default_factory=dict)
	file_matches        : dict[_File, _File]     = field(default_factory=dict)
	ignored_src_entries : set[_Relpath]          = field(default_factory=set)
	ignored_dst_entries : set[_Relpath]          = field(default_factory=set)

	def update(self, other: "_Diff"):
		self.src_file_metadata.update(other.src_file_metadata)
		self.dst_file_metadata.update(other.dst_file_metadata)
		self.src_only_dirs.update(other.src_only_dirs)
		self.dst_only_dirs.update(other.dst_only_dirs)
		self.src_only_files.update(other.src_only_files)
		self.dst_only_files.update(other.dst_only_files)
		self.dir_matches.update(other.dir_matches)
		self.file_matches.update(other.file_matches)
		self.ignored_src_entries.update(other.ignored_src_entries)
		self.ignored_dst_entries.update(other.ignored_dst_entries)

	def get_file_rename_map(self) -> dict[_File, _File]:
		'''Get a file rename map -- a dict with dst files and what they were renamed to in the src root.'''

		rename_map: dict[_File, _File] = {}

		if self.src_file_metadata is None or self.dst_file_metadata is None:
			return rename_map

		src_meta_to_relpath = _reverse_dict({k:v for k,v in self.src_file_metadata.items() if k not in self.ignored_src_entries})
		dst_meta_to_relpath = _reverse_dict({k:v for k,v in self.dst_file_metadata.items() if k not in self.ignored_dst_entries})

		unique_src = set(k for k,v in src_meta_to_relpath.items() if v is not None and k.size >= self.config.rename_threshold)
		unique_dst = set(k for k,v in dst_meta_to_relpath.items() if v is not None and k.size >= self.config.rename_threshold)
		matched_meta = unique_src.intersection(unique_dst)

		for meta in matched_meta:
			src_file = src_meta_to_relpath[meta]
			dst_file = dst_meta_to_relpath[meta]

			if src_file == dst_file:
				continue

			assert src_file is not None
			assert dst_file is not None

			# Ignore if last 1kb do not match.
			if not self.config.metadata_only:
				try:
					if not _last_bytes(self.config.src / src_file) == _last_bytes(self.config.dst / dst_file):
						continue
				except OSError as e:
					self.config.logger.warning(_exc_summary(e))
					continue

			rename_map[dst_file] = src_file

		# return rename_map
		sorted_map = {k:rename_map[k] for k in sorted(rename_map.keys(), key=lambda x: x.norm)} # TODO this won't be needed if the pattern in get_dir_rename_map is copied here
		return sorted_map

	def get_dir_rename_map(self, src_dir_hash: dict[_Dir, int], dst_dir_hash: dict[_Dir, int]) -> dict[_Dir, _Dir]:
		'''Get a directory rename map -- a dict with dst directories and what they were renamed to in the src root.'''

		rename_map: dict[_Dir, _Dir] = {}

		dst_meta_to_relpath = _reverse_dict({k:v for k,v in dst_dir_hash.items() if v is not None})
		src_meta_to_relpath = _reverse_dict({k:v for k,v in src_dir_hash.items() if v is not None})

		# Keys must be in bottom-up order, meaning child dirs comes before parent dirs.
		# Don't edit other collections here, don't know which renames are valid.
		for dst_dir in reversed(dst_dir_hash.keys()):
			hash = dst_dir_hash[dst_dir]
			if dst_meta_to_relpath[hash] is None:
				continue
			try:
				src_dir = src_meta_to_relpath[hash]
			except KeyError:
				continue
			if src_dir is None: # or all(a==b for a,b in zip(dst_dir.norm, src_dir.norm)):
				continue
			rename_map[dst_dir] = src_dir

		return rename_map

	def get_rename_chains(self, src_dir_hash: dict[_Dir, int]|None, dst_dir_hash: dict[_Dir, int]|None):
		'''
		Get chains of file/directories that need to be renamed in order.

		Most renames will be a chain of length 1, but renames that involve swaps or overlapping file names will have longer chain lengths.
		'''

		if dst_dir_hash is None:
			rename_map = self.get_file_rename_map()
		else:
			rename_map = self.get_dir_rename_map(src_dir_hash, dst_dir_hash)
			rename_map.update(self.get_file_rename_map())

		self.config.logger.debug(f"{src_dir_hash=}")
		self.config.logger.debug(f"{dst_dir_hash=}")
		self.config.logger.debug(f"{rename_map=}")

		chains: list[dict[_Relpath, _Relpath]] = [] # valid, top-level rename chains
		removed_by_rename: set[_Relpath] = set() # all entries removed by valid top level renames
		created_by_rename: set[_Relpath] = set() # all entries created by valid top level renames
		failed: set[_Relpath] = set()
		first = None

		# Find valid rename chains. Some may be invalid due to blocking entries.
		for rename_from, rename_to in rename_map.items():
			if rename_from in removed_by_rename or rename_from in failed:
				# Handled in a previous chain.
				continue
			if rename_from.parent in removed_by_rename:
				# Parent dir will be moved, so this dir will go with it.
				removed_by_rename.add(rename_from)
				created_by_rename.add(rename_map[rename_from])
				continue
			if rename_from.parent in failed:
				# Parent dir cannot be moved, so neither can this dir.
				failed.add(rename_from)
				continue

			is_dir = type(rename_from) == _Dir
			chain: dict[_Relpath, _Relpath] = {}
			potential_remove = set()
			potential_create = set()

			# Get chain/cycle if it exists.
			while True:
				if not first:
					first = rename_from
				rename_to = rename_map[rename_from]
				chain[rename_from] = rename_to

				# Don't edit removed_by_rename or created_by_rename unless the chain is verified.
				potential_remove.add(rename_from)
				potential_create.add(rename_to)

				if all(a==b for a,b in zip(rename_from.norm, rename_to.norm)):
					# They are equal or in a direct lineage.
					# If allowed, a temp file may be needed and that's a headache.
					self.config.logger.debug(f"ignorng chain: {chain}")
					failed.update(chain.keys())
					chain = {}
					break
				if not is_dir and any(rename_to.is_relative_to(d) for d in self.dst_only_files):
					# A file is blocking this rename.
					# TODO The rename could go through if the blocking file is to be deleted.
					self.config.logger.debug(f"ignorng chain: {chain}")
					failed.update(chain.keys())
					chain = {}
					break
				if is_dir and any(rename_to.is_relative_to(d) for d in self.dst_only_dirs):
					# A dir is blocking this rename.
					# TODO The rename could go through if the blocking dir is to be deleted.
					self.config.logger.debug(f"ignorng chain: {chain}")
					failed.update(chain.keys())
					chain = {}
					break
				if rename_to not in rename_map and (rename_to in self.dir_matches or rename_to in self.file_matches):
					# A matching file is blocking this rename.
					# The matched entry could be replaced by rename_to, but only if it has an older mtime than rename_to, or --force-update is on.
					# However, this would be difficult to do for directories.
					# So for now, just junk the whole chain, and handle it with create/update/delete.
					self.config.logger.debug(f"ignorng chain: {chain}")
					failed.update(chain.keys())
					chain = {}
					break

				# Find the highest level dir that contains rename_to.
				top_dest_dir = rename_to
				while True:
					parent_dir = top_dest_dir.parent
					if parent_dir in rename_map: # and parent_dir not in removed_by_rename:
						top_dest_dir = parent_dir
					else:
						break

				if is_dir and rename_to in self.src_only_dirs:
					# Final link in the chain creates a new file.
					break
				elif not is_dir and rename_to in self.src_only_files:
					# Final link in the chain creates a new dir.
					break
				elif rename_to.is_relative_to(first):
					# Circular.
					break

				rename_from = top_dest_dir

			if chain:
				chains.append(chain)
				removed_by_rename.update(potential_remove)
				created_by_rename.update(potential_create)
			first = None

		#changed_by_rename = removed_by_rename.intersection(created_by_rename)
		#removed_by_rename -= changed_by_rename
		#created_by_rename -= changed_by_rename
		return chains, removed_by_rename, created_by_rename #, changed_by_rename

	def update_other_sets(self, removed_by_rename: set[_Relpath], created_by_rename: set[_Relpath]):
		'''Remove renamed files from other collections in this `_Diff`.'''

		for rename_from in removed_by_rename:
			is_dir = type(rename_from) == _Dir
			try:
				if is_dir:
					del self.dir_matches[rename_from]
				else:
					del self.file_matches[rename_from]
			except KeyError:
				pass
			try:
				if is_dir:
					del self.dst_only_dirs[rename_from]
				else:
					del self.dst_only_files[rename_from]
			except KeyError:
				pass

		for rename_to in created_by_rename:
			is_dir = type(rename_to) == _Dir
			try:
				if is_dir:
					del self.src_only_dirs[rename_to]
				else:
					del self.src_only_files[rename_to]
			except KeyError:
				pass

	def get_rename_pairs(self, src_dir_hash = None, dst_dir_hash = None):
		'''Convert rename map to a list of 'from', 'to' pairs, adding temp files where needed.'''

		chains, removed_by_rename, created_by_rename = self.get_rename_chains(src_dir_hash, dst_dir_hash)
		self.update_other_sets(removed_by_rename, created_by_rename)

		self.config.logger.debug(f"{chains=}")
		self.config.logger.debug(f"{removed_by_rename=}")
		self.config.logger.debug(f"{created_by_rename=}")

		for chain in chains:
			pairs = list(reversed(chain.items()))
			is_cycle = pairs[0][1].is_relative_to(pairs[-1][0])

			if is_cycle:
				yield pairs[-1][0], pairs[-1][1] + ".tempmove"
				for a, b in pairs[:-1]:
					yield a, b
				yield pairs[-1][1] + ".tempmove", pairs[-1][1]
			else:
				for a,b in pairs:
					yield a, b

class _DualWalk:
	def __init__(self, config: _SyncConfig, *, bottom_up_lone_dst: bool=True, get_dir_hashes: bool=False):
		self.config = config
		self.bottom_up_lone_dst = bottom_up_lone_dst
		self.get_dir_hashes = get_dir_hashes
		self.src_dir_sizes: dict[_Dir, int] = {}
		self.dst_dir_sizes: dict[_Dir, int] = {}
		self.src_dir_hash : dict[_Dir, int] = {}
		self.dst_dir_hash : dict[_Dir, int] = {}
		self._src_ancestors: set[str] = set()
		self._dst_ancestors: set[str] = set()

	def __iter__(self):
		yield from self.dual_walk(self.config.src, self.config.dst)

	def dual_walk(self, src_path: _AbstractPath|None, dst_path: _AbstractPath|None, *, _bottom_up: bool=False) -> Iterator[_Diff]:
		# don't follow circular symlinks
		if self.config.follow_symlinks:
			if src_path is not None:
				src_true_path = str(src_path.resolve())
				if src_true_path in self._src_ancestors:
					self.config.logger.warning(f"Symlink circular reference: {src_path} -> {src_true_path}")
					return
				self._src_ancestors.add(src_true_path)
			if dst_path is not None:
				dst_true_path = str(dst_path.resolve())
				if dst_true_path in self._dst_ancestors:
					self.config.logger.warning(f"Symlink circular reference: {dst_path} -> {dst_true_path}")
					return
				self._dst_ancestors.add(dst_true_path)

		self.config.logger.debug(f"scanning src: {src_path}")
		self.config.logger.debug(f"scanning dst: {dst_path}")

		# if src_path or dst_path is None, an empty dir_list is returned, instead of None or an exception
		# this lets us still find the diff of every dir even when a corresponding dir doesn't exist
		try:
			src_list = self.dir_list(src_path, self.config.src)
		except OSError as e:
			# no read access
			# TODO tally_failure in Results, would need to do so without an Operation to pass
			self.config.logger.error(_exc_summary(e))
			return
		try:
			dst_list = self.dir_list(dst_path, self.config.dst)
		except OSError as e:
			self.config.logger.error(_exc_summary(e))
			return

		src_parent_dir, src_dirs, src_files, src_dir_size, src_file_metadata, src_nonstandard = src_list
		dst_parent_dir, dst_dirs, dst_files, dst_dir_size, dst_file_metadata, dst_nonstandard = dst_list

		if src_parent_dir:
			self.src_dir_sizes[src_parent_dir.unwrapped] = src_dir_size
		if dst_parent_dir:
			self.dst_dir_sizes[dst_parent_dir.unwrapped] = dst_dir_size

		# don't try to match with a nonstandard file (socket, named pipe, etc)
		# the number of nonstandard files should be low, so removal from list structures isn't *that* bad
		for f in src_nonstandard:
			try:
				dst_files.remove(f)
			except ValueError:
				pass
			try:
				dst_dirs.remove(f)
			except ValueError:
				pass
		for f in dst_nonstandard:
			try:
				src_files.remove(f)
			except ValueError:
				pass
			try:
				src_dirs.remove(f)
			except ValueError:
				pass

		diff = self.dir_diff(src_list, dst_list)

		# TODO remove ignored files from metadata dict, don't need to use 'ignored' lists in OperationFactory.get_rename_ops

		if _bottom_up:
			for relpath in diff.dst_only_dirs:
				yield from self.dual_walk(None, self.config.dst/relpath, _bottom_up=_bottom_up)
			for src_relpath, dst_relpath in diff.dir_matches.items():
				yield from self.dual_walk(self.config.src/src_relpath, self.config.dst/dst_relpath, _bottom_up=_bottom_up)
			for relpath in diff.src_only_dirs:
				yield from self.dual_walk(self.config.src/relpath, None, _bottom_up=_bottom_up)
			# TODO yield DualWalkNode object continaing diff, fields for sending data to other nodes, callbacks for received data
			yield diff
		else:
			if self.bottom_up_lone_dst:
				for relpath in diff.dst_only_dirs:
					yield from self.dual_walk(None, self.config.dst/relpath, _bottom_up=True)
				yield diff
			else:
				yield diff
				for relpath in diff.dst_only_dirs:
					yield from self.dual_walk(None, self.config.dst/relpath)
			for src_relpath, dst_relpath in diff.dir_matches.items():
				yield from self.dual_walk(self.config.src/src_relpath, self.config.dst/dst_relpath)
			for relpath in diff.src_only_dirs:
				yield from self.dual_walk(self.config.src/relpath, None)

		# TODO dw_node.data_handler(dw_node.data_from_children)

		if self.get_dir_hashes:
			# get directory hashes, used for finding renamed directories
			# no entry means the dir has unknown hash due to items filtered out
			# TODO look into hash functions that 1. accept iterators, 2. are permutation invariant
			files: list[Any]
			if src_parent_dir is not None:
				if len(src_files) + len(src_dirs) == src_dir_size:
					try:
						dirs = [self.src_dir_hash[d.unwrapped] for d in src_dirs] # raises KeyError
						files = sorted((k.name,v) for k,v in src_file_metadata.items())
						self.src_dir_hash[src_parent_dir.unwrapped] = hash(tuple(files + dirs))
					except KeyError:
						# hash unknown for some subdir, so it's unknown for this dir too
						pass

			if dst_parent_dir is not None:
				if len(dst_files) + len(dst_dirs) == dst_dir_size:
					try:
						dirs = [self.dst_dir_hash[d.unwrapped] for d in dst_dirs] # raises KeyError
						files = sorted((k.name,v) for k,v in dst_file_metadata.items())
						self.dst_dir_hash[dst_parent_dir.unwrapped] = hash(tuple(files + dirs))
					except KeyError:
						# hash unknown for some subdir, so it's unknown for this dir too
						pass

		if self.config.follow_symlinks:
			if src_path is not None:
				self._src_ancestors.remove(src_true_path)
			if dst_path is not None:
				self._dst_ancestors.remove(dst_true_path)

	def dir_list(self, dir: P|None, root: P) -> _DirList:
		'''Returns a tuple of the contents of a directory.'''

		parent_dir       : _Normalized|None = None
		dirs             : list[_Normalized]  = [] # empty data structs are better for updating/extending than None
		files            : list[_Normalized] = []
		dir_size         : int = 0
		file_metadata    : dict[_File, _Metadata] = {}
		nonstandard_files: list[_Normalized] = []

		try:
			if dir is None:
				raise FileNotFoundError()
			scanner: ContextManager
			if isinstance(dir, RemotePath):
				scanner = _RemotePathScanner(dir)
			else:
				# assert isinstance(dir, Path) # want to keep dir as type "P"
				scanner = os.scandir(cast(Path, dir))
		except FileNotFoundError:
			return _DirList(
				parent_dir        = parent_dir,
				dirs              = dirs,
				files             = files,
				dir_size          = dir_size,
				file_metadata     = file_metadata,
				nonstandard_files = nonstandard_files,
			)

		dir_entries = [] # TODO skip thiese, just use dirs & files lists
		file_entries = []
		nonstandard_entries = []

		with scanner as entries:
			entry: os.DirEntry|_AbstractPath
			for entry in entries:
				dir_size += 1
				try:
					if self.config.ignore_symlinks and entry.is_symlink():
						nonstandard_entries.append(entry)
						continue
					if self.config.follow_symlinks:
						is_dir = entry.is_dir(follow_symlinks=True) or entry.is_junction()
					else:
						is_dir = entry.is_dir(follow_symlinks=False)
				except OSError as e:
					self.config.logger.warning(_exc_summary(e))
					continue
				if is_dir:
					dir_entries.append(entry)
				else:
					file_entries.append(entry)

		# ensure entries are sorted
		dir_entries.sort(key = lambda x: x.name)
		file_entries.sort(key = lambda x: x.name)

		filter = self.config.filter.filter
		sep = "/" if isinstance(root, RemotePath) else os.sep
		root_name = (root.name + sep) if self.config._show_root_names else ""

		# prune dirs
		for entry in dir_entries:
			dirname = entry.name
			dir_path = dir / dirname
			dir_relpath = str(dir_path.relative_to(root))

			if not filter(dir_relpath + self.config.dst_sep, root=root):
				continue

			try:
				d = _Dir(
					relpath = dir_relpath,
					sep = sep,
					dst_sys = self.config.dst_sys,
				)
			except IncompatiblePathError:
				self.config.logger.warning(f"Ignoring incompatible dir: {root_name}{dir_relpath}{sep}")
				continue

			dirs.append(_Normalized(d))

		# prune files
		for entry in file_entries:
			# Ignore non-standard files (e.g., sockets, named pipes, block & character devices), but allow symlinks.
			if self.config.follow_symlinks:
				if not entry.is_file(follow_symlinks=True):
					nonstandard_entries.append(entry)
					continue
			else:
				if not entry.is_file(follow_symlinks=False) and not entry.is_symlink():
					nonstandard_entries.append(entry)
					continue

			filename = entry.name
			file_path = dir / filename
			file_relpath = str(file_path.relative_to(root))

			if not filter(file_relpath, root=root):
				continue

			try:
				f = _File(
					relpath = file_relpath,
					sep = sep,
					dst_sys = self.config.dst_sys,
				)
			except IncompatiblePathError as e:
				self.config.logger.warning(f"Ignoring incompatible file: {root_name}{file_relpath}")
				continue

			stat  = entry.stat(follow_symlinks=self.config.follow_symlinks)
			size  = stat.st_size
			mtime = stat.st_mtime
			if size is None or mtime is None:
				self.config.logger.warning(f"Ignoring file with unknown metadata: {root_name}{file_relpath}")
				continue

			if self.config.sftp_compat:
				mtime = float(int(mtime))

			files.append(_Normalized(f))
			file_metadata[f] = _Metadata(size=size, mtime=mtime)

		for entry in nonstandard_entries:
			filename = entry.name
			file_path = dir / filename
			file_relpath = str(file_path.relative_to(root))

			if not filter(file_relpath, root=root):
				continue

			try:
				f = _File(
					relpath = file_relpath,
					sep = sep,
					dst_sys = self.config.dst_sys,
				)
			except IncompatiblePathError as e:
				continue

			nonstandard_files.append(_Normalized(f))

		parent_relpath = str(dir.relative_to(root))
		parent_dir = _Normalized(
			_Dir(
				relpath = parent_relpath,
				sep = sep,
				dst_sys = self.config.dst_sys,
			)
		)

		return _DirList(
			parent_dir        = parent_dir,
			dirs              = dirs,
			files             = files,
			dir_size          = dir_size,
			file_metadata     = file_metadata,
			nonstandard_files = nonstandard_files,
		)

	def dir_diff(self, src_list: _DirList, dst_list: _DirList) -> _Diff:
		'''Returns a tuple of the differences between of two directories.'''

		src_parent, src_dirs, src_files, src_dir_size, src_file_metadata, _ = src_list
		dst_parent, dst_dirs, dst_files, dst_dir_size, dst_file_metadata, _ = dst_list

		src_only_dirs : dict[_Dir, None]   = {} # using a dict will keep order of keys, make it easy to remove some later
		dst_only_dirs : dict[_Dir, None]   = {}
		src_only_files: dict[_File, None]  = {}
		dst_only_files: dict[_File, None]  = {}
		dir_matches   : dict[_Dir, _Dir]   = {}
		file_matches  : dict[_File, _File] = {}
		# ignore entries that are ambiguous or in conflict with another entry
		# ignored entries won't be considered as rename candidates
		ignored_src_entries: set[_Relpath] = set()
		ignored_dst_entries: set[_Relpath] = set() # dst entries that are only matched with ignored entries


		#if dst_parent is None:
		#	# src needs to be copied over
		#	for s in src_dirs:
		#		src_only_dirs.add(s)
		#	for f in src_files:
		#		src_only_files.add(f)
		#elif src_parent is None:
		#	# dst dir needs to be deleted
		#	for d in dst_dirs:
		#		assert d is not None
		#		dst_only_dirs.add(d)
		#	for f in dst_files:
		#		dst_only_files.add(f)
		#else:

		# Find matches between src and dst.
		# A "strong match" means file names match exactly.
		# A "weak match" means file names match after normalizing case.
		# An entry can have at most one strong match, and it will always be chosen with priority.
		# An entry can have multiple weak matches.
		# A weak match will be be chosen if there is only one and there is no strong match.
		# If there is no chosen match, then the file is ignored, along with all weak matches to it.

		in_dst: dict[_Normalized, list[_Normalized]] = {}
		in_src_only: dict[_Normalized, list[_Normalized]] = {}

		for d in dst_dirs:
			in_dst[d] = []
		for f in dst_files:
			in_dst[f] = []

		for d in src_dirs:
			try:
				in_dst[d].append(d)
			except KeyError:
				try:
					in_src_only[d].append(d)
				except KeyError:
					in_src_only[d] = [d]
		for f in src_files:
			try:
				in_dst[f].append(f)
			except KeyError:
				try:
					in_src_only[f].append(f)
				except KeyError:
					in_src_only[f] = [f]

		for dst_normalized, matches in in_dst.items():
			dst_entry = dst_normalized.unwrapped
			strong_match: _Relpath|None = None
			weak_match: _Relpath|bool|None = None
			do_reject_dst = False
			rejected_src: list[_Relpath] = []
			for match_normalized in matches:
				match = match_normalized.unwrapped
				if type(dst_entry) != type(match) and not self.config.force_replace:
					do_reject_dst = True
					break
				elif dst_entry.name == match.name:
					strong_match = match
					if weak_match:
						assert isinstance(weak_match, _Relpath)
						rejected_src.append(weak_match)
				elif dst_entry.norm[-1] == match.norm[-1]:
					if strong_match:
						rejected_src.append(match)
					if weak_match is None:
						weak_match = match
					elif weak_match:
						assert isinstance(weak_match, _Relpath)
						rejected_src.append(match)
						rejected_src.append(weak_match)
						weak_match = False
					else:
						rejected_src.append(match)

			if weak_match is False and strong_match is None:
				do_reject_dst = True
			if do_reject_dst:
				rejected_src = [m.unwrapped for m in matches]
			for s in rejected_src:
				if type(s) == _Dir:
					self.config.logger.warning(f"Ignoring conflicting dir: {self.config.src_name}{s}{self.config.src_sep}")
				else:
					self.config.logger.warning(f"Ignoring conflicting file: {self.config.src_name}{s}")
				ignored_src_entries.add(s)
			if do_reject_dst:
				if type(dst_entry) == _Dir:
					self.config.logger.warning(f"Ignoring unmatched dir: {self.config.dst_name}{dst_entry}{self.config.dst_sep}")
				else:
					self.config.logger.warning(f"Ignoring unmatched file: {self.config.dst_name}{dst_entry}")
				ignored_dst_entries.add(dst_entry)

			final_match = strong_match or weak_match
			if final_match:
				if type(final_match) == _Dir and type(dst_entry) == _Dir:
					dir_matches[final_match] = dst_entry
				elif type(final_match) != _Dir and type(dst_entry) != _Dir:
					assert isinstance(final_match, _File)
					assert isinstance(dst_entry, _File)
					file_matches[final_match] = dst_entry
				elif type(final_match) != _Dir and type(dst_entry) == _Dir:
					assert isinstance(final_match, _File)
					assert type(dst_entry) == _Dir
					src_only_files[final_match] = None
					dst_only_dirs[dst_entry] = None
				else:
					assert type(final_match) == _Dir
					assert isinstance(dst_entry, _File)
					src_only_dirs[final_match] = None
					dst_only_files[dst_entry] = None

			elif not do_reject_dst:
				if type(dst_entry) == _Dir:
					dst_only_dirs[dst_entry] = None
				else:
					assert isinstance(dst_entry, _File)
					dst_only_files[dst_entry] = None

		for src_normalized, matches in in_src_only.items():
			src_entry = src_normalized.unwrapped
			if len(matches) > 1:
				for m_normalized in matches:
					m = m_normalized.unwrapped
					if type(m) == _Dir:
						self.config.logger.warning(f"Ignoring ambiguous dir: {self.config.src_name}{m}{self.config.src_sep}")
					else:
						self.config.logger.warning(f"Ignoring ambiguous file: {self.config.src_name}{m}")
					ignored_src_entries.add(m)
			else:
				m = matches[0].unwrapped
				if type(src_entry) == _Dir:
					src_only_dirs[src_entry] = None
				else:
					assert isinstance(src_entry, _File)
					src_only_files[src_entry] = None

		diff = _Diff(
			self.config,
			src_parent = None if src_parent is None else src_parent.unwrapped,
			dst_parent = None if dst_parent is None else dst_parent.unwrapped,
			src_file_metadata   = src_file_metadata,
			dst_file_metadata   = dst_file_metadata,
			src_only_dirs       = src_only_dirs,
			dst_only_dirs       = dst_only_dirs,
			src_only_files      = src_only_files,
			dst_only_files      = dst_only_files,
			dir_matches         = dir_matches,
			file_matches        = file_matches,
			ignored_src_entries = ignored_src_entries,
			ignored_dst_entries = ignored_dst_entries,
		)
		# self.config.logger.debug(diff)
		return diff

def _last_bytes(file:_AbstractPath, n:int = 1024) -> bytes:
	'''Reads and returns the last `n` bytes of a file.'''

	file_size = file.stat().st_size
	if file_size is None:
		raise PermissionError(1, "Could not get file size", str(file))
	bytes_to_read = file_size if n > file_size else n
	with file.open("rb") as f:
		f.seek(-bytes_to_read, os.SEEK_END)
		return f.read()
