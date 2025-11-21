# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import os
import ntpath
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Iterator, cast, ContextManager, TypeVar
from collections import namedtuple

from .config import SyncConfig
from .types import AbstractPath
from .sftp import RemotePath, _RemotePathScanner
from .errors import IncompatiblePathError
from .helpers import _merge_iters, _reverse_dict
from .log import _exc_summary

P = TypeVar("P", bound=AbstractPath) # for dir_list

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

	def __rtruediv__(self, other:AbstractPath):
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

# TODO remove ignored entries from metadata dict
@dataclass(frozen=True)
class _Diff:
	config              : SyncConfig
	src_parent          : _Dir|None              = None
	dst_parent          : _Dir|None              = None
	src_file_metadata   : dict[_File, _Metadata] = field(default_factory=dict)
	dst_file_metadata   : dict[_File, _Metadata] = field(default_factory=dict)
	src_only_dirs       : dict[_Dir, None]       = field(default_factory=dict) # used as sorted set
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

			# Ignore if last 1kb do not match
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

	def get_file_rename_chains(self) -> Iterator[tuple[_File, _File]]:
		rename_map = self.get_file_rename_map()
		handled_renames: set[_File] = set()

		for rename_from in rename_map:
			if rename_from in handled_renames:
				continue
			chain: dict[_File, _File] = {}
			first = rename_from
			is_cycle = False

			# get chain/cycle if it exists
			while True:
				handled_renames.add(rename_from)
				if rename_from not in rename_map:
					chain = {}
					break

				rename_to = rename_map[rename_from]
				chain[rename_from] = rename_to

				if all(a==b for a,b in zip(rename_from.norm, rename_to.norm)):
					# 'from' is relative to 'to' (or vice versa), ignore either way
					# otherwise, a temp file may be needed to do this rename and that's a headache
					chain = {}
					break
				if any(rename_to.is_relative_to(d) for d in self.dst_only_files):
					# a file is blocking this rename
					chain = {}
					break

				if rename_to in self.src_only_files:
					break
				elif rename_to == first: # or (self.config.global_renames and rename_to in self.dst_only_dirs):
					is_cycle = True
					break

				rename_from = rename_to

			if chain:
				# yield the chain
				if is_cycle:
					keys = list(reversed(chain.keys()))
					yield keys[-1], chain[keys[-1]] + ".tempmove"
					for k in keys[:-1]:
						yield k, chain[k]
					yield chain[keys[-1]] + ".tempmove", chain[keys[-1]]
				else:
					keys = list(reversed(chain.keys()))
					for k in keys:
						yield k, chain[k]

				# remove renamed files from other collections
				for rename_from, rename_to in chain.items():
					try:
						del self.src_only_files[rename_to]
						# the following part is only needed for global renames mode
						if len(rename_to.norm) > 1:
							new_dir = rename_to.parent
							while True:
								del self.src_only_dirs[new_dir]
								new_dir = new_dir.parent
					except KeyError:
						pass
					try:
						del self.file_matches[rename_from]
					except KeyError:
						pass
					try:
						del self.dst_only_files[rename_from]
					except KeyError:
						pass

class _DualWalk:
	def __init__(self, config: SyncConfig, *, bottom_up_lone_dst: bool=True, get_dir_hashes: bool=False):
		self.config = config
		self.bottom_up_lone_dst = bottom_up_lone_dst
		self.get_dir_hashes = get_dir_hashes
		self.src_dir_sizes: dict[_Dir, int] = {}
		self.dst_dir_sizes: dict[_Dir, int] = {}
		self.src_dir_hash: dict[_Dir, int] = {}
		self.dst_dir_hash: dict[_Dir, int] = {}
		self.src_ancestors: set[str] = set()
		self.dst_ancestors: set[str] = set()

	def __iter__(self):
		yield from self.dual_walk(self.config.src, self.config.dst)

	def dual_walk(self, src_path: AbstractPath|None, dst_path: AbstractPath|None, *, _bottom_up: bool=False) -> Iterator[_Diff]:
		# don't follow circular symlinks
		if self.config.follow_symlinks:
			if src_path is not None:
				src_true_path = str(src_path.resolve())
				if src_true_path in self.src_ancestors:
					self.config.logger.warning(f"Symlink circular reference: {src_path} -> {src_true_path}")
					return
				self.src_ancestors.add(src_true_path)
			if dst_path is not None:
				dst_true_path = str(dst_path.resolve())
				if dst_true_path in self.dst_ancestors:
					self.config.logger.warning(f"Symlink circular reference: {dst_path} -> {dst_true_path}")
					return
				self.dst_ancestors.add(dst_true_path)

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
		# TODO? Make _DirList use dicts instead of lists, so removal is easy?
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
				self.src_ancestors.remove(src_true_path)
			if dst_path is not None:
				self.dst_ancestors.remove(dst_true_path)

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
			entry: os.DirEntry|AbstractPath
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

		# sorting should not be necessary
		#dir_entries.sort(key = lambda x: x.name)
		#file_entries.sort(key = lambda x: x.name)

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

		# find matches between src and dst
		# strong match = file names match exactly
		# weak match = file names match after normalizing case
		# a dir/file can have at most one strong match, and it will always be chosen with priority
		# a dir/file can have multiple weak matches
		# a weak match will be be chosen if there is only one and there is no strong match
		# if there is no chosen match, then the file is ignored, along with all weak matches to it

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
				if type(dst_entry) != type(match) and not self.config.force:
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

	def get_dir_rename_map(self) -> dict[_Dir, _Dir]:
		if not self.get_dir_hashes:
			raise RuntimeError("dir hashses not calculated")

		rename_map: dict[_Dir, _Dir] = {}

		dst_meta_to_relpath = _reverse_dict({k:v for k,v in self.dst_dir_hash.items() if v is not None})
		src_meta_to_relpath = _reverse_dict({k:v for k,v in self.src_dir_hash.items() if v is not None})

		# Python 3.7+ required so keys are in insertion order
		# keys should be in bottom-up order
		# don't edit other collections here, don't know which renames are valid
		for dst_dir in reversed(self.dst_dir_hash.keys()):
			hash = self.dst_dir_hash[dst_dir]
			if dst_meta_to_relpath[hash] is None:
				continue
			if any(dst_dir.is_relative_to(d) for d in rename_map):
				continue
			try:
				src_dir = src_meta_to_relpath[hash]
			except KeyError:
				continue
			if src_dir is None or all(a==b for a,b in zip(dst_dir.norm, src_dir.norm)):
				# they are equal or in a direct lineage
				continue
			rename_map[dst_dir] = src_dir

		sorted_map = {k:rename_map[k] for k in sorted(rename_map.keys())} # TODO I think it is already sorted
		return sorted_map

	def get_dir_rename_chains(self, diff: _Diff) -> Iterator[tuple[_Dir, _Dir]]:
		# get dir renames
		dir_rename_map = self.get_dir_rename_map()
		handled_renames: set[_Dir] = set()
		removed_by_rename = set()

		for rename_from in reversed(self.dst_dir_hash.keys()):
			if rename_from in dir_rename_map:
				if rename_from in handled_renames:
					continue
				chain: dict[_Dir, _Dir] = {}
				first = rename_from
				is_cycle = False

				# get chain/cycle if it exists
				while True:
					handled_renames.add(rename_from)
					if rename_from not in dir_rename_map:
						chain = {}
						break

					rename_to = dir_rename_map[rename_from]
					chain[rename_from] = rename_to

					# renamed dirs were rejected if the src/dst were nested
					# assert not all(a==b for a,b in zip(rename_from.norm, rename_to.norm))

					if any(rename_to.is_relative_to(d) for d in diff.dst_only_files):
						# a file is blocking this rename
						chain = {}
						break

					if rename_to in diff.src_only_dirs:
						break
					elif rename_to == first:
						is_cycle = True
						break

					rename_from = rename_to

				if chain:
					# yield the chain
					if is_cycle:
						keys = list(reversed(chain.keys()))
						yield keys[-1], chain[keys[-1]] + ".tempmove"
						for k in keys[:-1]:
							yield k, chain[k]
						yield chain[keys[-1]] + ".tempmove", chain[keys[-1]]
					else:
						keys = list(reversed(chain.keys()))
						for k in keys:
							yield k, chain[k]

					# remove renamed files from other collections
					for rename_from, rename_to in chain.items():
						try:
							# remove created dirs
							new_dir = rename_to
							while True:
								del diff.src_only_dirs[new_dir]
								new_dir = new_dir.parent
						except KeyError:
							pass
						try:
							del diff.dir_matches[rename_from]
							removed_by_rename.add(rename_from)
						except KeyError:
							pass
						try:
							del diff.dst_only_dirs[rename_from]
							removed_by_rename.add(rename_from)
						except KeyError:
							pass
			else:
				# remove dirs under renamed dirs
				if len(rename_from.norm) > 1:
					parent = rename_from.parent
					if parent in removed_by_rename:
						try:
							del diff.dir_matches[rename_from]
							removed_by_rename.add(rename_from)
						except KeyError:
							pass
						try:
							del diff.dst_only_dirs[rename_from]
							removed_by_rename.add(rename_from)
						except KeyError:
							pass

	def get_combined_rename_chains(self, diff: _Diff) -> Iterator[tuple[_Relpath, _Relpath]]:
		dir_rename_keys: set[_Dir] = set()
		dir_iter  = self.get_dir_rename_chains(diff)
		file_iter = diff.get_file_rename_chains()
		for comp, d, f in _merge_iters(dir_iter, file_iter, key = lambda x: x[0].norm):
			if comp == -1:
				# yield dir renames
				dir_rename_keys.add(d[0])
				yield d[0], d[1]
			elif comp == 1:
				# yield file renames not covered by dir renames
				if not any(f[0].is_relative_to(k) for k in dir_rename_keys):
					yield f[0], f[1]
			else:
				raise RuntimeError("dir & file sets should be partitions")

def _last_bytes(file:AbstractPath, n:int = 1024) -> bytes:
	'''Reads and returns the last `n` bytes of a file.'''

	file_size = file.stat().st_size
	if file_size is None:
		raise PermissionError(1, "Could not get file size", str(file))
	bytes_to_read = file_size if n > file_size else n
	with file.open("rb") as f:
		f.seek(-bytes_to_read, os.SEEK_END)
		return f.read()

"""
def combine_renames(self, rename_map, diff, dst_dir_sizes):
	'''Combine RenameFileOps into RenameDirOps.'''

	self.dst_root_relpath = self.dst.name + self.dst_sep if self.trash else ""

	do_combine = True
	renames_to_check = {}

	while do_combine:
		do_combine = False

		targets = {} # parent dir -> target parent dir
		renames_in_dir = {} # parent dir -> list of rename_from

		for rename_from, rename_to in rename_map.items():

			if len(rename_from.norm) == 1:
				continue

			if rename_from.name != rename_to.name:
				continue

			parent = rename_from.parent
			target = rename_to.parent

			if parent not in targets and target not in dst_dir_sizes:
				targets[parent] = target
				renames_in_dir[parent] = [rename_from]
			elif targets[parent] is None:
				pass
			elif target != targets[parent]:
				targets[parent] = None
				renames_in_dir[parent] = []
			else:
				renames_in_dir[parent].append(rename_from)

		for parent, rename_froms in renames_in_dir.items():
			if dst_dir_sizes[parent] == len(rename_froms):
				do_combine = True
				rename_map[parent] = targets[parent]
				for old_rename_from in rename_froms:
					del rename_map[old_rename_from]
				try:
					diff.dst_only_dirs.remove(parent)
				except KeyError:
					parent2 = diff.dir_matches[parent]
					del diff.dir_matches[parent]
					diff.src_only_dirs.add(parent2)
				diff.src_only_dirs.remove(targets[parent])

	return {k:rename_map[k] for k in sorted(rename_map.keys())}
"""
