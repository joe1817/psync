# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

from dataclasses import dataclass
from logging import Logger
from typing import TypeVar

from .filter import Filter
from .types import _AbstractPath

@dataclass(frozen=True)
class _SyncConfig:
	'''Collects the essential properties of `Sync` into a read-only data structure.'''

	src                : _AbstractPath
	dst                : _AbstractPath

	create_files       : bool
	create_dir_tree    : bool
	renames            : bool
	delete_files       : bool
	delete_empty_dirs  : bool
	trash              : _AbstractPath|None
	force_update       : bool
	force_replace      : bool
	low_memory         : bool
	match_tail         : bool
	rename_threshold   : int
	dry_run            : bool
	err_limit          : int

	filter             : Filter

	translate_symlinks : bool
	ignore_symlinks    : bool
	follow_symlinks    : bool

	log_file           : _AbstractPath|None
	debug              : bool|int
	title              : str|None
	header             : bool
	footer             : bool
	rich               : bool

	shutdown_src       : bool
	shutdown_dst       : bool

	# derived
	src_sep            : str
	dst_sep            : str
	src_sys            : str
	dst_sys            : str
	src_name           : str
	dst_name           : str
	trash_name         : str
	sftp_compat        : bool

	# other
	logger             : Logger
	_show_root_names   : bool
