# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

from dataclasses import dataclass
from logging import Logger
from typing import TypeVar

from .filter import Filter
from .types import _AbstractPath

@dataclass(frozen=True)
class _SyncConfig:
	# collected in Sync constructor
	src               : _AbstractPath # using the protocol, not the typevar, allows each src, dst, etc. to be different classes
	dst               : _AbstractPath
	filter            : Filter
	translate_symlinks: bool
	ignore_symlinks   : bool
	follow_symlinks   : bool

	delete_files      : bool
	trash             : _AbstractPath|None
	force_update      : bool
	force_replace     : bool
	no_create         : bool
	no_renames        : bool
	global_renames    : bool
	metadata_only     : bool
	rename_threshold  : int

	shutdown_src      : bool
	shutdown_dst      : bool
	dry_run           : bool

	log_file          : _AbstractPath|None
	debug             : bool

	# derived
	src_sep           : str
	dst_sep           : str
	src_sys           : str
	dst_sys           : str
	src_name          : str
	dst_name          : str
	trash_name        : str
	sftp_compat       : bool

	# other
	logger            : Logger
	_show_root_names  : bool
