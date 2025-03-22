# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

from dataclasses import dataclass
from logging import Logger
from typing import TypeVar

from .filter import Filter
from .types import AbstractPath

@dataclass(frozen=True)
class SyncConfig:
	# collected in Sync constructor
	src               : AbstractPath # using the protocol, not the typevar, allows each src, dst, etc. to be different classes
	dst               : AbstractPath
	filter            : Filter
	trash             : AbstractPath|None
	delete_files      : bool
	no_create         : bool
	force             : bool
	global_renames    : bool
	metadata_only     : bool
	rename_threshold  : int|None
	translate_symlinks: bool
	ignore_symlinks   : bool
	follow_symlinks   : bool
	shutdown_src      : bool
	shutdown_dst      : bool
	dry_run           : bool
	log_file          : AbstractPath|None
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
