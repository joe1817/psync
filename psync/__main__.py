# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import sys
import logging
import argparse
#import traceback

from .core import Sync, Results
from .filter import PathFilter
from .sftp import RemotePath
from .log import logger

class _ArgParser:
	'''Argument parser for when this python file is run with arguments instead of an imported package.'''

	parser = argparse.ArgumentParser(
		description="Copy new and updated files from one directory to another.",
		epilog="(c) 2025 Joe Walter",
		fromfile_prefix_chars="!",
	)

	parser.add_argument("src", help="The root directory to copy files from.")
	parser.add_argument("dst", help="The root directory to copy files to.")

	parser.add_argument("-f", "--filter", metavar="filter_string", nargs="+", type=str, default="+ **/*/ **/*", help="The filter string that includes/excludes file system entries from the 'src' and 'dst' directories. Similar to rsync, the format of the filter string is one of more repetitions of: (+ or -), followed by a list of one of more relative path patterns. Including (+) or excluding (-) of file system entries is determined by the preceding symbol of the first matching pattern. Included files will be copied over as part of the backup, while included directories will be searched. Each pattern ending with \"/\" will apply to directories only. Otherise the pattern will apply only to files. (Defaults to \"+ **/*/ **/*\", which searches all directories and copies all files.)")
	parser.add_argument("-H", "--ignore-hidden", action="store_true", default=False, help="Ignore hidden files by default in glob patterns. That is, wildcards in glob patterns will not match file system entries beginning with a dot. However, globs containing a dot (e.g., \"**/.*\") will still match these file system entries.")
	parser.add_argument("-I", "--ignore-case", action="store_true", default=False, help="Ignore case when comparing files to the filter string.")

	symlink_handling = parser.add_mutually_exclusive_group()
	symlink_handling.add_argument("-L", "--ignore-symlinks", action="store_true", default=False, help="Ignore symbolic links under 'src' and 'dst'. Note that 'src' and 'dst' themselves will be followed regardless of this flag.")
	symlink_handling.add_argument("--follow-symlinks", action="store_true", default=False, help="Follow symbolic links under 'src' and 'dst'. Note that 'src' and 'dst' themselves will be followed regardless of this flag.")

	extra_handling = parser.add_mutually_exclusive_group()
	extra_handling.add_argument("-t", "--trash", metavar="path", nargs="?", type=str, default=None, const="auto", help="The root directory to move 'extra' files (those that are in 'dst' but not 'src'). Must be on the same file system as 'dst'. If set to \"auto\", then a directory will automatically be made next to 'dst'. Extra files will not be moved if this option is omitted.")
	extra_handling.add_argument("-x", "--delete-files", action="store_true", default=False, help="Permanently delete 'extra' files (those that are in 'dst' but not 'src').")

	parser.add_argument("-F", "--force-update", action="store_true", default=False, help="Allow replacement of any newer files in 'dst' with older copies in 'src'.")
	parser.add_argument("-m", "--metadata_only", action="store_true", default=False, help="Use only metadata in determining which files in 'dst' are the result of a rename. Otherwise, the backup process will also compare the last 1kb of files.")
	parser.add_argument("-R", "--rename-threshold", metavar="size", type=int, default=10000, help="The minimum size in bytes needed to consider renaming files in dst to match those in 'src'. Renamed files below this threshold will be simply deleted in dst and their replacements copied over.")

	parser.add_argument("-w", "--watch", action="store_true", default=False, help="Will watch the 'src' directory and automatically sync filesystem changes.")

	parser.add_argument("-d", "--dry-run", action="store_true", default=False, help="Forgo performing any operation that would make a file system change. Changes that would have occurred will still be printed to console.")

	parser.add_argument("--log", metavar="path", nargs="?", type=str, default=None, const="auto", help="The path of the log file to use. It will be created if it does not exist. With \"auto\" or no argument, a tempfile will be used for the log, and it will be moved to the user's home directory after the backup is done. If this flag is absent, then no logging will be performed.")
	parser.add_argument("--log-level", type=str, default="DEBUG", help="Log level for logging to file.")

	print_level = parser.add_mutually_exclusive_group()
	print_level.add_argument("--debug", action="store_true", default=False, help="Shorthand for --print-level DEBUG.")
	print_level.add_argument("-q", action="count", default=0, help="Shorthand for --print-level WARNING (-q) and --print-level CRITICAL (-qq).")
	print_level.add_argument("-p", "--print-level", type=str, default="INFO", help="Log level for printing to console.")

	parser.add_argument("-nh", "--no-header", action="store_true", default=False, help="Skip logging header information.")
	parser.add_argument("-nf", "--no-footer", action="store_true", default=False, help="Skip logging footer information.")
	parser.add_argument("-nhf", "--no-header-or-footer", action="store_true", default=False, help="Skip logging header and footer information.")

	@staticmethod
	def parse(args:list[str]) -> argparse.Namespace:
		log_levels = {"DEBUG": logging.DEBUG, "INFO":logging.INFO, "WARNING":logging.WARNING, "WARN":logging.WARNING, "ERROR":logging.ERROR, "ERR":logging.ERROR, "CRITICAL":logging.CRITICAL, "CRIT":logging.CRITICAL}

		parsed_args = _ArgParser.parser.parse_args(args)
		if parsed_args.q == 1:
			parsed_args.print_level = logging.WARNING
		elif parsed_args.q == 2:
			parsed_args.print_level = logging.CRITICAL
		elif parsed_args.q >= 2:
			parsed_args.print_level = logging.CRITICAL+1
		elif parsed_args.debug:
			parsed_args.print_level = logging.DEBUG
		else:
			parsed_args.print_level = log_levels[parsed_args.print_level.upper()]
		del parsed_args.q

		parsed_args.log_level = log_levels[parsed_args.log_level.upper()]

		filter = parsed_args.filter
		if isinstance(filter, list):
			filter = " ".join(filter)
		filter = PathFilter(
			filter,
			ignore_hidden = parsed_args.ignore_hidden,
			ignore_case   = parsed_args.ignore_case,
		)
		parsed_args.filter = filter

		return parsed_args

def _sync_cmd(args:list[str]) -> None:
	'''Run `Sync.run()` with command line arguments.'''
	try:
		parsed_args = _ArgParser.parse(args)
	except SystemExit:
		# message logged by argparse
		return

	if parsed_args.debug:
		logger.setLevel(logging.DEBUG)

	try:
		sync = Sync(
			parsed_args.src,
			parsed_args.dst,

			filter           = parsed_args.filter,
			ignore_symlinks  = parsed_args.ignore_symlinks,
			follow_symlinks  = parsed_args.follow_symlinks,

			trash            = parsed_args.trash,
			delete_files     = parsed_args.delete_files,
			force_update     = parsed_args.force_update,
			metadata_only    = parsed_args.metadata_only,
			rename_threshold = parsed_args.rename_threshold,

			dry_run          = parsed_args.dry_run,
			log_file         = parsed_args.log,
			log_level        = parsed_args.log_level,
			print_level      = parsed_args.print_level,
			no_header        = parsed_args.no_header or parsed_args.no_header_or_footer,
			no_footer        = parsed_args.no_footer or parsed_args.no_header_or_footer,
		)
	except (TypeError, ValueError) as e:
		logger.error(str(e), exc_info=False) # assume input error, don't print stack trace
		return
	except ConnectionError as e:
		logger.error(str(e), exc_info=False)
		return

	sync.run()

if __name__ == "__main__":
	try:
		_sync_cmd(sys.argv[1:])
	except KeyboardInterrupt:
		pass
	#except Exception:
	#	print()
	#	traceback.print_exc()
	finally:
		RemotePath.close_connections()
