# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import sys
import argparse
import traceback

from .core import sync, Results
from .sftp import RemotePath

class _ArgParser:
	'''Argument parser for when this python file is run with arguments instead of an imported package.'''

	parser = argparse.ArgumentParser(
		description="Copy new and updated files from one directory to another.",
		epilog="(c) 2025 Joe Walter",
		fromfile_prefix_chars="!",
	)

	parser.add_argument("src", help="The root directory to copy files from.")
	parser.add_argument("dst", help="The root directory to copy files to.")

	parser.add_argument("-f", "--filter", metavar="filter_string", nargs="+", type=str, default="+ **/*/ **/*", help="The filter string that includes/excludes file system entries from the `src` and `dst` directories. Similar to rsync, the format of the filter string is one of more repetitions of: (+ or -), followed by a list of one of more relative path patterns. Including (+) or excluding (-) of file system entries is determined by the preceding symbol of the first matching pattern. Included files will be copied over as part of the backup, while included directories will be searched. Each pattern ending with \"/\" will apply to directories only. Otherise the pattern will apply only to files. Note that it is still possible for excluded files in `dst` to be overwritten. (Defaults to \"+ **/*/ **/*\", which searches all directories and copies all files.)")
	parser.add_argument("-H", "--ignore-hidden", action="store_true", default=False, help="Ignore hidden files by default in glob patterns. That is, wildcards in glob patterns will not match file system entries beginning with a dot. However, globs containing a dot (e.g., \"**/.*\") will still match these file system entries.")
	parser.add_argument("-I", "--ignore-case", action="store_true", default=False, help="Ignore case when comparing files to the filter string.")

	symlink_handling = parser.add_mutually_exclusive_group()
	symlink_handling.add_argument("-L", "--ignore-symlinks", action="store_true", default=False, help="Ignore symbolic links under `src` and `dst`. Note that `src` and `dst` themselves will be followed regardless of this flag.")
	symlink_handling.add_argument("--follow-symlinks", action="store_true", default=False, help="Follow symbolic links under `src` and `dst`. Note that `src` and `dst` themselves will be followed regardless of this flag.")

	extra_handling = parser.add_mutually_exclusive_group()
	extra_handling.add_argument("-t", "--trash", metavar="path", nargs="?", type=str, default=None, const="auto", help="The root directory to move 'extra' files (those that are in `dst` but not `src`). Must be on the same file system as `dst`. If set to \"auto\", then a directory will automatically be made next to `dst`. Extra files will not be moved if this option is omitted.")
	extra_handling.add_argument("-x", "--delete-files", action="store_true", default=False, help="Permanently delete 'extra' files (those that are in `dst` but not `src`).")

	parser.add_argument("-F", "--force-update", action="store_true", default=False, help="Replace any newer files in `dst` with older copies in `src`.")
	parser.add_argument("-m", "--metadata_only", action="store_true", default=False, help="Use only metadata in determining which files in `dst` are the result of a rename. Otherwise, the backup process will also compare the last 1kb of files.")
	parser.add_argument("-R", "--rename-threshold", metavar="size", nargs=1, type=int, default=10000, help="The minimum size in bytes needed to consider renaming files in dst to match those in `src`. Renamed files below this threshold will be simply deleted in dst and their replacements copied over.")

	parser.add_argument("-d", "--dry-run", action="store_true", default=False, help="Forgo performing any operation that would make a file system change. Changes that would have occurred will still be printed to console.")

	parser.add_argument("--log", metavar="path", nargs="?", type=str, default=None, const="auto", help="The path of the log file to use. It will be created if it does not exist. With \"auto\" or no argument, a tempfile will be used for the log, and it will be moved to the user's home directory after the backup is done. If this flag is absent, then no logging will be performed.")
	parser.add_argument("--debug", action="store_true", default=False, help="Log debug messages.")
	parser.add_argument("-q", action="count", default=0, help="Forgo printing to stdout (-q) and stderr (-qq).")

	@staticmethod
	def parse(args:list[str]) -> argparse.Namespace:
		parsed_args = _ArgParser.parser.parse_args(args)
		parsed_args.quiet     = parsed_args.q >= 1
		parsed_args.veryquiet = parsed_args.q >= 2
		del parsed_args.q
		return parsed_args

def _sync_cmd(args:list[str]) -> Results:
	'''Run `sync()` with command line arguments.'''

	parsed_args = _ArgParser.parse(args)
	return sync(
		parsed_args.src,
		parsed_args.dst,

		filter           = parsed_args.filter if isinstance(parsed_args.filter, str) else " ".join(parsed_args.filter),
		ignore_hidden    = parsed_args.ignore_hidden,
		ignore_case      = parsed_args.ignore_case,
		ignore_symlinks  = parsed_args.ignore_symlinks,
		follow_symlinks  = parsed_args.follow_symlinks,

		trash            = parsed_args.trash,
		delete_files     = parsed_args.delete_files,
		force_update     = parsed_args.force_update,
		metadata_only    = parsed_args.metadata_only,
		rename_threshold = parsed_args.rename_threshold,

		dry_run          = parsed_args.dry_run,

		log              = parsed_args.log,
		debug            = parsed_args.debug,
		quiet            = parsed_args.quiet,
		veryquiet        = parsed_args.veryquiet
	)

if __name__ == "__main__":
	try:
		_sync_cmd(sys.argv[1:])
	except SystemExit:
		# from argparse
		pass
	except Exception:
		print()
		traceback.print_exc()
	finally:
		RemotePath.close_connections()