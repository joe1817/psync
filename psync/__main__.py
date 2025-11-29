# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import sys
import logging
import argparse
from argparse import BooleanOptionalAction as BOA

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

	parser.add_argument("-f", "--filter", metavar="filter_string", nargs="+", type=str, default=None, help="The filter string that includes/excludes file system entries from the 'src' and 'dst' directories. Similar to rsync, the format of the filter string is one of more repetitions of: (+ or -), followed by a list of one of more relative path patterns. Including (+) or excluding (-) of file system entries is determined by the preceding symbol of the first matching pattern. Included files will be copied over as part of the backup, while included directories will be searched. Each pattern ending with \"/\" will apply to directories only. Otherise the pattern will apply only to files. (Defaults to \"+ **/*\", which searches all directories and copies all files.)")
	parser.add_argument("-ih", "--ignore-hidden", action="store_true", default=None, help="Ignore hidden files by default in glob patterns. That is, wildcards in glob patterns will not match file system entries beginning with a dot. However, globs containing a dot (e.g., \"**/.*\") will still match these file system entries.")
	parser.add_argument("-ic", "--ignore-case", action="store_true", default=None, help="Ignore case when comparing files to the filter string.")

	parser.add_argument("--symlink-translation", action=BOA, default=None, help="Enable/disable symbolic link path transaltion on 'dst'. If false, symlinks will be copied exactly.")
	symlink_handling = parser.add_mutually_exclusive_group()
	symlink_handling.add_argument("--ignore-symlinks", action="store_true", default=None, help="Ignore symbolic links under 'src' and 'dst'. Note that 'src' and 'dst' themselves will be followed regardless of this flag.")
	symlink_handling.add_argument("--follow-symlinks", action="store_true", default=None, help="Follow symbolic links under 'src' and 'dst'. Note that 'src' and 'dst' themselves will be followed regardless of this flag.")

	parser.add_argument("-cf", "--create-files", action=BOA, default=None, help="Enable/disable file creation in 'dst'.")
	parser.add_argument("-cd", "--create-dir-tree", action=BOA, default=None, help="Enable/disable creation of the directory tree from 'src' in 'dst'.")
	parser.add_argument("--renames", action=BOA, default=None, help="Enable/disable file or directory renaming in 'dst'.")
	parser.add_argument("-xf", "--delete-files", action=BOA, default=None, help="Enable/disable file deletion in 'dst'. If 'trash' is set, then files will be moved into it instead of deleted.")
	parser.add_argument("-xd", "--delete-empty-dirs", action=BOA, default=None, help="Enable/disable deletion of empty directories in 'dst'. If 'trash' is set, then empty directories will be moved into it instead of deleted.")
	parser.add_argument("-t", "--trash", metavar="path", nargs="?", type=str, const="auto", default=None, help="The root directory to move 'extra' files (those that are in 'dst' but not 'src'). Must be on the same file system as 'dst'. If set to \"auto\", then a directory will automatically be made next to 'dst'. Extra files will not be moved if this option is omitted.")

	parser.add_argument("-fu", "--force-update", action=BOA, default=None, help="Enable/disable replacement of any files in 'dst' with older copies in 'src'.")
	parser.add_argument("-fr", "--force-replace", action=BOA, default=None, help="Enable/disable replacement of files with directories (or vice versa) when their names match.")
	parser.add_argument("-g", "--global-renames", action=BOA, default=None, help="Enable/disable renaming files between directories.")
	parser.add_argument("-c", "--content-match", action=BOA, default=None, help="Enable/disable reading the last 1kb of files when finding renamed files in 'dst'. If false, the backup process will rely solely on file metadata.")
	parser.add_argument("--rename-threshold", metavar="size", type=int, default=None, help="The minimum size in bytes needed to consider renaming files in dst to match those in 'src'. Renamed files below this threshold will be simply deleted in dst and their replacements copied over.")
	parser.add_argument("-m", "--mirror", action="store_true", default=None, help="Equivalent to --create-dir-tree, --delete-files, --force-update, and --force-replace.")

	parser.add_argument("--shutdown-src", action="store_true", default=None, help="Shutdown the src system when done.")
	parser.add_argument("--shutdown-dst", action="store_true", default=None, help="Shutdown the dst system when done.")
	parser.add_argument("-L", "--err-limit", metavar="limit", type=int, default=None, help="Quit after this many filesystem errors.")
	parser.add_argument("-d", "--dry-run", action="store_true", default=None, help="Forgo performing any operation that would make a file system change. Changes that would have occurred will still be printed to console.")

	parser.add_argument("-w", "--watch", action="store_true", default=None, help="Will watch the 'src' directory and automatically sync filesystem changes.")

	parser.add_argument("--log", metavar="path", nargs="?", type=str, const="auto", default=None, help="The path of the log file to use. It will be created if it does not exist. With \"auto\" or no argument, a tempfile will be used for the log, and it will be moved to the user's home directory after the backup is done. If this flag is absent, then no logging will be performed.")
	parser.add_argument("--log-level", type=str, default=None, help="Log level for logging to file.")

	print_level = parser.add_mutually_exclusive_group()
	print_level.add_argument("-q", action="count", default=None, help="Shorthand for --print-level WARNING (-q) and --print-level CRITICAL (-qq).")
	print_level.add_argument("-p", "--print-level", type=str, default=None, help="Log level for printing to console.")

	parser.add_argument("--debug", action="store_true", default=None, help="Shorthand for --print-level DEBUG and --log-level DEBUG.")

	parser.add_argument("--title", metavar="title", nargs="+", type=str, default=None, help="A strng to be printed in the header.")
	parser.add_argument("-nh", "--no-header", action="store_true", default=None, help="Skip logging header information.")
	parser.add_argument("-nf", "--no-footer", action="store_true", default=None, help="Skip logging footer information.")
	parser.add_argument("-nhf", "--no-header-or-footer", action="store_true", default=None, help="Skip logging header and footer information.")

	@staticmethod
	def parse(args:list[str]) -> argparse.Namespace:
		'''Convert flags specific to the command line into Sync options.'''

		log_levels = {"DEBUG": logging.DEBUG, "INFO":logging.INFO, "WARNING":logging.WARNING, "WARN":logging.WARNING, "ERROR":logging.ERROR, "ERR":logging.ERROR, "CRITICAL":logging.CRITICAL, "CRIT":logging.CRITICAL}

		parsed_args = _ArgParser.parser.parse_args(args)

		if parsed_args.q:
			if parsed_args.q == 1:
				parsed_args.print_level = logging.WARNING
			elif parsed_args.q == 2:
				parsed_args.print_level = logging.CRITICAL
			elif parsed_args.q >= 2:
				parsed_args.print_level = logging.CRITICAL+1
		elif parsed_args.print_level:
			parsed_args.print_level = log_levels[parsed_args.print_level.upper()]
		del parsed_args.q

		if parsed_args.log_level:
			parsed_args.log_level = log_levels[parsed_args.log_level.upper()]

		if parsed_args.filter:
			filter = parsed_args.filter
			if isinstance(filter, list):
				filter = " ".join(filter)
			filter = PathFilter(
				filter,
				ignore_hidden = parsed_args.ignore_hidden,
				ignore_case   = parsed_args.ignore_case,
			)
			parsed_args.filter = filter
		del parsed_args.ignore_hidden
		del parsed_args.ignore_case

		if isinstance(parsed_args.title, list):
			parsed_args.title = " ".join(parsed_args.title)

		parsed_args.no_header = parsed_args.no_header or parsed_args.no_header_or_footer
		parsed_args.no_footer = parsed_args.no_footer or parsed_args.no_header_or_footer
		del parsed_args.no_header_or_footer

		return parsed_args

def main(args:list[str]) -> None:
	'''Create a 'Sync' object and run it.'''

	try:
		parsed_args = _ArgParser.parse(args)

		if parsed_args.debug:
			logger.setLevel(logging.DEBUG)

		src = parsed_args.src
		dst = parsed_args.dst
		del parsed_args.src
		del parsed_args.dst

		watch = bool(parsed_args.watch)
		del parsed_args.watch

		kwargs = {
			key: getattr(parsed_args, key)
			for key in dir(parsed_args)
			if not key[0] == "_" and getattr(parsed_args, key) is not None
		}

		logger.debug(f"{kwargs=}")

		try:
			sync = Sync(src, dst, **kwargs)
		except (TypeError, ValueError) as e:
			logger.critical(e)
			sys.exit(1)

		if watch:
			sync.watch()
		else:
			sync.run()

		sys.exit(0)

	except KeyboardInterrupt:
		sys.exit(1)
	except ImportError as e:
		logger.critical(e)
		sys.exit(1)
	except ConnectionError as e:
		logger.critical(e)
		sys.exit(1)
	except NotImplementedError as e:
		logger.critical(e)
		sys.exit(1)
	except Exception as e:
		logger.critical("An unexpected error occurred.", exc_info=True)
		sys.exit(1)
	finally:
		RemotePath.close_connections()

if __name__ == "__main__":
	main(sys.argv[1:])
