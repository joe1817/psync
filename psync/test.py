# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import io
import os
import time
import contextlib
import hashlib
import tempfile
import traceback
import logging
import unittest
import doctest
from pathlib import Path

from . import core, sftp
from . import __main__ as main

logger = logging.getLogger("psync")

class TempLoggingLevel:
	def __init__(self, logger, level):
		self.logger = logger
		self.level = level
	def __enter__(self):
		self.old_level = self.logger.level
		self.logger.setLevel(self.level)
	def __exit__(self, exc_type, exc_val, exc_tb):
		self.logger.setLevel(self.old_level)

def hash_directory(root:Path, *, follow_links:bool=False, ignore_empty_dirs:bool=False, verbose:bool=False):
	if verbose:
		print("--- Hash Start ---")
	hasher = hashlib.sha256()
	for dir, dirnames, filenames in os.walk(root, followlinks=follow_links):
		if ignore_empty_dirs and not filenames:
			continue
		dirnames.sort(key=lambda x: (os.path.normcase(x), x))
		filenames.sort(key=lambda x: (os.path.normcase(x), x))
		dir_relpath = os.path.normcase(os.path.relpath(dir, root))
		hasher.update(dir_relpath.encode())
		if verbose:
			print(dir_relpath)
		for file in filenames:
			file_path = os.path.join(dir, file)
			file_relpath = os.path.normcase(os.path.relpath(file_path, root))
			hasher.update(file_relpath.encode())
			if verbose:
				print(file_relpath)
			try:
				with open(file_path, "rb") as f:
					while True:
						buf = f.read(4096)
						if not buf:
							break
						hasher.update(buf)
						if verbose:
							print(buf)
			except OSError as e:
				print(f"Error hashing {file_path}: {e}")
	if verbose:
		print("--- Hash End ---")
	return hasher.hexdigest()

def create_file_structure(root_dir:Path, structure:dict, *, _symlinks:dict|None = None, _delay:float = 0.01):
	'''Recursively creates a directory structure with files.'''
	root_dir.mkdir(parents=True, exist_ok=True)
	if _symlinks is not None:
		symlinks = _symlinks
	else:
		symlinks = {}
	for name, content in structure.items():
		file_path = root_dir / name
		if isinstance(content, Path):
			# create symlink
			symlinks[file_path] = content
		elif isinstance(content, dict):
			# create dir
			create_file_structure(file_path, content, _symlinks=symlinks, _delay=0)
		elif isinstance(content, (tuple, list)):
			# Create file with modtime and content
			file_path.write_text(content[0] or "")
			mtime = float(content[1])
			os.utime(file_path, (mtime, mtime))
			assert os.stat(file_path).st_mtime == mtime
			# Force the OS to write all metadata changes (including mtime) to disk
			#with open(file_path, "r+") as f:
			#	f.flush()
			#	os.fsync(f.fileno())
		elif content is None:
			# Create an empty file
			file_path.touch()
		else:
			# Create a file with content
			file_path.write_text(content)
	# On Windows, symlink type will be assumed to be "File" if the target does not exist
	# So, create symlinks after everything else
	if _symlinks is None:
		for path, target in symlinks.items():
			os.symlink(target, path)
	if _delay:
		# delay so filesystem cache can update (changes to modtimes), TODO not sure if this actually works
		time.sleep(_delay)

def load_tests(loader, tests, ignore):
	tests.addTests(doctest.DocTestSuite(core))
	tests.addTests(doctest.DocTestSuite(sftp))
	return tests

class TestBackup(unittest.TestCase):

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_argparse(self):
		parsed = main._ArgParser.parse(["src", "dst", "-f", "+ \"a b.txt\""])
		self.assertTrue(parsed.filter[0] == "+ \"a b.txt\"")

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_filter(self):
		f = core._Filter(r"+ **")
		self.assertTrue(f.filter("a"))
		self.assertTrue(f.filter("a/b"))
		self.assertTrue(f.filter("a/b/c"))
		self.assertTrue(f.filter(".git/"))
		self.assertTrue(f.filter("a/.git/"))
		self.assertTrue(f.filter("a/b/.git/"))
		self.assertTrue(f.filter("__pycache__/"))
		self.assertTrue(f.filter("a/__pycache__/"))
		self.assertTrue(f.filter("a/b/__pycache__/"))

		f = core._Filter(r"+ **/*")
		self.assertTrue(f.filter("a"))
		self.assertTrue(f.filter("a/b"))
		self.assertTrue(f.filter("a/b/c"))
		self.assertTrue(f.filter(".git/"))
		self.assertTrue(f.filter("a/.git/"))
		self.assertTrue(f.filter("a/b/.git/"))
		self.assertTrue(f.filter("__pycache__/"))
		self.assertTrue(f.filter("a/__pycache__/"))
		self.assertTrue(f.filter("a/b/__pycache__/"))

		f = core._Filter(r"- **/.*/ **/__pycache__/ + **/*/ **/*")
		self.assertTrue(f.filter("a"))
		self.assertTrue(f.filter("a/b"))
		self.assertTrue(f.filter("a/b/c"))
		self.assertFalse(f.filter(".git/"))
		self.assertFalse(f.filter("a/.git/"))
		self.assertFalse(f.filter("a/b/.git/"))
		self.assertFalse(f.filter("__pycache__/"))
		self.assertFalse(f.filter("a/__pycache__/"))
		self.assertFalse(f.filter("a/b/__pycache__/"))

		f = core._Filter(r"+ audio/music/**/*.flac - **/*/ **/*")
		self.assertTrue(f.filter("audio/"))
		self.assertTrue(f.filter("audio/music/"))
		self.assertTrue(f.filter("audio/music/OST/"))
		self.assertTrue(f.filter("audio/music/OST/Star Wars/"))
		self.assertTrue(f.filter("audio/music/OST/Star Wars/Duel of the Fates.flac"))
		self.assertFalse(f.filter("video/"))
		self.assertFalse(f.filter("audio/audiobooks/"))
		self.assertFalse(f.filter("audio/music/OST/Star Wars/cover.jpg"))

		f = core._Filter(r"- audio/music/**/*.wav + audio/music/**")
		self.assertTrue(f.filter("audio/"))
		self.assertTrue(f.filter("audio/music/"))
		self.assertTrue(f.filter("audio/music/OST/"))
		self.assertTrue(f.filter("audio/music/OST/Titanic/"))
		self.assertTrue(f.filter("audio/music/OST/Titanic/cover.jpg"))
		self.assertFalse(f.filter("audio/music/OST/Titanic/My Heart Will Go On (Recorder Cover).wav"))
		self.assertFalse(f.filter("video/"))
		self.assertFalse(f.filter("audio/audiobooks/"))

		f = core._Filter(r"places.sqlite key4.db logins.json cookies.sqlite prefs.js")
		self.assertTrue(f.filter("places.sqlite"))
		self.assertTrue(f.filter("key4.db"))
		self.assertTrue(f.filter("logins.json"))
		self.assertTrue(f.filter("cookies.sqlite"))
		self.assertTrue(f.filter("prefs.js"))
		self.assertFalse(f.filter("storage.sqlite"))
		self.assertFalse(f.filter("storage/"))

		f = core._Filter("'**/a b' \"**/A B\" **/'x y'  Joe\\'s\\ File")
		self.assertTrue(f.filter("**/a b"))
		self.assertFalse(f.filter("a b"))
		self.assertFalse(f.filter("1/2/a b"))
		self.assertTrue(f.filter("A B"))
		self.assertTrue(f.filter("1/2/A B"))
		self.assertTrue(f.filter("x y"))
		self.assertTrue(f.filter("1/2/x y"))
		self.assertTrue(f.filter("Joe's File"))
		self.assertFalse(f.filter("a"))

		f = core._Filter("\"'a'\"   '\"b\"'   \"x ?\"'y ?' ")
		self.assertTrue(f.filter("'a'"))
		self.assertTrue(f.filter("\"b\""))
		self.assertTrue(f.filter("x 1y ?"))
		self.assertFalse(f.filter("x 1y 2"))

		f = core._Filter(r"+ * - a/ b/a/ + b/*/ - **/x + ?/**/* - **/*")
		self.assertTrue(f.filter("a"))
		self.assertTrue(f.filter("b/a"))
		self.assertTrue(f.filter("b/a/a"))
		self.assertTrue(f.filter("b/b/"))
		self.assertFalse(f.filter("a/"))
		self.assertFalse(f.filter("b/a/"))
		self.assertFalse(f.filter("b/b/x"))
		self.assertTrue(f.filter("b/y"))
		self.assertTrue(f.filter("b/b/y"))
		self.assertFalse(f.filter("aa/y"))

		f = core._Filter(r"+ a B - A b c + **/*", ignore_hidden=True, ignore_case=True)
		self.assertTrue(f.filter("a"))
		self.assertTrue(f.filter("A"))
		self.assertTrue(f.filter("b"))
		self.assertTrue(f.filter("B"))
		self.assertFalse(f.filter("c"))
		self.assertFalse(f.filter("C"))
		self.assertFalse(f.filter(".a"))
		self.assertFalse(f.filter(".A"))
		self.assertFalse(f.filter("d/.a"))
		self.assertFalse(f.filter("d/.A"))

		f = core._Filter(r"./a/ ./a/b")
		self.assertTrue(f.filter("a/"))
		self.assertTrue(f.filter("a/b"))
		self.assertFalse(f.filter("c"))

		f = core._Filter(r"a/ a/b")
		if os.sep == "\\":
			self.assertTrue(f.filter("a/"))
			self.assertTrue(f.filter("a\\"))
			self.assertTrue(f.filter("a/b"))
			self.assertTrue(f.filter("a\\b"))
			self.assertFalse(f.filter("c"))
		else:
			self.assertTrue(f.filter("a/"))
			self.assertFalse(f.filter("a\\"))
			self.assertTrue(f.filter("a/b"))
			self.assertFalse(f.filter("a\\b"))
			self.assertFalse(f.filter("c"))

		f = core._Filter(r"a\\ a\b")
		if os.sep == "\\":
			self.assertTrue(f.filter("a/"))
			self.assertTrue(f.filter("a\\"))
			self.assertTrue(f.filter("a/b"))
			self.assertTrue(f.filter("a\\b"))
			self.assertFalse(f.filter("c"))
		else:
			self.assertFalse(f.filter("a/"))
			self.assertTrue(f.filter("a\\"))
			self.assertFalse(f.filter("a/b"))
			self.assertTrue(f.filter("a\\b"))
			self.assertFalse(f.filter("c"))

		f = core._Filter(r"'- a' -a -- a\  a\ - ./- \"a a\'b")
		self.assertTrue(f.filter("- a"))
		self.assertTrue(f.filter("-a"))
		self.assertTrue(f.filter("--"))
		self.assertTrue(f.filter("a "))
		self.assertTrue(f.filter("a -"))
		self.assertTrue(f.filter("-"))
		self.assertTrue(f.filter("\"a"))
		self.assertTrue(f.filter("a'b"))
		self.assertFalse(f.filter("b"))

		f = core._Filter(r"'-'")
		self.assertTrue(f.filter("-"))
		self.assertFalse(f.filter("a"))

		f = core._Filter("\"-\"")
		self.assertTrue(f.filter("-"))
		self.assertFalse(f.filter("a"))

		f = core._Filter(r"\-")
		self.assertFalse(f.filter("-"))
		self.assertTrue(f.filter("\\-"))
		self.assertFalse(f.filter("a"))

		self.assertRaises(core._InputError, core._Filter, r"+ 'a")
		self.assertRaises(core._InputError, core._Filter,  "+ a\\")

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_scandir(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"a": {
					"aa": {
						"aaa": {
							"1.txt": None,
						},
						"aab": {
							"12.txt": None,
						},
						"aac": {
							"21.txt": None,
						},

						"1.txt": None,
					},
					"ab": {
						"aba": {
							"1.jpg": None,
						},
						"abb": {
							"12.jpg": None,
						},
						"abc": {
							"21.jpg": None,
						},

						"1.jpg": None,
					},
					"ac": {
						"aca": {
							"1.html": None,
						},
						"acb": {
							"12.html": None,
						},
						"acc": {
							"21.html": None,
						},

						"1.html": None,
					},

					"1.txt": None,
					"1.jpg": None,
					"1.html": None,
				},
				"b": {
					"ba": {
						"1.txt": None,
					},
					"bb": {
					},
					"bc": {
					},
				},
				"c": {
					"ca": {},
					"cb": {},
					"cc": {},
				},
			}
			create_file_structure(root, file_structure)

			files = core._scandir(
				root = root,
				filter = "- b/ c/ + **/*/ **/1.???"
			)
			files_expected = [
				"a/aa/1.txt",
				"a/aa/aaa/1.txt",
				"a/ab/1.jpg",
				"a/ab/aba/1.jpg",
				"a/1.txt",
				"a/1.jpg",
			]
			self.assertEqual(
				sorted(f.normpath for f in files),
				sorted(f.replace("/", os.sep) for f in files_expected)
			)

			################################################################################

			files = core._scandir(
				root = root,
				filter = "+ a/a?/a?b/*",
			)
			files_expected = [
				"a/aa/aab/12.txt",
				"a/ab/abb/12.jpg",
				"a/ac/acb/12.html",
			]
			self.assertEqual(
				sorted(f.normpath for f in files),
				sorted(f.replace("/", os.sep) for f in files_expected)
			)

			################################################################################

			file_structure = {
				"d": root / "e",
				"e": {
					"1.txt": None,
					"2.txt": Path("./1.txt"),
					"3.txt": root / "e" / "1.txt",
					"ea": root / "f",
					"eb": Path("../f"),
				},
				"f": {
					"1.txt": None,
					"2.txt":  root / "f" / "1.txt",
				},
				"g": {
					"1.txt": None,
					"circular": root / "g",
				},
			}
			create_file_structure(root, file_structure)

			files = core._scandir(
				root = root / "d",
				filter = "+ **/*/ **/*",
				ignore_symlinks = False,
				follow_symlinks = False,
			)
			files_expected = [
				"1.txt",
				"2.txt",
				"3.txt",
				"ea",
				"eb",
			]
			self.assertEqual(
				sorted(f.normpath for f in files),
				sorted(f.replace("/", os.sep) for f in files_expected)
			)

			################################################################################

			files = core._scandir(
				root = root / "d",
				filter = "+ **/*/ **/*",
				follow_symlinks = True,
			)
			files_expected = [
				"1.txt",
				"2.txt",
				"3.txt",
				"ea/1.txt",
				"ea/2.txt",
				"eb/1.txt",
				"eb/2.txt",
			]
			self.assertEqual(
				sorted(f.normpath for f in files),
				sorted(f.replace("/", os.sep) for f in files_expected)
			)

			################################################################################

			files = core._scandir(
				root = root / "g",
				filter = "+ **/*/ **/*",
				follow_symlinks = True,
			)
			files_expected = [
				"1.txt",
			]
			with TempLoggingLevel(logger, logging.ERROR):
				self.assertEqual(
					sorted(f.normpath for f in files),
					sorted(f.replace("/", os.sep) for f in files_expected)
				)

			################################################################################

			files = core._scandir(
				root = root / "d",
				filter = "+ **/*/ **/*",
				ignore_symlinks = True,
			)
			files_expected = [
				"1.txt",
			]
			self.assertEqual(
				sorted(f.normpath for f in files),
				sorted(f.replace("/", os.sep) for f in files_expected)
			)

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_operations(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"a": {
					"a": {
						"1.txt": None
					},
				},
				"b": {
					"A": {
						"1.txt": (None, 0)
					},
					"empty": {
						"empty2": {
						},
					},
				},
				"c": {
					"aa": {
						"1.txt": None
					},
				},
			}
			create_file_structure(root, file_structure)

			a_root = root / "a"
			b_root = root / "b"
			a_files = core._scandir(
				root = a_root
			)
			b_files = core._scandir(
				root = b_root
			)

			actual = list(op.summary for op in core._operations(
				src_root         = a_root,
				dst_root         = b_root,
				src_files        = a_files,
				dst_files        = b_files,
				trash_root       = Path("/"),
				delete_files     = False,
				force_update     = False,
				metadata_only    = True,
				rename_threshold = 0,
			))

			if "nt" in os.name:
				expected = [
					f"- {os.path.join('empty', 'empty2') + os.sep}",
					f"U {os.path.join('A', '1.txt')}",
				]
			else:
				expected = [
					f"R {os.path.join('a', '1.txt')} -> {os.path.join('A', '1.txt')}",
					f"- {os.path.join('empty', 'empty2') + os.sep}"
				]
			self.assertEqual(actual, expected)

			################################################################################

			a_root = root / "a"
			c_root = root / "c"
			a_files = core._scandir(
				root = a_root
			)
			c_files = core._scandir(
				root = c_root
			)
			actual = list(op.summary for op in core._operations(
				src_root         = a_root,
				dst_root         = c_root,
				src_files        = a_files,
				dst_files        = c_files,
				trash_root       = Path("/"),
				delete_files     = False,
				force_update     = False,
				metadata_only    = True,
				rename_threshold = 1000,
			))
			expected = [
				f"~ {os.path.join('aa', '1.txt')}",
				f"+ {os.path.join('a', '1.txt')}"
			]
			self.assertEqual(actual, expected)

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_move(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"a": {
					"a": {
						"1.txt": None
					}
				}
			}
			create_file_structure(root, file_structure)

			src = root / "a" / "a" / "1.txt"
			dst = root / "b" / "b" / "2.txt"
			core._move(src, dst)
			self.assertEqual(os.listdir(root / "a" / "a"), [])
			self.assertEqual(os.listdir(root / "b" / "b"), ["2.txt"])

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_backup(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"src": {
					"a": {
						"aa": {
							"aa1.txt": None,
						},
						"a1.txt": "new info",
						"a2.txt": None,
					},
					"b": {
						"b1.txt": None,
					},
					"empty": {
						"empty-empty": {
						},
					},
					"empty2": {
					},
				},
				"dst": {
					"A": {
						"A1.txt": ("old info", 1),
						"A3.txt": None,
					},
					"Empty": {
						"Empty-empty": {
						},
					},
					"empty3": {
						"empty3-empty": {
						},
					},
				},
				"windows_expected_trash": {
					"A": {
						"A3.txt": None,
					},
				},
				"linux_expected_trash": {
					"A": {
						"A1.txt": ("old info", 1),
						"A3.txt": None,
					},
				},
			}
			create_file_structure(root, file_structure)
			src = root / "src"
			dst = root / "dst"
			hash_src_old = hash_directory(src)
			hash_dst_old = hash_directory(dst)

			################################################################################

			# test dry_run
			self.assertFalse(hash_src_old == hash_dst_old)
			results = core.sync(
				src,
				dst,
				trash = "auto",
				quiet = True,
				dry_run = True,
			)
			self.assertEqual(hash_directory(dst), hash_dst_old)

			################################################################################

			# test basic backup
			results = core.sync(
				src,
				dst,
				trash = "auto",
				quiet = True
			)
			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertFalse(hash_directory(dst) == hash_dst_old)
			self.assertEqual(hash_directory(src), hash_directory(dst))
			if "nt" in os.name:
				self.assertEqual(hash_directory(results.trash_root), hash_directory(root / "windows_expected_trash"))
			else:
				self.assertEqual(hash_directory(results.trash_root), hash_directory(root / "linux_expected_trash"))

			################################################################################

			# test backup with symlink src
			file_structure = {
				"src2" : root / "src",
				"dst2" : {}
			}
			create_file_structure(root, file_structure)
			src2 = root / "src2"
			dst2 = root / "dst2"
			hash_dst2_old = hash_directory(dst2)
			results = core.sync(
				src2,
				dst2,
				quiet = True,
			)
			self.assertFalse(hash_directory(dst2) == hash_dst2_old)
			self.assertEqual(hash_directory(src2), hash_directory(dst2))
			self.assertEqual(results.create_success, 4)
		assert not root.exists()

		################################################################################

		# test backup involving many overlapping file names
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"dst": {
					"a1": {
						"a": (None, 1)
					},
					"b1": {},
					"c1": (None, 1),

					"b2": {
						"a": (None, 1)
					},
					"c2": {},
					"a2": (None, 1),

					"c3": {
						"a": (None, 1)
					},
					"a3": {},
					"b3": (None, 1),
				},
				"src": {
					"a1": {
						"a": None
					},
					"b1": {},
					"c1": None,

					"a2": {
						"a": None
					},
					"b2": {},
					"c2": None,

					"a3": {
						"a": None
					},
					"b3": {},
					"c3": None,
				},
			}
			create_file_structure(root, file_structure)
			src = root / "src"
			dst = root / "dst"
			results = core.sync(
				src,
				dst,
				trash = "auto",
				quiet = True,
			)
			self.assertEqual(hash_directory(src), hash_directory(dst))

if __name__ == "__main__":
	try:
		unittest.main()
	except SystemExit as e:
		pass
