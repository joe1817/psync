# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import os
import unittest
import logging
import tempfile
from pathlib import Path

from psync import core, filter
from .utils import *

logger = logging.getLogger("psync.tests")

class TestSync(unittest.TestCase):

	@classmethod
	def setUpClass(cls):
		core.Sync._RAISE_UNKNOWN_ERRORS = True

	def test_init(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			sync = core.Sync(root, root)

			sync.filter = "- a b + c d - **"

			sync.trash = root
			sync.delete_files = False

			self.assertEqual(sync.trash, root)
			self.assertEqual(sync.delete_files, False)

			with self.assertRaises(RuntimeError):
				sync.delete_files = True

			sync.trash = None
			sync.delete_files = True

			self.assertEqual(sync.trash, None)
			self.assertEqual(sync.delete_files, True)

			with self.assertRaises(RuntimeError):
				sync.trash = root

			sync.no_create = True
			self.assertEqual(sync.no_create, True)
			sync.no_create = False
			self.assertEqual(sync.no_create, False)

			sync.force_update = True
			self.assertEqual(sync.force_update, True)
			sync.force_update = False
			self.assertEqual(sync.force_update, False)

			sync.metadata_only = True
			self.assertEqual(sync.metadata_only, True)
			sync.metadata_only = False
			self.assertEqual(sync.metadata_only, False)

			sync.rename_threshold = 100
			self.assertEqual(sync.rename_threshold, 100)
			sync.rename_threshold = 0
			self.assertEqual(sync.rename_threshold, 0)
			sync.rename_threshold = None
			self.assertEqual(sync.rename_threshold, None)

			sync.ignore_symlinks = True
			sync.follow_symlinks = False

			self.assertEqual(sync.ignore_symlinks, True)
			self.assertEqual(sync.follow_symlinks, False)

			with self.assertRaises(RuntimeError):
				sync.follow_symlinks = True

			sync.ignore_symlinks = False
			sync.follow_symlinks = True

			self.assertEqual(sync.ignore_symlinks, False)
			self.assertEqual(sync.follow_symlinks, True)

			with self.assertRaises(RuntimeError):
				sync.ignore_symlinks = True

			sync.dry_run = True
			self.assertEqual(sync.dry_run, True)
			sync.dry_run = False
			self.assertEqual(sync.dry_run, False)

			sync.log_file = root / "tmp.log"

			self.assertEqual(sync.log_file, root / "tmp.log")

			sync.log_level = logging.ERROR
			sync.print_level = logging.ERROR

			self.assertEqual(sync.print_level, logging.ERROR)
			self.assertEqual(sync.log_level, logging.ERROR)

			sync.log_file = None
			self.assertEqual(sync.log_file, None)

			sync.log_level = logging.INFO
			self.assertEqual(sync.log_level, logging.INFO)

			sync.no_header = True
			self.assertEqual(sync.no_header, True)
			sync.no_header = False
			self.assertEqual(sync.no_header, False)

			sync.no_footer = True
			self.assertEqual(sync.no_footer, True)
			sync.no_footer = False
			self.assertEqual(sync.no_footer, False)

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_scandir(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			sync = core.Sync(root, root) # src, dst need to exist, otherwise don't matter
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

			sync.filter = "- b/ c/ + **/*/ **/1.???"
			files = sync._scandir(root = root)
			files_expected = [
				"a/aa/1.txt",
				"a/aa/aaa/1.txt",
				"a/ab/1.jpg",
				"a/ab/aba/1.jpg",
				"a/1.txt",
				"a/1.jpg",
			]
			self.assertEqual(
				sorted(f.norm_relpath for f in files if not isinstance(f, core._Dir)),
				sorted(f.replace("/", os.sep) for f in files_expected)
			)

			################################################################################

			sync.filter = "+ a/a?/a?b/*"
			files = sync._scandir(root = root)
			files_expected = [
				"a/aa/aab/12.txt",
				"a/ab/abb/12.jpg",
				"a/ac/acb/12.html",
			]
			self.assertEqual(
				sorted(f.norm_relpath for f in files if not isinstance(f, core._Dir)),
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

			sync.filter = "+ **"
			sync.ignore_symlinks = False
			sync.follow_symlinks = False
			files = sync._scandir(root = root / "d")
			files_expected = [
				"1.txt",
				"2.txt",
				"3.txt",
				"ea",
				"eb",
			]
			self.assertEqual(
				sorted(f.norm_relpath for f in files if not isinstance(f, core._Dir)),
				sorted(f.replace("/", os.sep) for f in files_expected)
			)

			################################################################################

			sync.filter = "+ **"
			sync.ignore_symlinks = False
			sync.follow_symlinks = True
			files = sync._scandir(root = root / "d")
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
				sorted(f.norm_relpath for f in files if not isinstance(f, core._Dir)),
				sorted(f.replace("/", os.sep) for f in files_expected)
			)

			################################################################################

			sync.filter = "+ **"
			sync.ignore_symlinks = False
			sync.follow_symlinks = True
			files = sync._scandir(root = root / "g")
			files_expected = [
				"1.txt",
			]
			with TempLoggingLevel(sync.logger, logging.ERROR):
				self.assertEqual(
					sorted(f.norm_relpath for f in files if not isinstance(f, core._Dir)),
					sorted(f.replace("/", os.sep) for f in files_expected)
				)

			################################################################################

			sync.filter = "+ **"
			sync.follow_symlinks = False
			sync.ignore_symlinks = True
			files = sync._scandir(root = root / "d")
			files_expected = [
				"1.txt",
			]
			self.assertEqual(
				sorted(f.norm_relpath for f in files if not isinstance(f, core._Dir)),
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

			sync = core.Sync(a_root, b_root)
			sync.trash            = root / "trash"
			sync.delete_files     = False
			sync.force_update     = False
			sync.metadata_only    = True
			sync.rename_threshold = 0

			actual = list(op.summary for op in sync._operations(
				src_entries = sync._scandir(root = a_root),
				dst_entries = sync._scandir(root = b_root),
			))

			if os.name == "nt":
				expected = [
					f"- {os.path.join('b', 'empty', 'empty2') + os.sep}",
					f"- {os.path.join('b', 'empty') + os.sep}",
					f"U {os.path.join('b', 'A', '1.txt')}",
				]
			else:
				expected = [
					f"R {os.path.join('b', 'a', '1.txt')} -> {os.path.join('A', '1.txt')}",
					f"- {os.path.join('b', 'empty', 'empty2') + os.sep}"
					f"- {os.path.join('b', 'empty') + os.sep}",
				]
			self.assertEqual(actual, expected)

			################################################################################

			a_root = root / "a"
			c_root = root / "c"

			sync = core.Sync(a_root, c_root)
			sync.trash            = root / "trash"
			sync.delete_files     = False
			sync.force_update     = False
			sync.metadata_only    = True
			sync.rename_threshold = 1000

			actual = list(op.summary for op in sync._operations(
				src_entries = sync._scandir(root = a_root),
				dst_entries = sync._scandir(root = c_root),
			))
			expected = [
				f"+ {os.path.join('trash', 'aa') + os.sep}",
				f"T {os.path.join('c', 'aa', '1.txt')}",
				f"- {os.path.join('c', 'aa') + os.sep}",
				f"+ {os.path.join('c', 'a') + os.sep}",
				f"+ {os.path.join('c', 'a', '1.txt')}",
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

	def test_run__dry_run(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"src": {
					"a" : "2",
					"b" : None,
					"c" : Path("./a"),
					"d" : {
						"e" : None,
					},
					"f": {
					},
				},
				"dst": {
					"a" : ("1", 0),
					"g": {
					},
				},
			}
			create_file_structure(root, file_structure)
			src = root / "src"
			dst = root / "dst"

			hash_src_old = hash_directory(src)
			hash_dst_old = hash_directory(dst)

			results = core.Sync(
				src,
				dst,
				trash = "auto",
				force_update = True,
				dry_run = True,
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(dst), hash_dst_old)

			results = core.Sync(
				src,
				dst,
				delete_files = True,
				force_update = True,
				dry_run = True,
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(dst), hash_dst_old)

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_run__no_dst(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"src": {
					"a" : None,
				},
			}
			create_file_structure(root, file_structure)
			src = root / "src"
			dst = root / "dst"

			results = core.Sync(
				src,
				dst,
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(src), hash_directory(dst))

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_run__basic(self):
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

			results = core.Sync(
				src,
				dst,
				trash = "auto",
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(src), hash_directory(dst))
			if os.name == "nt":
				self.assertEqual(hash_directory(results.sync.trash), hash_directory(root / "windows_expected_trash"))
			else:
				self.assertEqual(hash_directory(results.sync.trash), hash_directory(root / "linux_expected_trash"))

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_run__basic2(self):
		# test backup involving many overlapping file names
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"src": {
					"a1": {
						"a": ""
					},
					"b1": {},
					"c1": "-",

					"a2": {
						"a": ""
					},
					"b2": {},
					"c2": "",

					"a3": {
						"a": ""
					},
					"b3": {},
					"c3": "",
				},
				"dst": {
					"a1": {
						"a": ("", 1)
					},
					"b1": {},
					"c1": ("", 1),

					"b2": {
						"a": ("", 1)
					},
					"c2": {},
					"a2": ("", 1),

					"c3": {
						"a": ("", 1)
					},
					"a3": {},
					"b3": ("", 1),
				},
			}
			create_file_structure(root, file_structure)
			src = root / "src"
			dst = root / "dst"

			results = core.Sync(
				src,
				dst,
				trash = "auto",
				print_level = 10,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(src, hash_mtime=True, verbose=True), hash_directory(dst, hash_mtime=True, verbose=True))

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_run__symlink_roots(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"src": root / "a",
				"dst": root / "b",
				"a": {
					"a": "1",
				},
				"b": {
				},
			}
			create_file_structure(root, file_structure)
			src = root / "src"
			dst = root / "dst"

			# follow_symlinks = False is default, but should not apply to the root dirs

			results = core.Sync(
				src,
				dst,
				print_level = 100,
				follow_symlinks = False,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(src), hash_directory(dst))
			self.assertEqual(results[core.CreateFileOperation].success, 1)

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_run__symlinks_with_translation(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"src": {
					"a": {
						"1": None,
					},
					"b": Path("a/1"),
					"c": Path(root / "src/a/1"),
				},
				"dst": {
				},
			}
			create_file_structure(root, file_structure)
			src = root / "src"
			dst = root / "dst"

			self.assertEqual(readlink(src/"b"), os.path.join("a", "1"))
			self.assertEqual(readlink(src/"c"), str(root / "src/a/1"))

			results = core.Sync(
				src,
				dst,
				translate_symlinks = True, # these symlink settings are all default
				follow_symlinks = False,
				ignore_symlinks = False,
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(src, follow_symlinks=True), hash_directory(dst, follow_symlinks=True))
			self.assertEqual(readlink(dst/"b"), os.path.join("a", "1"))
			self.assertEqual(readlink(dst/"c"), str(root / "dst/a/1"))
			self.assertEqual(results[core.CreateDirOperation].success, 1)
			self.assertEqual(results[core.CreateFileOperation].success, 1)
			self.assertEqual(sum(results[core.CreateSymlinkOperation]), 2) # Windows will fail to update time metadata on a symlink

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_run__renames(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"src": {
					"a": ("1", 1),
					"b": ("2", 2),
					"c": ("3", 3),
					"d": ("3", 10),
					"e": ("3", 10),
					"f": ("3", 20),
				},
				"dst": {
					"a2": ("1", 1),
					"b2": ("2", 2),
					"c2": ("3", 3),
					"d2": ("3", 10),
					"e2": ("3", 20),
					"f2": ("3", 20),
				},
			}
			create_file_structure(root, file_structure)
			src = root / "src"
			dst = root / "dst"

			results = core.Sync(
				src,
				dst,
				delete_files = True,
				rename_threshold = 0,
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(src), hash_directory(dst))
			self.assertEqual(results[core.RenameFileOperation].success, 3)
			self.assertEqual(results[core.CreateFileOperation].success, 3)
			self.assertEqual(results[core.DeleteFileOperation].success, 3)

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_run__rename_blocked(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"src": {
					"a": ("1", 1),
				},
				"dst": {
					"a": {
						"1": None,
					},
					"b": ("1", 1),
				},
			}
			create_file_structure(root, file_structure)
			src = root / "src"
			dst = root / "dst"

			results = core.Sync(
				src,
				dst,
				delete_files = True,
				rename_threshold = 0,
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(src), hash_directory(dst))
			self.assertEqual(results[core.RenameFileOperation].success, 1)
			self.assertEqual(results[core.DeleteFileOperation].success, 1)
