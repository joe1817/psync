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
			sync.trash            = Path("/")
			sync.delete_files     = False
			sync.force_update     = False
			sync.metadata_only    = True
			sync.rename_threshold = 0

			actual = list(op.summary for op in sync._operations(
				src_entries = sync._scandir(root = a_root),
				dst_entries = sync._scandir(root = b_root),
			))

			if "nt" in os.name:
				expected = [
					f"- {os.path.join('empty', 'empty2') + os.sep}",
					f"- empty{os.sep}",
					f"U {os.path.join('A', '1.txt')}",
				]
			else:
				expected = [
					f"R {os.path.join('a', '1.txt')} -> {os.path.join('A', '1.txt')}",
					f"- {os.path.join('empty', 'empty2') + os.sep}"
					f"- empty{os.sep}",
				]
			self.assertEqual(actual, expected)

			################################################################################

			a_root = root / "a"
			c_root = root / "c"

			sync = core.Sync(a_root, c_root)
			sync.trash            = Path("/")
			sync.delete_files     = False
			sync.force_update     = False
			sync.metadata_only    = True
			sync.rename_threshold = 1000

			actual = list(op.summary for op in sync._operations(
				src_entries = sync._scandir(root = a_root),
				dst_entries = sync._scandir(root = c_root),
			))
			expected = [
				f"T {os.path.join('aa', '1.txt')}",
				f"- aa{os.sep}",
				f"+ a{os.sep}",
				f"+ {os.path.join('a', '1.txt')}",
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
			results = core.Sync(
				src,
				dst,
				trash = "auto",
				dry_run = True,
				print_level = 100,
			).run()

			self.assertEqual(hash_directory(dst), hash_dst_old)

			################################################################################

			# test basic backup
			results = core.Sync(
				src,
				dst,
				trash = "auto",
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertFalse(hash_directory(dst) == hash_dst_old)
			self.assertEqual(hash_directory(src), hash_directory(dst))
			if "nt" in os.name:
				self.assertEqual(hash_directory(results.sync.trash), hash_directory(root / "windows_expected_trash"))
			else:
				self.assertEqual(hash_directory(results.sync.trash), hash_directory(root / "linux_expected_trash"))

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
			results = core.Sync(
				src2,
				dst2,
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertFalse(hash_directory(dst2) == hash_dst2_old)
			self.assertEqual(hash_directory(src2), hash_directory(dst2))
			self.assertEqual(results[core.CreateFileOperation][0], 4)
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
			results = core.Sync(
				src,
				dst,
				trash = "auto",
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(src), hash_directory(dst))
