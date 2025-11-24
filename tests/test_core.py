# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import os
import unittest
import logging
import tempfile
from pathlib import Path

from psync import core, operations, filter, log
from .helpers import *

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

			sync.trash = False
			sync.delete_files = True

			self.assertEqual(sync.trash, None)
			self.assertEqual(sync.delete_files, True)

			with self.assertRaises(RuntimeError):
				sync.trash = root

			sync.delete_files = False
			sync.trash = "auto"

			self.assertEqual(sync.trash, True)

			sync.no_create = True
			self.assertEqual(sync.no_create, True)
			sync.no_create = False
			self.assertEqual(sync.no_create, False)

			sync.force = True
			self.assertEqual(sync.force, True)
			sync.force = False
			self.assertEqual(sync.force, False)

			sync.global_renames = True
			self.assertEqual(sync.global_renames, True)
			sync.global_renames = False
			self.assertEqual(sync.global_renames, False)

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

			sync.symlink_translation = True
			self.assertEqual(sync.symlink_translation, True)
			sync.symlink_translation = False
			self.assertEqual(sync.symlink_translation, False)

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

			sync.log_file = False

			self.assertEqual(sync.log_file, None)

			sync.log_file = "auto"

			self.assertEqual(sync.log_file, True)

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
			operations._move(src, dst)
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
				force = True,
				dry_run = True,
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(dst), hash_dst_old)

			results = core.Sync(
				src,
				dst,
				delete_files = True,
				force = True,
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
					"empty3": {
						"empty3-empty": {
						},
					},
				},
				"linux_expected_trash": {
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
				self.assertEqual(hash_directory(results.config.trash), hash_directory(root / "windows_expected_trash"))
			else:
				self.assertEqual(hash_directory(results.config.trash), hash_directory(root / "linux_expected_trash"))

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_run__filtering(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"src": {
					"aa.txt": None,
					"ab": {
						"1.txt": None,
						"2.html": None,
					},
					"ba.txt": None,
					"bb": {
						"1.txt": None,
						"2.html": None,
					},
					"aaa": root / "src/aa.txt",
					"baa": root / "src/aa.txt",
				},
				"dst": {
				},
				"expected": {
					"aa.txt": None,
					"ab": {
						"1.txt": None,
					},
					"aaa": root / "src/aa.txt",
				}
			}
			create_file_structure(root, file_structure)
			src = root / "src"
			dst = root / "dst"
			expected = root / "expected"

			results = core.Sync(
				src,
				dst,
				filter = "+ a* a*/*.txt",
				translate_symlinks = False,
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(dst), hash_directory(expected))

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_run__update_case(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"src": {
					"A": None,
				},
				"dst": {
					"a": (None, 1),
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
			self.assertEqual(os.listdir(src), os.listdir(dst))

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_run__no_force(self):
		# test backup involving many overlapping file names
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"src": {
					"a1": {
						"a": (None, 2)
					},
					"b1": {},
					"c1": (None, 2),

					"a2": {
						"a": (None, 2)
					},
					"b2": {},
					"c2": (None, 2),

					"a3": {
						"a": (None, 2)
					},
					"b3": {},
					"c3": (None, 2),
				},
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
				"expected": {
					"a1": {
						"a": (None, 2)
					},
					"b1": {},
					"c1": (None, 2),

					"b2": {},
					"c2": {},
					"a2": (None, 1),

					"c3": {
						"a": (None, 1)
					},
					"a3": {
						"a": (None, 2)
					},
					"b3": (None, 1),
				},
			}
			create_file_structure(root, file_structure)
			src = root / "src"
			dst = root / "dst"
			expected = root / "expected"

			results = core.Sync(
				src,
				dst,
				trash = "auto",
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(dst), hash_directory(expected))
	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_run__force(self):
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
			}
			create_file_structure(root, file_structure)
			src = root / "src"
			dst = root / "dst"

			results = core.Sync(
				src,
				dst,
				trash = "auto",
				force = True,
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(src), hash_directory(dst))

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
				follow_symlinks = False,
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(src), hash_directory(dst))
			self.assertEqual(results[operations.CreateFileOperation].success, 1)

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
			self.assertEqual(results[operations.CreateDirOperation].success, 1)
			self.assertEqual(results[operations.CreateFileOperation].success, 1)
			self.assertEqual(sum(results[operations.CreateSymlinkOperation]), 2) # Windows will fail to update time metadata on a symlink

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
				metadata_only = True,
				rename_threshold = 0,
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(src), hash_directory(dst))
			self.assertEqual(results[operations.RenameFileOperation].success, 3)
			self.assertEqual(results[operations.CreateFileOperation].success, 3)
			self.assertEqual(results[operations.DeleteFileOperation].success, 3)

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_run__rename_blocked(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"src": {
					"a": (None, 1),
					"d": (None, 2),
				},
				"dst": {
					"a": {
						"1": None,
					},
					"b": (None, 1),

					"c": (None, 2),
					"d": (None, 3),
				},
			}
			create_file_structure(root, file_structure)
			src = root / "src"
			dst = root / "dst"

			results = core.Sync(
				src,
				dst,
				force = True,
				delete_files = True,
				rename_threshold = 0,
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)

			self.assertEqual(hash_directory(src), hash_directory(dst))
			self.assertEqual(results[operations.RenameFileOperation].success, 1)
			self.assertEqual(results[operations.DeleteFileOperation].success, 2) # c, a\1

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_run__trash_dir(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"src": {
				},
				"dst": {
					"a": {
						"b": None,
						"c": {
							"d": None,
							"e": {
								"f": None,
								"g": {
								},
							},
						},
					},
				},
			}
			create_file_structure(root, file_structure)
			src = root / "src"
			dst = root / "dst"

			results = core.Sync(
				src,
				dst,
				#delete_files = True,
				trash = "auto",
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(src), hash_directory(dst))

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_run__rename_collisions(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"src": {
					"aa": {
						"1": (None, 1),
					},

					"ab": (None, 2),


					"ac": {
						"1": (None, 3),
					},
					"ad": {
						"1": (None, 3),
					},

					"ae": (None, 4),
					"af": (None, 4),
				},
				"dst": {
					"ba": {
						"1": (None, 1),
					},
					"bb": {
						"1": (None, 1),
					},

					"bc": (None, 2),
					"bd": (None, 2),

					"be": {
						"1": (None, 3),
					},

					"bf": (None, 4),
				},
			}
			create_file_structure(root, file_structure)
			src = root / "src"
			dst = root / "dst"

			results = core.Sync(
				src,
				dst,
				delete_files = True,
				global_renames = True,
				rename_threshold = 0,
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(src), hash_directory(dst))
			self.assertEqual(sum(results[operations.RenameFileOperation]), 0)
			self.assertEqual(sum(results[operations.RenameDirOperation]), 0)


	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_run__global_renames(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"src": {
					"a": ("a", 1),
					"b": ("b", 2),
					"d": {
						"c": ("c", 3),
					},

					"m": ("m", 4),
					"p": {
						"q": ("q", 5),
					},

					"r": {
						"s": ("s", 6),
					},

					"t": {
						"1": ("t1", 7),
						"2": ("t2", 8),
					},

					"x": {
						"y": ("y", 9),
					},
				},
				"dst": {
					"a2": ("a", 1),
					"d2": {
						"b2": ("b", 2),
					},
					"c2": ("c", 3),

					"m": {
						"n": ("m", 4),
					},
					"p": ("q", 5),

					"r": None,
					"s": ("s", 6),

					"u": {
						"1": ("t1", 7),
						"2": ("t2", 8),
					},

					"x": ("y", 9),
				},
			}
			create_file_structure(root, file_structure)
			src = root / "src"
			dst = root / "dst"

			results = core.Sync(
				src,
				dst,
				delete_files = True,
				force = True, # TODO force_replace in the future
				global_renames = True,
				rename_threshold = 0,
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(src), hash_directory(dst))
			#self.assertEqual(sum(results[operations.CreateFileOperation]), 0)
			#self.assertEqual(sum(results[operations.DeleteFileOperation]), 1) # r

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_run__global_renames2(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			root = Path(temp_root)
			file_structure = {
				"src": {
					"a": {
						"1.txt": 1,
						"b": {
							"2.txt": 2,
						},
					},
					"c": {
						"3.txt": 3,
					},
				},
				"dst": {
					"c": {
						"1.txt": 1,
						"b": {
							"2.txt": 2,
						},
					},
					"a": {
						"3.txt": 3,
					},
				},
			}
			create_file_structure(root, file_structure)
			src = root / "src"
			dst = root / "dst"

			results = core.Sync(
				src,
				dst,
				global_renames = True,
				metadata_only = True,
				rename_threshold = 0,
				print_level = 100,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(src), hash_directory(dst))
			self.assertEqual(results[operations.CreateFileOperation].success, 0)
