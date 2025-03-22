# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import os
import unittest
import logging

from psync import core, filter, helpers, sftp, watch

logger = logging.getLogger("psync.tests")

class TestFilter(unittest.TestCase):

	def test_pathfilter__basic(self):
		f = filter.PathFilter(r"+ **")
		self.assertTrue(f.filter("a"))
		self.assertTrue(f.filter("a/b"))
		self.assertTrue(f.filter("a/b/c"))
		self.assertTrue(f.filter(".git/"))
		self.assertTrue(f.filter("a/.git/"))
		self.assertTrue(f.filter("a/b/.git/"))
		self.assertTrue(f.filter("__pycache__/"))
		self.assertTrue(f.filter("a/__pycache__/"))
		self.assertTrue(f.filter("a/b/__pycache__/"))

		f = filter.PathFilter(r"+ **/*")
		self.assertTrue(f.filter("a"))
		self.assertTrue(f.filter("a/b"))
		self.assertTrue(f.filter("a/b/c"))
		self.assertTrue(f.filter(".git/"))
		self.assertTrue(f.filter("a/.git/"))
		self.assertTrue(f.filter("a/b/.git/"))
		self.assertTrue(f.filter("__pycache__/"))
		self.assertTrue(f.filter("a/__pycache__/"))
		self.assertTrue(f.filter("a/b/__pycache__/"))

		f = filter.PathFilter(r"- **/.*/ **/__pycache__/ + **/*/ **/*")
		self.assertTrue(f.filter("a"))
		self.assertTrue(f.filter("a/b"))
		self.assertTrue(f.filter("a/b/c"))
		self.assertFalse(f.filter(".git/"))
		self.assertFalse(f.filter("a/.git/"))
		self.assertFalse(f.filter("a/b/.git/"))
		self.assertFalse(f.filter("__pycache__/"))
		self.assertFalse(f.filter("a/__pycache__/"))
		self.assertFalse(f.filter("a/b/__pycache__/"))

		f = filter.PathFilter(r"+ audio/music/**/*.flac - **/*/ **/*")
		self.assertTrue(f.filter("audio/"))
		self.assertTrue(f.filter("audio/music/"))
		self.assertTrue(f.filter("audio/music/OST/"))
		self.assertTrue(f.filter("audio/music/OST/Star Wars/"))
		self.assertTrue(f.filter("audio/music/OST/Star Wars/Duel of the Fates.flac"))
		self.assertFalse(f.filter("video/"))
		self.assertFalse(f.filter("audio/audiobooks/"))
		self.assertFalse(f.filter("audio/music/OST/Star Wars/cover.jpg"))

		f = filter.PathFilter(r"a*/1.txt")
		self.assertTrue(f.filter("ab/1.txt"))
		self.assertFalse(f.filter("a/2.txt"))

		f = filter.PathFilter(r"- audio/music/**/*.wav + audio/music/**")
		self.assertTrue(f.filter("audio/"))
		self.assertTrue(f.filter("audio/music/"))
		self.assertTrue(f.filter("audio/music/OST/"))
		self.assertTrue(f.filter("audio/music/OST/Titanic/"))
		self.assertTrue(f.filter("audio/music/OST/Titanic/cover.jpg"))
		self.assertFalse(f.filter("audio/music/OST/Titanic/My Heart Will Go On (Recorder Cover).wav"))
		self.assertFalse(f.filter("video/"))
		self.assertFalse(f.filter("audio/audiobooks/"))

		f = filter.PathFilter(r"places.sqlite key4.db logins.json cookies.sqlite prefs.js")
		self.assertTrue(f.filter("places.sqlite"))
		self.assertTrue(f.filter("key4.db"))
		self.assertTrue(f.filter("logins.json"))
		self.assertTrue(f.filter("cookies.sqlite"))
		self.assertTrue(f.filter("prefs.js"))
		self.assertFalse(f.filter("storage.sqlite"))
		self.assertFalse(f.filter("storage/"))

		f = filter.PathFilter(r"+ * - a/ b/a/ + b/*/ - **/x + ?/**/* - **/*")
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

	def test_pathfilter__quotes(self):
		f = filter.PathFilter("'**/a b'   \"**/m n\"   **/'x y'")
		self.assertTrue(f.filter("**/a b"))
		self.assertFalse(f.filter("a b"))
		self.assertFalse(f.filter("1/2/a b"))
		self.assertTrue(f.filter("m n"))
		self.assertTrue(f.filter("1/2/m n"))
		self.assertTrue(f.filter("x y"))
		self.assertTrue(f.filter("1/2/x y"))
		self.assertFalse(f.filter("a"))

		f = filter.PathFilter("\"'a'\"   '\"b\"'   \"x ?\"'y ?' ")
		self.assertTrue(f.filter("'a'"))
		self.assertTrue(f.filter("\"b\""))
		self.assertTrue(f.filter("x 1y ?"))
		self.assertFalse(f.filter("x 1y 2"))

	def test_pathfilter__escapes(self):
		f = filter.PathFilter("Joe\\'s\\ File")
		self.assertTrue(f.filter("Joe's File"))

	def test_pathfilter__case(self):
		f = filter.PathFilter(r"+ a B - A b c + **/*", ignore_case=True)
		self.assertTrue(f.filter("a"))
		self.assertTrue(f.filter("A"))
		self.assertTrue(f.filter("b"))
		self.assertTrue(f.filter("B"))
		self.assertFalse(f.filter("c"))
		self.assertFalse(f.filter("C"))


	def test_pathfilter__default_case_sensitivity(self):
		f = filter.PathFilter(r"a")
		if os.name == "nt":
			self.assertTrue(f.filter("A"))
		else:
			self.assertFalse(f.filter("A"))

	def test_pathfilter__hidden(self):
		f = filter.PathFilter("a b/c .d", ignore_hidden=True)
		self.assertTrue(f.filter("a"))
		self.assertFalse(f.filter(".a"))
		self.assertTrue(f.filter("b/c"))
		self.assertFalse(f.filter(".b/c"))
		self.assertFalse(f.filter("b/.c"))
		self.assertTrue(f.filter(".d"))

	def test_pathfilter__relpaths(self):
		f = filter.PathFilter("./0 a/ b/ ./1 c/ c/d/ ./2")
		self.assertTrue(f.filter("0"))
		self.assertFalse(f.filter("1"))
		self.assertTrue(f.filter("a/1"))
		self.assertTrue(f.filter("b/1"))
		self.assertTrue(f.filter("a/2"))
		self.assertTrue(f.filter("b/2"))
		self.assertFalse(f.filter("c/1"))
		self.assertTrue(f.filter("a/2"))
		self.assertTrue(f.filter("b/2"))
		self.assertTrue(f.filter("c/2"))
		self.assertFalse(f.filter("c/3"))
		self.assertTrue(f.filter("c/d/2"))
		self.assertFalse(f.filter("d/2"))

		f = filter.PathFilter("a/ - b/ + c/ d ./1 + e/ ./**/*")
		self.assertFalse(f.filter("a/1"))
		self.assertFalse(f.filter("b/1"))
		self.assertTrue(f.filter("c/1"))
		self.assertFalse(f.filter("d/1"))
		self.assertTrue(f.filter("e/e/e/1"))

		f = filter.PathFilter("./a/ ./1")
		self.assertTrue(f.filter("a/"))
		self.assertTrue(f.filter("1"))
		self.assertFalse(f.filter("a/1"))

	def test_pathfilter__hyphen(self):
		f = filter.PathFilter(r"'-'")
		self.assertTrue(f.filter("-"))
		self.assertFalse(f.filter("a"))

		f = filter.PathFilter("\"-\"")
		self.assertTrue(f.filter("-"))
		self.assertFalse(f.filter("a"))

		f = filter.PathFilter(r"'- a' -a -- a\  a\ - ./- \"a a\'b")
		self.assertTrue(f.filter("- a"))
		self.assertTrue(f.filter("-a"))
		self.assertTrue(f.filter("--"))
		self.assertTrue(f.filter("a "))
		self.assertTrue(f.filter("a -"))
		self.assertTrue(f.filter("-"))
		self.assertTrue(f.filter("\"a"))
		self.assertTrue(f.filter("a'b"))
		self.assertFalse(f.filter("b"))

	def test_pathfilter__pathseps(self):
		f = filter.PathFilter(r"a/ a/b")
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

		f = filter.PathFilter(r"a\\ a\b")
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

	def test_pathfilter__invalid_hyphen(self):
		self.assertRaises(ValueError, filter.PathFilter, r"+ \-")

	def test_pathfilter__invalid_quotes(self):
		self.assertRaises(ValueError, filter.PathFilter, r"+ 'a")

	def test_pathfilter__invalid_abspath(self):
		self.assertRaises(ValueError, filter.PathFilter,  "+ /a")

	def test_pathfilter__invalid_escape(self):
		self.assertRaises(ValueError, filter.PathFilter,  "+ a\\")

	def test_allfilter__basic(self):
		f1 = filter.PathFilter("a b")
		f2 = filter.PathFilter("b c")
		f = filter.AllFilter(f1, f2)
		self.assertFalse(f.filter("a"))
		self.assertTrue(f.filter("b"))
		self.assertFalse(f.filter("c"))
