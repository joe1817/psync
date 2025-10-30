# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import unittest
import logging

from psync import __main__ as main

logger = logging.getLogger("psync.tests")

class TestArgParser(unittest.TestCase):

	def test_parse(self):
		parsed = main._ArgParser.parse(["src", "dst", "-f", "+ \"a b.txt\""])
		self.assertTrue(parsed.filter.filter("a b.txt"))
		self.assertFalse(parsed.filter.filter("b.txt"))
