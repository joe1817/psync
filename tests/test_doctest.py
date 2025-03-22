# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import doctest
import logging

from psync import core, filter, helpers, log, sftp, watch

logger = logging.getLogger("psync.tests")

def load_tests(loader, tests, ignore):
	logger.info("Adding doctests to unittest.")
	tests.addTests(doctest.DocTestSuite(core))
	tests.addTests(doctest.DocTestSuite(filter))
	tests.addTests(doctest.DocTestSuite(helpers))
	tests.addTests(doctest.DocTestSuite(log))
	tests.addTests(doctest.DocTestSuite(sftp))
	tests.addTests(doctest.DocTestSuite(watch))
	return tests
