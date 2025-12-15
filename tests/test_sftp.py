# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import os
import unittest
import logging
import tempfile
import configparser
from pathlib import Path

from psync import core, sftp
from .helpers import *

# config file location
TEST_DIR = Path(__file__).resolve().parent
CONFIG_PATH = TEST_DIR / "test_sftp.ini"

logger = logging.getLogger("psync.tests")

class TestSFTP(unittest.TestCase):

	@classmethod
	def setUpClass(cls):
		if os.getenv("TEST_REMOTE", "0").lower() not in ["true", "t", "yes", "y", "1", "on"]:
			raise unittest.SkipTest(f"Environment variable TEST_REMOTE is absent or set to false. Skipping test.")
		# read login info from config file
		if not CONFIG_PATH.exists():
			raise unittest.SkipTest(f"Configuration file not found at: {CONFIG_PATH}. Skipping test.")
		config = configparser.ConfigParser()
		config.read(CONFIG_PATH)
		try:
			cls.host     = os.getenv("HOSTNAME", None) or config.get("SERVER_INFO", "hostname")
			cls.port     = os.getenv("PORT"    , None) or config.get("SERVER_INFO", "port")
			cls.username = os.getenv("USERNAME", None) or config.get("SERVER_INFO", "username")
			cls.password = os.getenv("PASSWORD", None) or config.get("SERVER_INFO", "password")
		except (configparser.NoSectionError, configparser.NoOptionError) as e:
			raise unittest.SkipTest(f"Configuration file is missing required section/option: {e}. Skipping test.")

	@classmethod
	def tearDownClass(cls):
		# close SFTP connections
		for ssh in RemotePath.ssh_connections.values():
			try:
				command = "rm -rf ~/.psync.remote-test"
				ssh.exec_command(command)
			except:
				pass
		RemotePath.close_connections()
		logger.info("")

	@classmethod
	def get_remote_root(cls, path):
		logger.info("")
		try:
			return RemotePath.create(f"{cls.username}:{cls.password}@{cls.host}:{cls.port}/{path}", timeout=3)
		except ConnectionError:
			raise unittest.SkipTest(f"Unable to connect to test server.")

	def test_run__basic(self):
		remote_root = self.get_remote_root("~/.psync.remote-test")
		remote_structure = {
			"src": {
				"a": "a",
				"A": "A",

				"b": remote_root / "src/a",

				"cc": "cC",
				"cC": {
					"1.txt": None,
				},
				"Cc": "Cc",
			},
		}
		create_file_structure(remote_root, remote_structure)
		src = remote_root / "src"

		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			local_root = Path(temp_root)
			local_structure = {
				"dst": {
					"A": ("", 1),
					"cC": None,
					"d": None,
					"e": {
						"1.txt": None,
					}
				},
			}
			expected_windows = {
				"expected": {
					"A": "A",
					"b": local_root / "dst/a",
					"cC": {
						"1.txt": None,
					},
				}
			}
			expcted_linux = {
				"expected": {
					"a": "a",
					"A": "A",

					"b": local_root / "dst/a",

					"cc": "cC",
					"cC": {
						"1.txt": None,
					},
					"Cc": "Cc",
				},
			}
			if os.name == "nt":
				local_structure.update(expected_windows)
			else:
				local_structure.update(expected_linux)

			create_file_structure(local_root, local_structure)
			dst = local_root / "dst"
			expected = local_root / "expected"

			results = core.Sync(
				src,
				dst,
				force_replace = True,
				delete_files = True,
				print_level = 100,
				#debug = True,
			).run()

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(dst), hash_directory(expected))
