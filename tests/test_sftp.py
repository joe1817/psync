# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import os
import unittest
import logging
import tempfile
import configparser
from pathlib import Path

from psync import core, sftp
from .utils import *

# config file location
TEST_DIR = Path(__file__).resolve().parent
CONFIG_PATH = TEST_DIR / "test_sftp.ini"

logger = logging.getLogger("psync.tests")

class TestSFTP(unittest.TestCase):

	@classmethod
	def setUpClass(cls):
		# read login info from config file
		config = configparser.ConfigParser()
		if not CONFIG_PATH.exists():
			raise unittest.SkipTest(f"Configuration file not found at: {CONFIG_PATH}")
		config.read(CONFIG_PATH)
		try:
			cls.enabled = config.get("SERVER_INFO", "enabled").lower() == "true"
			if not cls.enabled:
				raise unittest.SkipTest(f"Testing over SFTP is disabled.")
			cls.host = config.get("SERVER_INFO", "host")
			cls.port = config.getint("SERVER_INFO", "port")
			cls.username = config.get("SERVER_INFO", "username")
			self.password = os.getenv("SERVER_PASSWORD", None) or config.get("SERVER_INFO", "password")
		except (configparser.NoSectionError, configparser.NoOptionError) as e:
			raise unittest.SkipTest(f"Configuration file is missing required section/option: {e}")

	@classmethod
	def tearDownClass(cls):
		# close SFTP connections
		for ssh in RemotePath.ssh_connections:
			command = "rm -rf ~/.psync.remote-test"
			ssh.exec_command(command)
		RemotePath.close_connections()

	@classmethod
	def get_remote_root(cls, path):
		return RemotePath.create(f"{cls.username}:{cls.password}@{cls.host}:{cls.port}/{path}")

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
					"A": None,
					"cC": None,
					"d": None,
					"e": {
						"1.txt": None,
					}
				},
				"expected_windows": {
					"A": "A",
					"b": local_root / "dst/a",
					"cC": {
						"1.txt": None,
					},
				},
				"expected_linux": {
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
			create_file_structure(local_root, local_structure)
			dst = root / "dst"

			results = core.Sync(
				src,
				dst,
				force_update = True,
				print_level = 100,
			).run()

			if os.name == "nt":
				expected = local_root / "expected_windows"
			else:
				expected = local_root / "expected_linux"

			self.assertTrue(results.status == core.Results.Status.COMPLETED)
			self.assertEqual(hash_directory(dst), hash_directory(expected))
