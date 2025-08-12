# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import os
import posixpath
import stat
import socket
import tempfile
import logging
from pathlib import PurePath
from urllib.parse import urlparse
from collections.abc import Buffer
from typing import Generator

try:
	import paramiko
except ImportError:
	pass

logger = logging.getLogger("psync")

# TODO use/extend PurePath
class RemotePath:
	"""
	A Path subclass that operates on a remote SFTP server using an existing
	paramiko SFTPClient object.
	"""

	ssh_connections :dict[str, paramiko.client.SSHClient] = {}
	sftp_connections:dict[str, paramiko.sftp_client.SFTPClient] = {}

	@classmethod
	def create(cls, s:str, timeout=10) -> "RemotePath":
		if "paramiko" not in globals():
			raise ImportError("Paramiko package is needed for SFTP connections. Install it with: pip install paramiko")

		if not s.startswith("ftp://") and not s.startswith("sftp://"):
			s = "sftp://" + s
		parsed = urlparse(s)
		if not parsed.hostname or not parsed.username:
			raise ValueError("Malformed URI")

		if parsed.netloc not in cls.ssh_connections:
			password = input(f"Password for {parsed.netloc}: ")

			ssh = paramiko.SSHClient()
			ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

			try:
				ssh.connect(
					parsed.hostname,
					port     = parsed.port or 22,
					username = parsed.username,
					password = password,
					timeout  = timeout,
				)
			except socket.gaierror as e:
				raise ValueError("Invalid hostname") from e
			except TimeoutError as e:
				raise ValueError("Host timeout") from e
			except paramiko.ssh_exception.AuthenticationException as e:
				raise ValueError(str(e)) from e
			cls.ssh_connections[parsed.netloc] = ssh

			ftp = ssh.open_sftp()
			cls.sftp_connections[parsed.netloc] = ftp

		return RemotePath(parsed.path or "/", parsed.netloc)

	@classmethod
	def close_connections(cls):
		for ftp in cls.sftp_connections:
			try:
				ftp.close()
			except:
				pass
		for ssh in cls.ssh_connections:
			try:
				ssh.close()
			except:
				pass

	@classmethod
	def copy_file(cls, src, dst, *, follow_symlinks:bool = False):
		if isinstance(src, RemotePath) and isinstance(dst, RemotePath):
			with tempfile.NamedTemporaryFile(mode="w+b", delete=True) as temp_file:
				src_connection = RemotePath.sftp_connections[src.conn_details]
				if follow_symlinks:
					src = src.resolve()
				stat = src.stat()
				src_connection.get(str(src), temp_file)

				dst_connection = RemotePath.sftp_connections[dst.conn_details]
				dst_connection.put(temp_file, str(dst))
				dst_connection.utime(str(dst), (stat.st_atime, stat.st_mtime))
		elif isinstance(src, RemotePath):
			connection = RemotePath.sftp_connections[src.conn_details]
			if follow_symlinks:
				src = src.resolve()
			stat = src.stat()
			connection.get(str(src), dst)
			os.utime(dst, (stat.st_atime, stat.st_mtime))
		elif isinstance(dst, RemotePath):
			connection = RemotePath.sftp_connections[dst.conn_details]
			if follow_symlinks:
				src = src.resolve()
			stat = src.stat()
			connection.put(src, str(dst))
			connection.utime(str(dst), (stat.st_atime, stat.st_mtime))
		else:
			raise ValueError("At least one path must be a RemotePath")

	connection:paramiko.sftp_client.SFTPClient

	def __init__(self, path:str|os.PathLike[str], conn_details:str):
		path = str(path)
		if path != "/" and path.endswith("/"):
			path = path[:-1]
		parts = posixpath.splitext(path)
		self.path         : str = path
		self.conn_details : str = conn_details
		self.stem         : str = parts[0].split("/")[-1]
		self.suffix       : str = parts[1]
		self.name         : str = self.stem + self.suffix
		self._stat        : paramiko.sftp_attr.SFTPAttributes|None = None
		self._lstat       : paramiko.sftp_attr.SFTPAttributes|None = None

	@property
	def parent(self) -> "RemotePath":
		new_path_obj = posixpath.dirname(self.path) # this is why self.path cannot end with trailing slash
		return type(self)(new_path_obj, self.conn_details)

	def __truediv__(self, other:str|os.PathLike[str]) -> "RemotePath":
		new_path_obj = self.path + "/" + str(other)
		return type(self)(new_path_obj, self.conn_details)

	def __eq__(self, other:object) -> bool:
		if not isinstance(other, RemotePath):
			return NotImplemented
		if self.conn_details != other.conn_details:
			raise ValueError("Cannot compare RemotePaths with different SSH connections")
		return str(self) == str(other)

	def __str__(self):
		return self.path

	def __fspath__(self):
		return self.path

	def joinpath(self, *other:str|os.PathLike[str]) -> "RemotePath":
		"""
		Overrides the joinpath method to return a new "RemotePath" object.
		"""
		#new_path_obj = self.path + "/" + "/".join(str(s) for s in other)
		new_path_obj = posixpath.join(self.path, *other)
		return type(self)(new_path_obj, self.conn_details)

	def with_name(self, new_name) -> "RemotePath":
		return self.parent / new_name

	def stat(self, *, follow_symlinks=True) -> paramiko.sftp_attr.SFTPAttributes:
		if follow_symlinks:
			if not self._stat:
				self._stat = RemotePath.sftp_connections[self.conn_details].stat(str(self))
				if not stat.S_ISLNK(self._stat.st_mode):
					self._lstat = self._stat
			assert self._stat is not None
			return self._stat
		else:
			if not self._lstat:
				self._lstat = RemotePath.sftp_connections[self.conn_details].lstat(str(self))
				if not stat.S_ISLNK(self._lstat.st_mode):
					self._stat = self._lstat
			assert self._lstat is not None
			return self._lstat

	def exists(self, *, follow_symlinks:bool=True) -> bool:
		try:
			attrs = self.stat(follow_symlinks=follow_symlinks)
			return True
		except FileNotFoundError:
			return False

	def is_symlink(self) -> bool:
		attrs = RemotePath.sftp_connections[self.conn_details].lstat(str(self))
		assert attrs.st_mode is not None
		return stat.S_ISLNK(attrs.st_mode)

	# needed to mimic os.DirEntry
	def is_junction(self) -> bool:
		return False # TODO

	def is_dir(self, *, follow_symlinks:bool=True) -> bool:
		attrs = self.stat(follow_symlinks=follow_symlinks)
		assert attrs.st_mode is not None
		return stat.S_ISDIR(attrs.st_mode)

	def is_file(self, *, follow_symlinks:bool=True) -> bool:
		attrs = self.stat(follow_symlinks=follow_symlinks)
		assert attrs.st_mode is not None
		return stat.S_ISREG(attrs.st_mode)

	def resolve(self, strict=False) -> "RemotePath":
		rp = type(self)(RemotePath.sftp_connections[self.conn_details].normalize(str(self)), self.conn_details)
		if rp._stat is None:
			rp._stat = self._lstat
		return rp

	def iterdir(self) -> Generator["RemotePath"]:
		for item_name in RemotePath.sftp_connections[self.conn_details].listdir(str(self)):
			yield type(self)(str(self / item_name), self.conn_details)

	def chmod(self, mode, *, follow_symlinks=True) -> None:
		RemotePath.sftp_connections[self.conn_details].chmod(str(self), mode)

	def read_text(self, encoding:str|None="utf-8", errors:str|None="strict", newline:str|None=os.linesep) -> str:
		"""
		Reads the content of a file on the remote server and returns it as a string.
		Uses a temporary local file for the transfer.
		"""

		if not self.is_file():
			raise FileNotFoundError(f"No such file on remote:'{self}'")

		# Create a temporary file to download the content
		with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
			tmp_path = tmp_file.name

		try:
			RemotePath.sftp_connections[self.conn_details].get(str(self), tmp_path)
			with open(tmp_path, "r", encoding=encoding, errors=errors) as f:
				return f.read()
		finally:
			os.remove(tmp_path) # Clean up the temporary file

	def write_text(self, data:str, encoding:str|None="utf-8", errors:str|None="strict", newline:str|None=os.linesep) -> int:
		"""
		Writes a string to a file on the remote server.
		Uses a temporary local file for the transfer.
		"""

		# Create a temporary file to write the content to locally first
		with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding=encoding, errors=errors) as tmp_file:
			tmp_file.write(data)
			tmp_path = tmp_file.name

		try:
			RemotePath.sftp_connections[self.conn_details].put(tmp_path, str(self))
		finally:
			os.remove(tmp_path) # Clean up the temporary file
		return 0

	def read_bytes(self) -> bytes:
		"""
		Reads the content of a file on the remote server and returns it as bytes.
		Uses a temporary local file for the transfer.
		"""

		if not self.is_file():
			raise FileNotFoundError(f"No such file on remote:'{self}'")

		with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
			tmp_path = tmp_file.name

		try:
			RemotePath.sftp_connections[self.conn_details].get(str(self), tmp_path)
			with open(tmp_path, "rb") as f:
				return f.read()
		finally:
			os.remove(tmp_path)

	def write_bytes(self, data:Buffer) -> int:
		"""
		Writes bytes to a file on the remote server.
		Uses a temporary local file for the transfer.
		"""

		with tempfile.NamedTemporaryFile(mode="wb", delete=False) as tmp_file:
			tmp_file.write(data)
			tmp_path = tmp_file.name

		try:
			RemotePath.sftp_connections[self.conn_details].put(tmp_path, str(self))
		finally:
			os.remove(tmp_path)
		return 0

	def mkdir(self, mode:int=0o777, parents:bool=False, exist_ok:bool=False) -> None:
		try:
			parent = self.parent
			if parents and self != parent and not parent.exists():
				parent.mkdir(mode, parents, exist_ok)
			RemotePath.sftp_connections[self.conn_details].mkdir(str(self), mode)
		except IOError as e:
			if exist_ok:
				pass

	def rmdir(self) -> None:
		RemotePath.sftp_connections[self.conn_details].rmdir(str(self))

	def unlink(self, missing_ok:bool=True) -> None:
		RemotePath.sftp_connections[self.conn_details].remove(str(self))

	def open(self, mode="r", buffering=-1, encoding=None, errors=None, newline=None):
		return RemotePath.sftp_connections[self.conn_details].open(str(self), mode="r")

	def rename(self, target:str|os.PathLike[str]) -> "RemotePath":
		if not isinstance(target, RemotePath):
			target = type(self)(str(target), self.conn_details)
			#raise ValueError("target not a RemotePath")
		if self.conn_details != target.conn_details:
			raise ValueError("netloc mismatch")
		RemotePath.sftp_connections[self.conn_details].rename(str(self), str(target))
		return target

	def replace(self, target:str|os.PathLike[str]) -> "RemotePath":
		if not isinstance(target, RemotePath):
			target = type(self)(str(target), self.conn_details)
			#raise ValueError("target not a RemotePath")
		if self.conn_details != target.conn_details:
			raise ValueError("netloc mismatch")
		try:
			RemotePath.sftp_connections[self.conn_details].remove(str(target))
		except FileNotFoundError:
			pass
		return self.rename(str(target))

	def samefile(self, target:str|os.PathLike[str]) -> bool:
		if not isinstance(target, RemotePath):
			target = type(self)(str(target), self.conn_details)
			#raise ValueError("target not a RemotePath")
		if self.conn_details != target.conn_details:
			# TODO need to consider if self netloc is localhost
			return False
		return self.resolve() == target.resolve()

	def relative_to(self, target:str|os.PathLike[str]) -> "RemotePath":
		if not isinstance(target, RemotePath):
			target = type(self)(str(target), self.conn_details)
			#raise ValueError("target not a RemotePath")
		if self.conn_details != target.conn_details:
			raise ValueError("netloc mismatch")
		new_path_obj = posixpath.relpath(str(self), str(target))
		return type(self)(new_path_obj, self.conn_details)

	def is_relative_to(self, target:str|os.PathLike[str]) -> bool:
		if not isinstance(target, RemotePath):
			target = type(self)(str(target), self.conn_details)
			#raise ValueError("target not a RemotePath")
		if self.conn_details != target.conn_details:
			return False
		return PurePath(self).is_relative_to(target)

class RemotePathScanner:
	def __init__(self, path:RemotePath):
		connection = RemotePath.sftp_connections[path.conn_details]
		#true_path = connection.normalize(path) # TODO not sure if this is needed
		self.entries = (path / d for d in connection.listdir(str(path)))

	def __iter__(self):
		return self

	def __next__(self):
		return self.entries.__next__()

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		pass
