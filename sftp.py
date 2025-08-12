# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import os
import stat
import paramiko
import tempfile
from pathlib import PurePath
from urllib.parse import urlparse
from collections.abc import Buffer
from typing import Generator

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
		if not s.startswith("ftp://") and not s.startswith("sftp://"):
			s = "sftp://" + s
		parsed = urlparse(s)
		if not parsed.hostname or not parsed.username:
			raise ValueError("Malformed URI")

		if parsed.netloc not in cls.ssh_connections:
			password = input(f"Password for {parsed.netloc}: ")

			ssh = paramiko.SSHClient()
			ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

			ssh.connect(
				parsed.hostname,
				port     = parsed.port or 22,
				username = parsed.username,
				password = password,
				timeout  = timeout,
			)
			cls.ssh_connections[parsed.netloc] = ssh

			ftp = ssh.open_sftp()
			cls.sftp_connections[parsed.netloc] = ftp

		return RemotePath(parsed.path, parsed.netloc)

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

	connection:paramiko.sftp_client.SFTPClient

	def __init__(self, path:str|os.PathLike[str], conn_details:str):
		parts = os.path.splitext(path)
		self.path         : str = str(path)
		self.conn_details : str = conn_details
		self.name         : str = os.path.basename(self.path)
		self.stem         : str = parts[0].replace("\\", "/").split("/")[-1]
		self.suffix       : str = parts[1]
		self._stat        : paramiko.sftp_attr.SFTPAttributes|None = None

	@property
	def parent(self) -> "RemotePath":
		new_path_obj = os.path.dirname(self.path)
		return type(self)(new_path_obj, self.conn_details)

	def __truediv__(self, other:str|os.PathLike[str]) -> "RemotePath":
		new_path_obj = os.path.join(self.path, other)
		return type(self)(new_path_obj, self.conn_details)

	def __eq__(self, other:object) -> bool:
		if not isinstance(other, RemotePath):
			return NotImplemented
		assert isinstance(other, RemotePath)
		return self.conn_details == other.conn_details and str(self) == str(other)

	def __str__(self):
		return str(self.path)

	def __fspath__(self):
		return str(self.path)

	def joinpath(self, *other:str|os.PathLike[str]) -> "RemotePath":
		"""
		Overrides the joinpath method to return a new "RemotePath" object.
		"""
		new_path_obj = os.path.join(self.path, *other)
		return type(self)(new_path_obj, self.conn_details)

	def with_name(self, new_name) -> "RemotePath":
		return self.parent / new_name

	def stat(self, *, follow_symlinks=True) -> paramiko.sftp_attr.SFTPAttributes:
		if not self._stat:
			if follow_symlinks:
				self._stat = RemotePath.sftp_connections[self.conn_details].stat(str(self))
			else:
				self._stat = RemotePath.sftp_connections[self.conn_details].lstat(str(self))
		assert self._stat is not None
		return self._stat

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
		return type(self)(RemotePath.sftp_connections[self.conn_details].normalize(str(self)), self.conn_details)

	def iterdir(self) -> Generator["RemotePath"]:
		for item_name in RemotePath.sftp_connections[self.conn_details].listdir(str(self)):
			# Yield a new "RemotePath" object for each item,
			# ensuring it also carries the connection.
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

	def rename(self, target:str|os.PathLike[str]) -> "RemotePath":
		RemotePath.sftp_connections[self.conn_details].rename(str(self), str(target))
		if isinstance(target, RemotePath):
			return target
		else:
			return type(self)(target, self.conn_details)

	def replace(self, target:str|os.PathLike[str]) -> "RemotePath":
		try:
			RemotePath.sftp_connections[self.conn_details].remove(str(target))
		except FileNotFoundError:
			pass
		return self.rename(str(target))

	def open(self, mode="r", buffering=-1, encoding=None, errors=None, newline=None):
		return RemotePath.sftp_connections[self.conn_details].open(str(self), mode="r", buffering=-1)

	def samefile(self, other:str|os.PathLike[str]) -> bool:
		if isinstance(other, RemotePath):
			if self.conn_details != other.conn_details:
				return False
			else:
				# Path.samefile compares dev & inode, but this info is not available over SFTP
				return self.resolve() == other.resolve()
		else:
			# TODO need to consider if netloc is localhost
			return False

	def relative_to(self, other:str|os.PathLike[str]) -> "RemotePath":
		if isinstance(other, RemotePath):
			if self.conn_details != other.conn_details:
				raise ValueError("netloc mismatch")
		new_path_obj = os.path.relpath(str(self), str(other))
		return type(self)(new_path_obj, self.conn_details)

	def is_relative_to(self, other:str|os.PathLike[str]) -> bool:
		if isinstance(other, RemotePath):
			if self.conn_details != other.conn_details:
				return False
		return PurePath(self).is_relative_to(other)