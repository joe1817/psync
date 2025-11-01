# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import os
import sys
import stat
import tempfile
import posixpath
import socket
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse
from typing import Iterator, Union

if sys.version_info >= (3, 12):
    from collections.abc import Buffer
else:
    Buffer = Union[bytes, bytearray, memoryview]

try:
	import paramiko
except ImportError:
	pass

from .errors import MetadataUpdateError

class RemotePath:
	'''A class that mimics a Path object while operating on a remote SFTP server using a paramiko SFTPClient object.'''

	ssh_connections :dict[str, paramiko.client.SSHClient] = {}
	sftp_connections:dict[str, paramiko.sftp_client.SFTPClient] = {}

	@classmethod
	def create(cls, s:str, timeout=10) -> "RemotePath":
		'''Factory method for creating new `RemotePath` objects. This is the way to create new RemotePath objects outside this module.'''

		if "paramiko" not in globals():
			raise ImportError("Paramiko package is needed for SFTP connections. Install it with: pip install paramiko")

		if not s.startswith("ftp://") and not s.startswith("sftp://"):
			s = "sftp://" + s
		parsed = urlparse(s)
		if not parsed.hostname or not parsed.username:
			raise ValueError("Malformed URI")

		if parsed.netloc not in cls.ssh_connections:
			password = parsed.password or input(f"Password for {parsed.netloc}: ")

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
				raise ConnectionError("Invalid hostname.") from e
			except TimeoutError as e:
				raise ConnectionError("Host timeout.") from e
			except paramiko.ssh_exception.AuthenticationException as e:
				raise ConnectionError(str(e)) from e
			except PermissionError as e:
				#raise ConnectionError("Access denied.") from e
				raise ConnectionError(str(e)) from e
			cls.ssh_connections[parsed.netloc] = ssh

			ftp = ssh.open_sftp()
			cls.sftp_connections[parsed.netloc] = ftp

		path = parsed.path or "/"
		if path.startswith("/~"):
			# expand ~ to user's home directory
			parts = path.split("/")
			user  = parts[1][1:]
			ssh   = cls.ssh_connections[parsed.netloc]
			try:
				if user == "":
					stdin, stdout, stderr = ssh.exec_command("echo $HOME")
				else:
					stdin, stdout, stderr = ssh.exec_command(f"getent passwd {user} | cut -d: -f6")
				remote_home_dir = stdout.read().decode().strip()
			except paramiko.ssh_exception.SSHException:
				raise ConnectionError(f"Unable to expand ~ to user's home directory.")
			if not remote_home_dir:
				raise ConnectionError(f"User not found.")
			else:
				path = posixpath.join(remote_home_dir, *parts[2:])
		elif path.startswith("/\\~"):
			# treat escaped ~ character as literal
			path = "/" + path[2:]

		path = cls.sftp_connections[parsed.netloc].normalize(path)
		return RemotePath(path, parsed.netloc)

	@classmethod
	def close_connections(cls):
		'''Close all SSH and SFTP connections.'''

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
		cls.sftp_connections = []
		cls.ssh_connections = []

	@classmethod
	def copy_file(cls, src, dst, *, follow_symlinks:bool = False):
		'''Copy a file where at least one of `src` and `dst` is a `RemotePath` object.'''

		if isinstance(src, RemotePath) and isinstance(dst, RemotePath):
			try:
				temp_file_path = None
				with tempfile.NamedTemporaryFile(delete=False, mode='w+') as temp_file:
					temp_file_path = Path(temp_file.name)
				RemotePath._get_file(src, temp_file_path, follow_symlinks=follow_symlinks)
				RemotePath._put_file(temp_file_path, dst, follow_symlinks=follow_symlinks)
			finally:
				if temp_file_path:
					os.unlink(temp_file_path)
		elif isinstance(src, RemotePath):
			RemotePath._get_file(src, dst, follow_symlinks=follow_symlinks)
		elif isinstance(dst, RemotePath):
			RemotePath._put_file(src, dst, follow_symlinks=follow_symlinks)
		else:
			raise ValueError("At least one path must be a RemotePath")

	@classmethod
	def _get_file(cls, src:"RemotePath", dst:Path, *, follow_symlinks:bool = False):
		'''Download `src` file to `src`.'''

		connection = RemotePath.sftp_connections[src.conn_details]
		st = src.stat(follow_symlinks=follow_symlinks)
		if follow_symlinks or not src.is_symlink():
			connection.get(str(src), str(dst))
		else:
			target = connection.readlink(str(src))
			if target:
				# TODO Absolute path symlinks can be updated if src_root and dst_root are known
				#try:
				#	if posixpath.commonpath([target, src_root]):
				#		target = posixpath.join(dst_root, posixpath.relpath(target, src_root))
				#except ValueError:
				#	# target is a relative path or is on different drive from src_root
				#	pass
				if os.sep == "\\":
					if os.path.isreserved(target) or "\\" in target:
						raise OSError(f"Symlink has incompatible target: {src} -> {target}")
					else:
						target = target.replace("/", os.sep)
				os.symlink(target, str(dst), target_is_directory=src.is_dir())
			else:
				raise OSError(f"Broken symlink: {src}")
		if st.st_atime is not None and st.st_mtime is not None:
			os.utime(dst, (st.st_atime, st.st_mtime))
		else:
			raise MetadataUpdateError(f"Could not update time metadata: {src}")

	@classmethod
	def _put_file(cls, src:Path, dst:"RemotePath", *, follow_symlinks:bool = False):
		'''Upload `src` file to `dst`.'''

		connection = RemotePath.sftp_connections[dst.conn_details]
		st = src.stat(follow_symlinks=follow_symlinks)
		if follow_symlinks or not src.is_symlink():
			connection.put(str(src), str(dst))
		else:
			target = os.readlink(str(src)).replace(os.sep, "/")
			if target:
				connection.symlink(target, str(dst))
			else:
				raise OSError(f"Broken symlink: {src}")
		if st.st_atime is not None and st.st_mtime is not None:
			connection.utime(str(dst), (st.st_atime, st.st_mtime))
		else:
			raise MetadataUpdateError(f"Could not update time metadata: {src}")

	connection:paramiko.sftp_client.SFTPClient

	def __init__(self, path:str|os.PathLike[str], conn_details:str):
		path = str(path)
		if path != "/":
			path = path.rstrip("/")
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
		'''Returns a `RemotePath` object of the parent directory.'''

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
		'''Append path elements to create a new `RemotePath`.'''

		#new_path_obj = self.path + "/" + "/".join(str(s) for s in other)
		new_path_obj = posixpath.join(self.path, *other)
		return type(self)(new_path_obj, self.conn_details)

	def with_name(self, new_name:str|os.PathLike[str]) -> "RemotePath":
		'''Returns a new `RemotePath` with the `name` changed.'''

		return self.parent / new_name

	def stat(self, *, follow_symlinks:bool = True) -> paramiko.sftp_attr.SFTPAttributes:
		'''Returns a `paramiko.sftp_attr.SFTPAttributes` object for the remote file/directory.'''

		if follow_symlinks:
			if not self._stat:
				self._stat = RemotePath.sftp_connections[self.conn_details].stat(str(self))
				if self._stat.st_mode is not None and not stat.S_ISLNK(self._stat.st_mode):
					self._lstat = self._stat
			assert self._stat is not None
			return self._stat
		else:
			if not self._lstat:
				self._lstat = RemotePath.sftp_connections[self.conn_details].lstat(str(self))
				if self._lstat.st_mode is not None and not stat.S_ISLNK(self._lstat.st_mode):
					self._stat = self._lstat
			assert self._lstat is not None
			return self._lstat

	def exists(self, *, follow_symlinks:bool = True) -> bool:
		'''Returns `True` if the remote file/directory exists.'''

		try:
			st = self.stat(follow_symlinks=follow_symlinks)
			return True
		except FileNotFoundError:
			return False

	def is_symlink(self) -> bool:
		'''Returns `True` if the remote file/directory is a symlink.'''

		st = self.stat(follow_symlinks=False)
		assert st.st_mode is not None
		return stat.S_ISLNK(st.st_mode)

	# needed to mimic os.DirEntry
	def is_junction(self) -> bool:
		return False # TODO

	def is_dir(self, *, follow_symlinks:bool = True) -> bool:
		'''Returns `True` if the remote file/directory is a directory.'''

		st = self.stat(follow_symlinks=follow_symlinks)
		assert st.st_mode is not None
		return stat.S_ISDIR(st.st_mode)

	def is_file(self, *, follow_symlinks:bool=True) -> bool:
		'''Returns `True` if the remote file/directory is a regular file.'''

		st = self.stat(follow_symlinks=follow_symlinks)
		assert st.st_mode is not None
		return stat.S_ISREG(st.st_mode)

	def resolve(self, strict:bool = False) -> "RemotePath":
		'''Returns a `RemotePath` with an absolute, normalized path string.'''

		rp = type(self)(RemotePath.sftp_connections[self.conn_details].normalize(str(self)), self.conn_details)
		if rp._stat is None:
			rp._stat = self._lstat
		return rp

	def iterdir(self) -> Iterator["RemotePath"]:
		'''Returns an iterator of `RemotePath` objects pointing to the contents of the remote directory.'''

		for st in RemotePath.sftp_connections[self.conn_details].listdir_iter(str(self)):
			entry = self / st.filename
			entry._lstat = st
			yield entry

	def chmod(self, mode, *, follow_symlinks:bool = True) -> None:
		'''chmod of the remote file/directory.'''

		RemotePath.sftp_connections[self.conn_details].chmod(str(self), mode)

	def read_text(self, encoding:str|None = "utf-8", errors:str|None = "strict", newline:str|None = os.linesep) -> str:
		'''Reads and returns the content of the remote file as text.'''

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

	def write_text(self, data:str, encoding:str|None = "utf-8", errors:str|None = "strict", newline:str|None = os.linesep) -> int:
		'''Writes text content to the remote file.'''

		with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding=encoding, errors=errors) as tmp_file:
			tmp_file.write(data)
			tmp_path = tmp_file.name
		try:
			RemotePath.sftp_connections[self.conn_details].put(tmp_path, str(self))
		finally:
			os.remove(tmp_path) # Clean up the temporary file
		return 0

	def read_bytes(self) -> bytes:
		'''Reads and returns the content of the remote file as bytes.'''

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
		'''Writes byte content to the remote file.'''

		with tempfile.NamedTemporaryFile(mode="wb", delete=False) as tmp_file:
			tmp_file.write(data)
			tmp_path = tmp_file.name
		try:
			RemotePath.sftp_connections[self.conn_details].put(tmp_path, str(self))
		finally:
			os.remove(tmp_path)
		return 0

	def mkdir(self, mode:int = 0o777, parents:bool = False, exist_ok:bool = False) -> None:
		'''Create the remote directory.'''

		try:
			parent = self.parent
			if parents and self != parent and not parent.exists():
				parent.mkdir(mode, parents, exist_ok)
			RemotePath.sftp_connections[self.conn_details].mkdir(str(self), mode)
		except IOError as e:
			if exist_ok:
				pass

	def rmdir(self) -> None:
		'''Delete the remote directory.'''

		RemotePath.sftp_connections[self.conn_details].rmdir(str(self))

	def unlink(self, missing_ok:bool = True) -> None:
		'''Delete the remote file.'''

		RemotePath.sftp_connections[self.conn_details].remove(str(self))

	def open(self, mode = "r", buffering = -1, encoding = None, errors = None, newline = None):
		'''Open the remote file.'''

		return RemotePath.sftp_connections[self.conn_details].open(str(self), mode="r")

	def rename(self, target:"RemotePath") -> "RemotePath":
		'''Renames this file/directory to the given `target`, and return a new `Path` instance pointing to `target`.'''

		if not isinstance(target, RemotePath):
			#target = type(self)(str(target), self.conn_details)
			raise TypeError(f"Expected 'target' to be a RemotePath, but received type: {type(target).__name__}")
		if self.conn_details != target.conn_details:
			raise ValueError("Netloc mismatch.")
		RemotePath.sftp_connections[self.conn_details].posix_rename(str(self), str(target)) # atomic
		return target

	def replace(self, target:"RemotePath") -> "RemotePath":
		'''Alias for `rename()`.'''
		return self.rename(target)

	def samefile(self, target:"RemotePath") -> bool:
		'''Returns `True` if the remote file/directory and `target` are the same file.'''

		if not isinstance(target, RemotePath):
			#target = type(self)(str(target), self.conn_details)
			raise TypeError(f"Expected 'target' to be a RemotePath, but received type: {type(target).__name__}")
		if self.conn_details != target.conn_details:
			# TODO need to consider if self netloc is localhost
			return False
		return self.resolve() == target.resolve()

	def relative_to(self, target:"RemotePath") -> "RemotePath":
		'''Returns a `RemotePath` with path string relative to `target`.'''

		if not isinstance(target, RemotePath):
			#target = type(self)(str(target), self.conn_details)
			raise TypeError(f"Expected 'target' to be a RemotePath, but received type: {type(target).__name__}")
		if self.conn_details != target.conn_details:
			raise ValueError("Netloc mismatch.")
		new_path = posixpath.relpath(str(self), str(target))
		# Path.relative_to() returns a Path object, though a str would probably make more sense
		return type(self)(new_path, self.conn_details)

	def is_relative_to(self, target:"RemotePath") -> bool:
		'''Returns `True` if the remote file/directory is relative to `target`.'''

		if not isinstance(target, RemotePath):
			#target = type(self)(str(target), self.conn_details)
			raise TypeError(f"Expected 'target' to be a RemotePath, but received type: {type(target).__name__}")
		if self.conn_details != target.conn_details:
			return False
		return PurePosixPath(self).is_relative_to(target)

class _RemotePathScanner:
	def __init__(self, path:RemotePath):
		self.entries = path.iterdir()

	def __iter__(self):
		return self.entries

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		pass
