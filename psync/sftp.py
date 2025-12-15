# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import os
import sys
import stat
import time
import tempfile
import posixpath
import socket
from getpass import getpass
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

from .log import logger
from .errors import MetadataUpdateError

class RemotePath:
	'''A class that mimics a Path object while operating on the filesystem of a remote SFTP server.'''

	# environment variables
	HOSTNAME = "HOSTNAME"
	PORT     = "PORT"
	USERNAME = "USERNAME"
	PASSWORD = "PASSWORD"

	# netloc keys
	ssh_connections  : dict[str, paramiko.client.SSHClient] = {}
	sftp_connections : dict[str, paramiko.sftp_client.SFTPClient] = {}
	# hostname keys
	os_names         : dict[str, str] = {}

	@classmethod
	def create(cls, url:str, timeout=10) -> "RemotePath":
		'''Factory method for creating new `RemotePath` objects. This is the way to create new RemotePath objects outside this module. The `url` must be a 'sftp://' or 'ftp://' protocol or none at all.'''

		if "paramiko" not in globals():
			raise ImportError("Paramiko package is needed for SFTP connections. Install it with: pip install paramiko")

		if not url.startswith("ftp://") and not url.startswith("sftp://"):
			url = "sftp://" + url
		parsed = urlparse(url)
		if parsed.hostname is None or parsed.username is None:
			raise ValueError("Malformed URI")

		if parsed.netloc not in cls.ssh_connections:
			hostname = parsed.hostname or os.getenv(cls.HOSTNAME, "")
			port     = parsed.port     or int(os.getenv(cls.PORT, 0)) or 22
			username = parsed.username or os.getenv(cls.USERNAME)
			password = parsed.password or os.getenv(cls.PASSWORD) or getpass(f"Password for {parsed.username}@{parsed.hostname}: ")

			ssh = paramiko.SSHClient()
			ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

			try:
				logger.info(f"Connecting to {parsed.username}@{parsed.hostname}…")
				ssh.connect(
					hostname,
					port     = port,
					username = username,
					password = password,
					timeout  = timeout,
				)
			except socket.gaierror as e:
				raise ConnectionError("Invalid hostname.") from e
			except paramiko.ssh_exception.AuthenticationException as e:
				raise ConnectionError(str(e)) from e
			except TimeoutError as e:
				raise ConnectionError("Host timeout.") from e
			except PermissionError as e:
				#raise ConnectionError("Access denied.") from e
				raise ConnectionError(str(e)) from e
			cls.ssh_connections[parsed.netloc] = ssh

			ftp = ssh.open_sftp()
			cls.sftp_connections[parsed.netloc] = ftp

		path = parsed.path or "/"
		if path.startswith("/~"):
			# expand ~ to user's home directory
			# TODO implement for Windows server too
			parts = path.split("/")
			user  = parts[1][1:]
			ssh   = cls.ssh_connections[parsed.netloc]
			try:
				if user == "":
					stdin, stdout, stderr = ssh.exec_command("echo $HOME")
				else:
					stdin, stdout, stderr = ssh.exec_command(f"getent passwd {user} | cut -d: -f6")
				stdout_output = stdout.read() # drain buffers
				stderr_output = stderr.read()
				exit_status = stdout.channel.recv_exit_status()
				if exit_status:
					raise paramiko.ssh_exception.SSHException()
				remote_home_dir = stdout_output.decode("utf-8").strip()
			except paramiko.ssh_exception.SSHException as e:
				raise ConnectionError(f"Unable to expand ~ to user's home directory.") from e
			if not remote_home_dir:
				raise ConnectionError(f"User not found.")
			else:
				path = posixpath.join(remote_home_dir, *parts[2:])
		elif path.startswith("/\\~"):
			# treat escaped ~ character as literal
			path = "/" + path[2:]

		path = cls.sftp_connections[parsed.netloc].normalize(path)
		return cls(path, parsed.netloc)

	@classmethod
	def get_netlocs_from_hostname(cls, hostname: str):
		'''Returns the first connection with the given hostname.'''

		for netloc in cls.ssh_connections:
			if netloc == hostname or netloc.endswith(f"@{hostname}") or f"@{hostname}:" in netloc:
				yield netloc

	@classmethod
	def os_name(cls, hostname: str) -> str:
		'''Get the server's os name. Currently returns either "nt" or "posix".'''

		try:
			os_name = cls.os_names[hostname]
		except KeyError:
			netloc = next(cls.get_netlocs_from_hostname(hostname))
			ssh = cls.ssh_connections[netloc]
			stdin, stdout, stderr = ssh.exec_command("uname -a")
			stdout_output = stdout.read() # drain buffers
			stderr_output = stderr.read()
			exit_status = stdout.channel.recv_exit_status()
			os_name = "nt" if exit_status else "posix"
			cls.os_names[hostname] = os_name
		return os_name

	@classmethod
	def sep(cls, hostname: str) -> str:
		'''Get the path separator used on the system.'''

		return "\\" if cls.os_name(hostname) == "nt" else "/"

	@classmethod
	def shutdown(cls, hostname: str) -> None:
		'''Command server to shut down (power off).'''

		netlocs = cls.get_netlocs_from_hostname(hostname)
		try:
			netloc = next(netlocs)
			ssh = cls.ssh_connections[netloc]
			# Wait 1 minute so exit status can be read.
			if cls.os_name == "nt":
				stdin, stdout, stderr = ssh.exec_command("shutdown /s /t 60")
			else:
				password = os.getenv(cls.PASSWORD) or getpass(f"Password for {netloc}: ")
				# Sync to clear disk cache.
				command = f"echo {password} | sudo -S sync && echo {password} | sudo -S shutdown -h +1"
				stdin, stdout, stderr = ssh.exec_command(command)
			stdout_output = stdout.read() # drain buffers
			stderr_output = stderr.read()
			exit_status = stdout.channel.recv_exit_status()
			if exit_status:
				logger.error(f"Could not shut down system: {hostname}")
				logger.debug(f"{exit_status=}")
				logger.debug(stderr_output.decode("utf-8").strip())
			else:
				logger.info(f"Shutting down system in 1 minute: {hostname}")
				# Forget all connections to the shut down host.
				del cls.sftp_connections[netloc]
				del cls.ssh_connections[netloc]
				for n in netlocs:
					del cls.sftp_connections[n]
					del cls.ssh_connections[n]

		except (EOFError, paramiko.SSHException):
			logger.error(f"Could not shut down system: {hostname}")
		except StopIteration:
			# assume it was already shut down
			pass

	@classmethod
	def close_connections(cls) -> None:
		'''Close all SSH and SFTP connections.'''

		for ftp in cls.sftp_connections.values():
			try:
				ftp.close()
			except:
				pass
		for ssh in cls.ssh_connections.values():
			try:
				ssh.close()
			except:
				pass
		cls.sftp_connections = {}
		cls.ssh_connections = {}

	@classmethod
	def copy_file(cls, src, dst, *, follow_symlinks:bool) -> None:
		'''Copy a file where at least one of `src` and `dst` is a `RemotePath` object.'''

		if isinstance(src, RemotePath) and isinstance(dst, RemotePath):
			if src.netloc == dst.netloc and cls.os_names[src.hostname] == "posix":
				try:
					ssh = cls.ssh_connections[src.netloc]
					# TODO implement for Windows too
					stdin, stdout, stderr = ssh.exec_command(f"cp {'' if follow_symlinks else '-P'} {str(src)} {str(dst)}")
					stdout_output = stdout.read() # drain buffers
					stderr_output = stderr.read()
					exit_status = stdout.channel.recv_exit_status()
					if exit_status:
						raise OSError(1, stderr_output.decode().strip(), str(src))
				except paramiko.ssh_exception.SSHException:
					raise OSError(1, "Connection error", str(src))
			else:
				try:
					temp_file_path = None
					with tempfile.NamedTemporaryFile(delete=False, mode='w+') as temp_file:
						temp_file_path = Path(temp_file.name)
					cls._get_file(src, temp_file_path, follow_symlinks=follow_symlinks)
					cls._put_file(temp_file_path, dst, follow_symlinks=follow_symlinks)
				finally:
					if temp_file_path:
						os.unlink(temp_file_path)
		elif isinstance(src, RemotePath):
			cls._get_file(src, dst, follow_symlinks=follow_symlinks)
		elif isinstance(dst, RemotePath):
			cls._put_file(src, dst, follow_symlinks=follow_symlinks)
		else:
			raise ValueError("At least one path must be a 'RemotePath'.")

	@classmethod
	def _get_file(cls, src:"RemotePath", dst:Path, *, follow_symlinks:bool) -> None:
		'''Download `src` file to `src`.'''

		connection = cls.sftp_connections[src.netloc]
		st = src.stat(follow_symlinks=follow_symlinks)
		if follow_symlinks or not src.is_symlink():
			connection.get(str(src), str(dst))
		else:
			target = connection.readlink(str(src))
			if target:
				os.symlink(target, str(dst), target_is_directory=src.is_dir())
			else:
				raise OSError(f"Broken symlink: {src}")

		if st.st_atime is not None and st.st_mtime is not None:
			if follow_symlinks or not src.is_symlink():
				os.utime(str(dst), (st.st_atime, st.st_mtime))
			else:
				try:
					os.utime(str(dst), (st.st_atime, st.st_mtime), follow_symlinks=False)
				except NotImplementedError:
					raise MetadataUpdateError(f"Could not update time metadata: {dst}")
		else:
			raise MetadataUpdateError(f"Could not update time metadata: {dst}")

	@classmethod
	def _put_file(cls, src:Path, dst:"RemotePath", *, follow_symlinks:bool) -> None:
		'''Upload `src` file to `dst`.'''

		connection = cls.sftp_connections[dst.netloc]
		st = src.stat(follow_symlinks=follow_symlinks)
		if follow_symlinks or not src.is_symlink():
			connection.put(str(src), str(dst))
		else:
			target = os.readlink(str(src))
			if os.sep == "\\" and (target.startswith("\\\\?\\") or target.startswith("\\??\\")):
				target = target[4:]
			if target:
				connection.symlink(target, str(dst))
			else:
				raise OSError(f"Broken symlink: {src}")

		cls._utime(dst, st=st, follow_symlinks=follow_symlinks)

	@classmethod
	def readlink(cls, src:"RemotePath") -> str|None:
		'''Read the target of a `RemotePath` symlink.'''

		connection = cls.sftp_connections[src.netloc]
		return connection.readlink(str(src))

	@classmethod
	def symlink(cls, target:str, dst:"RemotePath") -> None:
		'''Create a symlink at `dst` targeting `target`.'''

		if target:
			connection = cls.sftp_connections[dst.netloc]
			connection.symlink(target, str(dst))
		else:
			raise OSError(f"Broken symlink: {dst}")

	@classmethod
	def _utime(cls, dst:"RemotePath", *, st, follow_symlinks:bool) -> None:
		'''Update the mtime of a `RemotePath` file from the mtime of a stat object.'''

		if st.st_atime is None or st.st_mtime is None:
			raise MetadataUpdateError(f"Could not update time metadata: {dst}")

		if follow_symlinks:
			connection = cls.sftp_connections[dst.netloc]
			connection.utime(str(dst), (st.st_atime, st.st_mtime))
		else:
			mtime_epoch = int(st.st_mtime)
			new_mtime = time.strftime("%Y%m%d%H%M.%S", time.localtime(mtime_epoch))
			# touch -h changes the link times, not the target
			command = f"touch -h -m -t {new_mtime} {str(dst)}" # TODO no simple Windows equivalent (of course)
			ssh = cls.ssh_connections[dst.netloc]
			stdin, stdout, stderr = ssh.exec_command(command)
			stdout_output = stdout.read() # drain buffers
			stderr_output = stderr.read()
			exit_status = stdout.channel.recv_exit_status()
			if exit_status:
				raise MetadataUpdateError(f"Could not update time metadata: {dst}")

	@classmethod
	def walk(cls, top:"RemotePath", followlinks:bool = False) -> Iterator[tuple["RemotePath", list["RemotePath"], list["RemotePath"]]]:
		'''Walk a `RemotePath` directory.'''

		stack        : list["RemotePath"] = [top]
		visited_dirs : set[str]  = set()

		assert not isinstance(top, str)

		while stack:
			top = stack.pop()

			dirs    = []
			nondirs = []

			try:
				with _RemotePathScanner(top) as entries:
					for entry in entries:
						try:
							if followlinks:
								is_dir = entry.is_dir(follow_symlinks=True) or (hasattr(entry, "is_junction") and entry.is_junction())
							else:
								is_dir = entry.is_dir(follow_symlinks=False)
						except OSError as e:
							continue
						if is_dir:
							dirs.append(entry)
						else:
							nondirs.append(entry)
			except OSError as e:
				# top does not exist or user has no read access
				continue

			yield top, dirs, nondirs

			# Traverse into sub-directories
			for dir in reversed(dirs):
				# in case dir symlink status changed after yield
				new_path = top / dir.name
				if followlinks or not new_path.is_symlink():
					stack.append(new_path)

	connection:paramiko.sftp_client.SFTPClient

	def __init__(self, path:str, netloc:str):
		'''Initialize a `RemotePath` object.'''

		path = str(path)
		if path != "/":
			path = path.rstrip("/")
		parts = posixpath.splitext(path)
		self.path     : str = path
		self.netloc   : str = netloc
		self.hostname : str = netloc.split("@")[-1]
		self.stem     : str = parts[0].split("/")[-1]
		self.suffix   : str = parts[1]
		self.name     : str = self.stem + self.suffix
		self._stat    : paramiko.sftp_attr.SFTPAttributes|None = None
		self._lstat   : paramiko.sftp_attr.SFTPAttributes|None = None

	@property
	def parent(self) -> "RemotePath":
		'''Returns a `RemotePath` object of the parent directory.'''

		new_path_obj = posixpath.dirname(self.path) # this is why self.path cannot end with trailing slash
		return type(self)(new_path_obj, self.netloc)

	def __truediv__(self, other:str) -> "RemotePath":
		if not isinstance(other, str):
			return NotImplemented
		new_path_obj = self.path + "/" + str(other)
		return type(self)(new_path_obj, self.netloc)

	def __eq__(self, other:object) -> bool:
		if not isinstance(other, RemotePath): # TODO? AbstractPath, adn return False if not RemotePath
			return NotImplemented
		if self.netloc != other.netloc:
			raise ValueError("Cannot compare RemotePaths with different SSH connections")
		return str(self) == str(other)

	def __str__(self) -> str:
		return self.path

	def __repr__(self) -> str:
		return f"{self.netloc}/{str(self)}"

	def __hash__(self) -> int:
		return hash(self.__repr__())

	def __fspath__(self) -> str:
		return self.path

	def joinpath(self, *other:str) -> "RemotePath":
		'''Append path elements to create a new `RemotePath`.'''

		#new_path_obj = self.path + "/" + "/".join(str(s) for s in other)
		new_path_obj = posixpath.join(self.path, *other)
		return type(self)(new_path_obj, self.netloc)

	def with_name(self, new_name:str) -> "RemotePath":
		'''Returns a new `RemotePath` with the `name` changed.'''

		return self.parent / new_name

	def stat(self, *, follow_symlinks:bool = True) -> paramiko.sftp_attr.SFTPAttributes:
		'''Returns a `paramiko.sftp_attr.SFTPAttributes` object for the remote file/directory.'''

		if follow_symlinks:
			if not self._stat:
				self._stat = RemotePath.sftp_connections[self.netloc].stat(str(self))
				if self._stat.st_mode is not None and not stat.S_ISLNK(self._stat.st_mode):
					self._lstat = self._stat
			assert self._stat is not None
			return self._stat
		else:
			if not self._lstat:
				self._lstat = RemotePath.sftp_connections[self.netloc].lstat(str(self))
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

		rp = type(self)(RemotePath.sftp_connections[self.netloc].normalize(str(self)), self.netloc)
		if rp._stat is None:
			rp._stat = self._lstat
		return rp

	def iterdir(self) -> Iterator["RemotePath"]:
		'''Returns an iterator of `RemotePath` objects pointing to the contents of the remote directory.'''

		for st in RemotePath.sftp_connections[self.netloc].listdir_iter(str(self), read_aheads=1):
			entry = self / st.filename
			entry._lstat = st
			yield entry

	def chmod(self, mode, *, follow_symlinks:bool = True) -> None:
		'''chmod of the remote file/directory.'''

		RemotePath.sftp_connections[self.netloc].chmod(str(self), mode)

	def read_text(self, encoding:str|None = "utf-8", errors:str|None = "strict", newline:str|None = os.linesep) -> str:
		'''Reads and returns the content of the remote file as text.'''

		if not self.is_file():
			raise FileNotFoundError(f"No such file on remote:'{self}'")

		# Create a temporary file to download the content
		with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
			tmp_path = tmp_file.name

		try:
			RemotePath.sftp_connections[self.netloc].get(str(self), tmp_path)
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
			RemotePath.sftp_connections[self.netloc].put(tmp_path, str(self))
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
			RemotePath.sftp_connections[self.netloc].get(str(self), tmp_path)
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
			RemotePath.sftp_connections[self.netloc].put(tmp_path, str(self))
		finally:
			os.remove(tmp_path)
		return 0

	def mkdir(self, mode:int = 0o777, parents:bool = False, exist_ok:bool = False) -> None:
		'''Create the remote directory.'''

		try:
			parent = self.parent
			if parents and self != parent and not parent.exists():
				parent.mkdir(mode, parents, exist_ok)
			RemotePath.sftp_connections[self.netloc].mkdir(str(self), mode)
		except IOError as e:
			if exist_ok:
				pass

	def rmdir(self) -> None:
		'''Delete the remote directory.'''

		RemotePath.sftp_connections[self.netloc].rmdir(str(self))

	def touch(self, mode = 0o666, exist_ok = True):
		conn = RemotePath.sftp_connections[self.netloc]
		file = conn.open(str(self), mode="a")
		if mode != 0o666:
			conn.chmod(mode)
		file.close()

	def unlink(self, missing_ok:bool = True) -> None:
		'''Delete the remote file.'''

		try:
			RemotePath.sftp_connections[self.netloc].remove(str(self))
		except FileNotFoundError as e:
			if not missing_ok:
				raise e

	def open(self, mode = "r", buffering = -1, encoding = None, errors = None, newline = None):
		'''Open the remote file.'''

		return RemotePath.sftp_connections[self.netloc].open(str(self), mode="r")

	def rename(self, target:"RemotePath") -> "RemotePath":
		'''Renames this file/directory to the given `target`, and return a new `Path` instance pointing to `target`.'''

		if not isinstance(target, RemotePath):
			#target = type(self)(str(target), self.netloc)
			raise TypeError(f"Expected 'target' to be a RemotePath, but received type: {type(target).__name__}")
		if self.netloc != target.netloc:
			raise ValueError("Netloc mismatch.")
		RemotePath.sftp_connections[self.netloc].posix_rename(str(self), str(target)) # atomic
		return target

	def replace(self, target:"RemotePath") -> "RemotePath":
		'''Alias for `rename()`.'''

		return self.rename(target)

	def samefile(self, target:"RemotePath") -> bool:
		'''Returns `True` if the remote file/directory and `target` are the same file.'''

		if not isinstance(target, RemotePath):
			#target = type(self)(str(target), self.netloc)
			raise TypeError(f"Expected 'target' to be a RemotePath, but received type: {type(target).__name__}")
		if self.netloc != target.netloc:
			# TODO need to consider if self netloc is localhost
			return False
		return self.resolve() == target.resolve()

	def relative_to(self, target:"RemotePath") -> "RemotePath":
		'''Returns a `RemotePath` with path string relative to `target`.'''

		if not isinstance(target, RemotePath):
			#target = type(self)(str(target), self.netloc)
			raise TypeError(f"Expected 'target' to be a RemotePath, but received type: {type(target).__name__}")
		if self.netloc != target.netloc:
			raise ValueError("Netloc mismatch.")
		new_path = posixpath.relpath(str(self), str(target))
		# Path.relative_to() returns a Path object, though a str would probably make more sense
		return type(self)(new_path, self.netloc)

	def is_relative_to(self, target:"RemotePath") -> bool:
		'''Returns `True` if the remote file/directory is relative to `target`.'''

		if not isinstance(target, RemotePath):
			#target = type(self)(str(target), self.netloc)
			raise TypeError(f"Expected 'target' to be a RemotePath, but received type: {type(target).__name__}")
		if self.netloc != target.netloc:
			return False
		return PurePosixPath(self).is_relative_to(target)

class _RemotePathScanner:
	def __init__(self, path:RemotePath):
		try:
			self.entries = path.iterdir()
		except paramiko.sftp.SFTPError as e:
			raise FileNotFoundError() from e # this is to match os.scandir

	def __iter__(self) -> Iterator[RemotePath]:
		return self.entries

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		pass
