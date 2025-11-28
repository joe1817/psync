# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import os
import io
import time
import hashlib
from pathlib import Path

from psync import RemotePath

class TempLoggingLevel:
	def __init__(self, logger, level):
		self.logger = logger
		self.level = level
	def __enter__(self):
		self.old_level = self.logger.level
		self.logger.setLevel(self.level)
	def __exit__(self, exc_type, exc_val, exc_tb):
		self.logger.setLevel(self.old_level)

def hash_directory(root:Path, *, follow_symlinks=False, ignore_empty_dirs=False, verbose=False, include_mtime=False):
	if isinstance(root, RemotePath):
		join = lambda x, y: x / y
		walk = RemotePath.walk
		normcase = lambda x: str(x).lower() if RemotePath.sep(x) == "\\" else str(x)
		relative_to = lambda x,y: x.relative_to(y)
		stat = lambda x: x.stat()
		islink = lambda x: x.is_symlink()
	else:
		join = os.path.join
		walk = os.walk
		normcase = os.path.normcase
		relative_to = os.path.relpath
		stat = os.stat
		islink = os.path.islink
	if verbose:
		print("--- Hash Start ---")
	hasher = hashlib.sha256()
	for dir, dirnames, filenames in walk(root, followlinks=bool(follow_symlinks)):
		if ignore_empty_dirs and not filenames:
			continue
		dirnames.sort(key=lambda x: (normcase(x), x))
		filenames.sort(key=lambda x: (normcase(x), x))
		dir_relpath = normcase(relative_to(dir, root))
		hasher.update(dir_relpath.encode())
		if verbose:
			print(" "*dir.count(os.sep) + dir_relpath)
		for file in filenames:
			file_path = join(dir, file)
			file_relpath = str(normcase(relative_to(file_path, root)))
			hasher.update(file_relpath.encode())
			if include_mtime:
				mtime = str(int(stat(file_path).st_mtime)) # SFTP returns mtime as int
				hasher.update(mtime.encode())
			if verbose:
				print(" "*dir.count(os.sep) + file_relpath)
				if include_mtime:
					print(" "*dir.count(os.sep) + mtime)
			try:
				if not follow_symlinks and islink(file_path):
					target = readlink(file_path) or ""
					if verbose:
						print(" "*dir.count(os.sep) + target)
					target = target.replace("\\", "/") # not perfect, but good enough
					hasher.update(target.encode()) # can't set mtime for symlinks on Windows, just ignore it here
				else:
					with open(file_path, "rb") as f:
						while True:
							buf = f.read(4096)
							if not buf:
								break
							hasher.update(buf)
							if verbose:
								print(buf)
			except OSError as e:
				print(f"Error hashing {file_path}: {e}")
	if verbose:
		print("--- Hash End ---")
	return hasher.hexdigest()

def create_file_structure(root:Path|RemotePath, structure:dict, *, _symlinks:dict|None = None):
	'''Recursively creates a directory structure with files.'''
	if isinstance(root, RemotePath):
		utime = lambda x, times: RemotePath.sftp_connections[x.netloc].utime(str(x), times)
		symlink = lambda x, y: RemotePath.sftp_connections[y.netloc].symlink(str(x), str(y))
	else:
		utime = os.utime
		symlink = os.symlink
	root.mkdir(parents=True, exist_ok=True)
	if _symlinks is not None:
		symlinks = _symlinks
	else:
		symlinks = {}
	for name, content in structure.items():
		file_path = root / name
		if isinstance(content, (Path, RemotePath)):
			# create symlink
			symlinks[file_path] = content
		elif isinstance(content, dict):
			# create dir
			create_file_structure(file_path, content, _symlinks=symlinks)
		elif type(content) in (float, int):
			file_path.touch()
			mtime = float(content)
			utime(file_path, (mtime, mtime))
		elif isinstance(content, (tuple, list)):
			# Create file with modtime and content
			file_path.write_text(content[0] or "")
			mtime = float(content[1])
			utime(file_path, (mtime, mtime))
		elif content is None:
			# Create an empty file
			file_path.touch()
		else:
			# Create a file with content
			file_path.write_text(content)
	# On Windows, symlink type will be assumed to be "File" if the target does not exist
	# So, create symlinks after everything else
	if _symlinks is None:
		for path, target in symlinks.items():
			symlink(target, path)

def readlink(path:str|os.PathLike) -> str|None:
	link = RemotePath.readlink(path) if isinstance(path, RemotePath) else os.readlink(path)
	if link is None:
		return None
	if os.name == "nt" and (link.startswith("\\\\?\\") or link.startswith("\\??\\")):
		return link[4:]
	return link
