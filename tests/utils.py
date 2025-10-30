import os
import io
import time
import hashlib
from pathlib import Path

class TempLoggingLevel:
	def __init__(self, logger, level):
		self.logger = logger
		self.level = level
	def __enter__(self):
		self.old_level = self.logger.level
		self.logger.setLevel(self.level)
	def __exit__(self, exc_type, exc_val, exc_tb):
		self.logger.setLevel(self.old_level)

def hash_directory(root:Path, *, follow_links:bool=False, ignore_empty_dirs:bool=False, verbose:bool=False):
	if verbose:
		print("--- Hash Start ---")
	hasher = hashlib.sha256()
	for dir, dirnames, filenames in os.walk(root, followlinks=follow_links):
		if ignore_empty_dirs and not filenames:
			continue
		dirnames.sort(key=lambda x: (os.path.normcase(x), x))
		filenames.sort(key=lambda x: (os.path.normcase(x), x))
		dir_relpath = os.path.normcase(os.path.relpath(dir, root))
		hasher.update(dir_relpath.encode())
		if verbose:
			print(dir_relpath)
		for file in filenames:
			file_path = os.path.join(dir, file)
			file_relpath = os.path.normcase(os.path.relpath(file_path, root))
			hasher.update(file_relpath.encode())
			if verbose:
				print(file_relpath)
			try:
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

def create_file_structure(root_dir:Path, structure:dict, *, _symlinks:dict|None = None, _delay:float = 0.01):
	'''Recursively creates a directory structure with files.'''
	root_dir.mkdir(parents=True, exist_ok=True)
	if _symlinks is not None:
		symlinks = _symlinks
	else:
		symlinks = {}
	for name, content in structure.items():
		file_path = root_dir / name
		if isinstance(content, Path):
			# create symlink
			symlinks[file_path] = content
		elif isinstance(content, dict):
			# create dir
			create_file_structure(file_path, content, _symlinks=symlinks, _delay=0)
		elif isinstance(content, (tuple, list)):
			# Create file with modtime and content
			file_path.write_text(content[0] or "")
			mtime = float(content[1])
			os.utime(file_path, (mtime, mtime))
			assert os.stat(file_path).st_mtime == mtime
			# Force the OS to write all metadata changes (including mtime) to disk
			#with open(file_path, "r+") as f:
			#	f.flush()
			#	os.fsync(f.fileno())
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
			os.symlink(target, path)
	if _delay:
		# delay so filesystem cache can update (changes to modtimes), TODO not sure if this actually works
		time.sleep(_delay)