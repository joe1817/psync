# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import os
import sys
from pathlib import Path
from typing import Union, Protocol, TypeVar, Any, Iterator, runtime_checkable

if sys.version_info >= (3, 12):
    from collections.abc import Buffer
else:
    Buffer = Union[bytes, bytearray, memoryview]

# used to make sure _AbstractPath's methods return the same type as itself, not any other concrete class that implements _AbstractPath
T_AbstractPath = TypeVar("T_AbstractPath", bound="_AbstractPath")

class _AbstractStat(Protocol):
	'''Protocol class representing a stat-like object.'''

	@property
	def st_size(self) -> int:
		...

	@property
	def st_mode(self) -> int:
		...

	@property
	def st_atime(self) -> float:
		...

	@property
	def st_mtime(self) -> float:
		...

@runtime_checkable
class _AbstractPath(Protocol):
	'''Protocol class representing the methods a Path-like object needs to do a sync operation.'''
	
	@property
	def name(self: T_AbstractPath) -> str:
		...
	
#	@property
#	def suffix(self: T_AbstractPath) -> str:
#		...
	
#	@property
#	def stem(self: T_AbstractPath) -> str:
#		...
	
	@property
	def parent(self: T_AbstractPath) -> T_AbstractPath:
		...

	def __truediv__(self: T_AbstractPath, other: str) -> T_AbstractPath:
		...

#	def __eq__(self: T_AbstractPath, other: object) -> bool:
#		...

#	def __str__(self: T_AbstractPath) -> str:
#		...

#	def __repr__(self: T_AbstractPath) -> str:
#		...

#	def __hash__(self: T_AbstractPath) -> int:
#		...

#	def __fspath__(self: T_AbstractPath) -> str:
#		...

	def joinpath(self: T_AbstractPath, *other:str) -> T_AbstractPath:
		...

	def with_name(self: T_AbstractPath, new_name:str) -> T_AbstractPath:
		...

	def stat(self: T_AbstractPath, *, follow_symlinks:bool = True) -> _AbstractStat:
		...

	def exists(self: T_AbstractPath, *, follow_symlinks:bool = True) -> bool:
		...

	def is_symlink(self: T_AbstractPath) -> bool:
		...

#	not in PosixPath
#	def is_junction(self: T_AbstractPath) -> bool:
#		...

	def is_dir(self: T_AbstractPath, *, follow_symlinks:bool = True) -> bool:
		...

	def is_file(self: T_AbstractPath, *, follow_symlinks:bool=True) -> bool:
		...

	def resolve(self: T_AbstractPath, strict:bool = False) -> T_AbstractPath:
		...

#	def iterdir(self: T_AbstractPath) -> Iterator[T_AbstractPath]:
#		...

	def chmod(self: T_AbstractPath, mode, *, follow_symlinks:bool = True) -> None:
		...

#	def read_text(self: T_AbstractPath, encoding:str|None = "utf-8", errors:str|None = "strict", newline:str|None = os.linesep) -> str:
#		...

#	def write_text(self: T_AbstractPath, data:str, encoding:str|None = "utf-8", errors:str|None = "strict", newline:str|None = os.linesep) -> int:
#		...

#	def read_bytes(self: T_AbstractPath) -> bytes:
#		...

#	def write_bytes(self: T_AbstractPath, data:Buffer) -> int:
#		...

	def mkdir(self: T_AbstractPath, mode:int = 0o777, parents:bool = False, exist_ok:bool = False) -> None:
		...

#	def rmdir(self: T_AbstractPath) -> None:
#		...

	def unlink(self: T_AbstractPath, missing_ok:bool = True) -> None:
		...

	def open(self: T_AbstractPath, mode = "r", buffering = -1, encoding = None, errors = None, newline = None):
		...

#	def rename(self: T_AbstractPath, target: T_AbstractPath) -> T_AbstractPath:
#		...

	def replace(self: T_AbstractPath, target: T_AbstractPath) -> T_AbstractPath:
		...

#	def samefile(self: T_AbstractPath, target: T_AbstractPath) -> bool:
#		...

	def relative_to(self: T_AbstractPath, target: T_AbstractPath) -> T_AbstractPath:
		...

	def is_relative_to(self: T_AbstractPath, target: T_AbstractPath) -> bool:
		...
