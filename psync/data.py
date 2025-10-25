from dataclasses import dataclass, field
from typing import Final
from pathlib import Path

from .sftp import RemotePath

@dataclass(frozen=True)
class _Metadata:
	'''File metadata that will be used to find probable duplicates.'''

	size  : int
	mtime : float

@dataclass(frozen=True)
class _ScandirEntry:
	'''Filesystem entries yielded by `_scandir()`.'''

	class Category:
		EMPTY_DIR : Final[str] = "Empty Dir"
		FILE      : Final[str] = "File"

	# mypy gives an error for some reason
	#category : Literal[Category.EMPTY_DIR, Category.FILE]
	category : str
	normpath : str # normcased and replaced \\ -> /
	path     : str
	meta     : _Metadata # TODO: _Metadata | None

@dataclass(frozen=True)
class _Operation:
	'''Filesystem operation yielded by `_operations()`.'''

	class Category:
		CREATE_FILE : Final[str] = "+"
		UPDATE_FILE : Final[str] = "U"
		RENAME_FILE : Final[str] = "R"
		DELETE_FILE : Final[str] = "-"
		TRASH_FILE  : Final[str] = "~"

		CREATE_DIR : Final[str] = "D+"
		DELETE_DIR : Final[str] = "D-"

	# mypy gives an error for some reason
	#category  : Literal[Category.CREATE_FILE, Category.UPDATE_FILE, Category.RENAME_FILE, Category.DELETE_FILE, Category.TRASH_FILE, Category.CREATE_DIR, Category.DELETE_DIR]
	category  : str
	src       : Path | RemotePath | None
	dst       : Path | RemotePath | None
	byte_diff : int
	summary   : str

@dataclass
class Results:
	'''Various statistics and other information returned by `sync()`.'''

	class Status:
		PENDING              : Final[str] = "Pending"
		COMPLETED            : Final[str] = "Completed"
		INPUT_ERROR          : Final[str] = "Input Error"
		CONNECTION_ERROR     : Final[str] = "Connection Error"
		INTERRUPTED_BY_USER  : Final[str] = "Interrupted by User"
		INTERRUPTED_BY_ERROR : Final[str] = "Interrupted by Error"

	trash_root : Path | RemotePath | None = None
	log_file   : Path | RemotePath | None = None

	# mypy gives an error for some reason
	#status     : Literal[Status.PENDING, Status.COMPLETED, Status.INPUT_ERROR, Status.CONNECTION_ERROR, Status.INTERRUPTED_BY_USER, Status.INTERRUPTED_BY_ERROR] = Status.PENDING
	status     : str = Status.PENDING
	errors     : list[str] = field(default_factory=list)

	rename_success : int = 0
	delete_success : int = 0
	trash_success  : int = 0
	create_success : int = 0
	update_success : int = 0

	rename_error : int   = 0
	delete_error : int   = 0
	trash_error  : int   = 0
	create_error : int   = 0
	update_error : int   = 0

	byte_diff : int      = 0

	dir_delete_success : int = 0
	dir_create_success : int = 0

	dir_delete_error : int   = 0
	dir_create_error : int   = 0

	@property
	def err_count(self) -> int:
		return self.rename_error + self.delete_error + self.trash_error + self.create_error + self.update_error + self.dir_delete_error + self.dir_create_error