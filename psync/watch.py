# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

import os
from pathlib import Path
from typing import TYPE_CHECKING

from .config import _SyncConfig
from .filter import PathFilter, AllFilter
from .log import logger
from .errors import UnsupportedOperationError

if TYPE_CHECKING:
	from .core import Sync

try:
	from watchdog.observers import Observer
	from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileDeletedEvent, FileModifiedEvent, FileMovedEvent
except ImportError:
	pass

class _LocalWatcher(FileSystemEventHandler):

	def __init__(self, sync: "Sync"):
		if "Observer" not in globals():
			raise ImportError("Watchdog package is needed to watch for filesystem changes. Install it with: pip install watchdog")
		self.sync = sync
		self.base_filter = sync.filter

	def watch(self):
		logger.info(f"Watching: {self.sync.src} -> {self.sync.dst}")
		logger.info( "Press CTRL-C to quit.")
		logger.info( "---------------------")
		observer = Observer()
		if isinstance(self.sync.src, Path):
			observer.schedule(self, str(self.sync.src), recursive=True)
		else:
			raise UnsupportedOperationError("Can only watch local directories.")
		observer.start()
		try:
			while observer.is_alive():
				observer.join(1)
		finally:
			observer.stop()
			observer.join()

	def on_created(self, event):
		logger.debug(f"{type(event)=} {event.src_path=}")
		relpath = os.path.relpath(event.src_path, self.sync.src)
		if isinstance(event, FileCreatedEvent):
			new_filter = AllFilter(
				self.base_filter,
				PathFilter(is_glob=False).allow(relpath),
			)
			self.sync.filter = new_filter
			self.sync.run()
		else:
			new_filter = AllFilter(
				self.base_filter,
				PathFilter().allow(relpath, is_glob=False, is_dir=True).allow("./**/*", is_glob=True),
			)
			self.sync.filter = new_filter
			self.sync.run()

	def on_deleted(self, event):
		logger.debug(f"{type(event)=} {event.src_path=}")
		relpath = os.path.relpath(event.src_path, self.sync.src)
		# Bug: Watchdog is currently raising a FileDeletedEvent even when directories are deleted.
		# Until this is fixed, the correct operation is determined through the type of the dst file.
		dst_path = os.path.join(self.sync.dst, relpath)
		if os.path.isfile(dst_path):
			if self.sync.delete_files or self.sync.trash:
				new_filter = AllFilter(
					self.base_filter,
					PathFilter(is_glob=False).allow(relpath),
				)
				self.sync.filter = new_filter
				self.sync.run()
		elif os.path.isdir(dst_path):
			if self.sync.delete_files or self.sync.trash:
				new_filter = AllFilter(
					self.base_filter,
					PathFilter().allow(relpath, is_glob=False, is_dir=True).allow("./**/*", is_glob=True),
				)
				self.sync.filter = new_filter
				self.sync.run()

	def on_modified(self, event):
		logger.debug(f"{type(event)=} {event.src_path=}")
		relpath = os.path.relpath(event.src_path, self.sync.src)
		if isinstance(event, FileModifiedEvent):
			new_filter = AllFilter(
				self.base_filter,
				PathFilter(is_glob=False).allow(relpath),
			)
			self.sync.filter = new_filter
			self.sync.run()
		else:
			pass

	def on_moved(self, event):
		logger.debug(f"{type(event)=} {event.src_path=} {event.dest_path=}")
		src_relpath = os.path.relpath(event.src_path, self.sync.src)
		dst_relpath = os.path.relpath(event.dest_path, self.sync.src)
		if isinstance(event, FileMovedEvent):
			new_filter = AllFilter(
				self.base_filter,
				PathFilter(is_glob=False).allow(src_relpath, dst_relpath),
			)
			self.sync.filter = new_filter
			self.sync.run()
		else:
			if self.sync.delete_files or self.sync.trash:
				new_filter = AllFilter(
					self.base_filter,
					PathFilter().allow(
						src_relpath,
						dst_relpath,
						is_glob=False,
						is_dir = True,
					).allow(
						"./**/*",
						is_glob=True,
					),
				)
				self.sync.filter = new_filter
				self.sync.run()
