import sys
import logging
from enum import Enum

def _exc_summary(e) -> str:
	'''Get a one-line summary of an `Exception`.'''

	error_type = type(e).__name__
	affected_file = getattr(e, "filename", None)
	error_message = getattr(e, "strerror", None)
	if isinstance(e, OSError) and affected_file:
		msg = f"{error_type}: {affected_file}"
	elif error_message:
		msg = f"{error_type}: {error_message}"
	else:
		msg = error_type
	return msg

class _RecordTag(Enum):
	HEADER = 1
	FOOTER = 2
	SYNC_OP = 3

	def dict(self):
		return {self.name: True}

class _DebugInfoFilter(logging.Filter):
	'''Logging filter that only allows DEBUG and INFO records to pass.'''
	def filter(self, record):
		return logging.DEBUG <= record.levelno <= logging.INFO

class _NonEmptyFilter(logging.Filter):
	'''Logging filter that only allows non-empty messages.'''
	def filter(self, record):
		return bool(str(record.msg).strip())

class _TagFilter(logging.Filter):
	'''Logging filter that does not allow messages with certain tags supplied in `extras`.'''
	def __init__(self, enabled:bool = True):
		self.enabled : bool = enabled
		self.hidden  : dict[_RecordTag, bool] = {}
	def __getitem__(self, k:_RecordTag) -> bool:
		return k in self.hidden and self.hidden[k]
	def __setitem__(self, k:_RecordTag, v) -> None:
		self.hidden[k] = v
	def filter(self, record) -> bool:
		if not self.enabled:
			return False
		return not any(self.hidden[k] and bool(getattr(record, k.name, False)) for k in self.hidden)

class _ConsoleFormatter(logging.Formatter):
	#BASE_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
	BASE_FORMAT = "%(message)s"

	def __init__(self, fmt=BASE_FORMAT, datefmt=None, style="%"):
		super().__init__(fmt, datefmt, style)

	def format(self, record):
		msg = super().format(record)
		extra_indent = "" if getattr(record, _RecordTag.SYNC_OP.name, False) else "  "
		if record.levelno == logging.DEBUG:
			msg = f"  {extra_indent}{msg.replace("\n", f"\n  {extra_indent}").rstrip(" ")}"
		else:
			msg = f"{extra_indent}{msg.replace("\n", f"\n{extra_indent}").rstrip(" ")}"
		return msg

class _LogFileFormatter(logging.Formatter):
	#BASE_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
	BASE_FORMAT = "%(message)s"

	def __init__(self, fmt=BASE_FORMAT, datefmt=None, style="%"):
		super().__init__(fmt, datefmt, style)

	def format(self, record):
		msg = super().format(record)
		if record.levelno == logging.DEBUG:
			msg = f"  {msg.replace("\n", "\n  ").rstrip(" ")}"
		elif record.levelno == logging.INFO:
			pass
		elif record.levelno == logging.WARNING:
			msg = f"WARNING: {msg}"
		elif record.levelno == logging.ERROR:
			msg = f"ERROR: {msg}"
		elif record.levelno == logging.CRITICAL:
			msg = f"*** CRITICAL ***: {msg}"
		return msg

logger = logging.getLogger("psync")
logger.setLevel(logging.INFO)
handler_stdout = logging.StreamHandler(sys.stdout)
handler_stderr = logging.StreamHandler(sys.stderr)
handler_stdout.addFilter(_DebugInfoFilter())
handler_stdout.setLevel(logging.DEBUG)
handler_stderr.setLevel(logging.WARNING)
logger.addHandler(handler_stdout)
logger.addHandler(handler_stderr)
