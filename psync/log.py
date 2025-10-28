import sys
import logging
from enum import Enum

class _RecordTag(Enum):
	HEADER = 1
	FOOTER = 2

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

class _LogFileFormatter(logging.Formatter):
	#BASE_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
	BASE_FORMAT = "%(message)s"

	def __init__(self, fmt=BASE_FORMAT, datefmt=None, style="%"):
		super().__init__(fmt, datefmt, style)

	def format(self, record):
		original_message = record.msg
		if record.levelno == logging.DEBUG:
			record.msg = f"\t{original_message}"
		elif record.levelno == logging.INFO:
			pass
		elif record.levelno == logging.WARNING:
			record.msg = f"WARN: {original_message}"
		elif record.levelno == logging.ERROR:
			record.msg = f"ERROR: {original_message}"
		elif record.levelno == logging.CRITICAL:
			record.msg = f"\n*** {original_message} ***\n"
		formatted_message = super().format(record)
		return formatted_message

logger = logging.getLogger("psync")
logger.setLevel(logging.INFO)
handler_stdout = logging.StreamHandler(sys.stdout)
handler_stderr = logging.StreamHandler(sys.stderr)
handler_stdout.addFilter(_DebugInfoFilter())
handler_stdout.setLevel(logging.DEBUG)
handler_stderr.setLevel(logging.WARNING)
logger.addHandler(handler_stdout)
logger.addHandler(handler_stderr)
