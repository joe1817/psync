import sys
import os
import logging
from enum import Enum

# Summary of logging levels used in this package:
# CRITICAL = Exception raised which halted the program entirely
# ERROR    = problem encountered and the operation failed
# WARNING  = problem encountered but the operation completed
# INFO     = operation performed, no problems encountered
# DEBUG    = useful for finding bugs

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
		msg = str(e)
	return msg

class _DebugInfoFilter(logging.Filter):
	'''Logging filter that only allows DEBUG and INFO records to pass.'''

	def filter(self, record):
		return logging.DEBUG <= record.levelno <= logging.INFO

class _NonEmptyFilter(logging.Filter):
	'''Logging filter that only allows non-empty messages.'''

	def filter(self, record):
		return bool(str(record.msg).strip())

class _Formatter(logging.Formatter):
	'''Logging formatter for records printed to the console.'''

	#BASE_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
	BASE_FORMAT = "%(message)s"

	def __init__(self, fmt=BASE_FORMAT, datefmt=None, style="%"):
		super().__init__(fmt, datefmt, style)

	def format(self, record):
		msg = super().format(record)
		extra_indent = "" if getattr(record, "Operation", None) else "  "
		if record.levelno == logging.DEBUG:
			msg = extra_indent + msg.replace("\n", f"\n  {extra_indent}").rstrip(" ")
		else:
			msg = extra_indent + msg.replace("\n", f"\n{extra_indent}").rstrip(" ")
		return msg

class _RichConsoleFormatter(_Formatter):

	def format(self, record):
		msg = super().format(record)
		op = getattr(record, "Operation", None)

		# Supplying a Formatter to a RichHandler will enable highlighting,
		# even if highlighter=None is passed to RichHandler
		record.highlighter = None

		if op:
			if "Rename" in op:
				msg = f"[cyan]{msg}"
			elif "Delete" in op:
				msg = f"[red]{msg}"
			elif "Update" in op:
				msg = f"[yellow]{msg}"
			elif "Create" in op:
				msg = f"[green]{msg}"

		return msg

logger = logging.getLogger("psync")

# enable ANSI escape codes on Windows
if sys.platform == "win32":
	os.system("")

logger.setLevel(logging.INFO)
handler_stdout = logging.StreamHandler(sys.stdout)
handler_stderr = logging.StreamHandler(sys.stderr)
handler_stdout.addFilter(_DebugInfoFilter())
handler_stdout.setLevel(logging.DEBUG)
handler_stderr.setLevel(logging.WARNING)
handler_stdout.setFormatter(_Formatter())
handler_stderr.setFormatter(_Formatter())
logger.addHandler(handler_stdout)
logger.addHandler(handler_stderr)
