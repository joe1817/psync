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

class _ConsoleFormatter(logging.Formatter):
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

class _RichConsoleFormatter(_ConsoleFormatter):

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

'''
class ANSIColorFormatter(logging.Formatter):
	# ANSI escape codes
	COLORS = {
		"RED"   : "\033[31m",
		"GREEN" : "\033[32m",
		"YELLOW": "\033[33m",
		"CYAN"  : "\033[36m",
		"RESET" : "\033[0m",
	}

	def format(self, record):
		msg = super().format(record)
		op = getattr(record, "Operation", None)

		if op:
			if "Rename" in op:
				msg = f"{self.COLORS['CYAN']}{msg}{self.COLORS['RESET']}"
			elif "Delete" in op:
				msg = f"{self.COLORS['RED']}{msg}{self.COLORS['RESET']}"
			elif "Update" in op:
				msg = f"{self.COLORS['YELLOW']}{msg}{self.COLORS['RESET']}"
			elif "Create" in op:
				msg = f"{self.COLORS['GREEN']}{msg}{self.COLORS['RESET']}"

		return msg
'''

class _LogFileFormatter(logging.Formatter):
	'''Logging formatter for records saved to a log file.'''

	#BASE_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
	BASE_FORMAT = "%(message)s"

	def __init__(self, fmt=BASE_FORMAT, datefmt=None, style="%"):
		super().__init__(fmt, datefmt, style)

	def format(self, record):
		msg = super().format(record)
		if record.levelno == logging.DEBUG:
			msg = "  " + msg.replace("\n", "\n  ").rstrip(" ")
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

def setup_logger():
	'''Set up the "psync" package logger.'''

	if not logger.handlers:
		# enable ANSI escape codes on Windows
		if sys.platform == "win32":
			os.system('')

		logger.setLevel(logging.INFO)
		handler_stdout = logging.StreamHandler(sys.stdout)
		handler_stderr = logging.StreamHandler(sys.stderr)
		handler_stdout.addFilter(_DebugInfoFilter())
		handler_stdout.setLevel(logging.DEBUG)
		handler_stderr.setLevel(logging.WARNING)
		handler_stdout.setFormatter(_ConsoleFormatter())
		handler_stderr.setFormatter(_ConsoleFormatter())
		logger.addHandler(handler_stdout)
		logger.addHandler(handler_stderr)

setup_logger()
