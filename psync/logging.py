import logging

class _DebugInfoFilter(logging.Filter):
	'''Logging filter that only allows DEBUG and INFO records to pass.'''
	def filter(self, record):
		return logging.DEBUG <= record.levelno <= logging.INFO

class _NonEmptyFilter(logging.Filter):
    '''Logging filter that only allows non-empty messages.'''
    def filter(self, record):
        return bool(str(record.msg).strip())

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