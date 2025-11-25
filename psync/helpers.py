import time
import threading
from datetime import datetime
from typing import Any

from .errors import IncompatiblePathError

class _UniqueIDGenerator:
	_last_time = 0
	_counter = 0
	_lock = threading.Lock()

	@classmethod
	def get_timestamp(cls) -> str:
		with cls._lock:
			current_time = int(time.time())

			if current_time > cls._last_time:
				# Time advanced: reset the counter
				cls._counter = 0
				cls._last_time = current_time
			elif current_time == cls._last_time:
				# Time is the same: increment the counter
				cls._counter += 1
			else:
				# Time went backwards: shouldn't happen, but handle anyways
				cls._counter = 0
				cls._last_time += 1
				current_time = cls._last_time

			dt = datetime.fromtimestamp(cls._last_time)
			unique_id = dt.strftime("%Y%m%d_%H%M%S")
			if cls._counter:
				unique_id += f".{cls._counter}"
			return unique_id

def _reverse_dict(old_dict:dict[Any, Any]) -> dict[Any, Any]:
	'''
	Reverses a `dict` by swapping keys and values. If a value in `old_dict` appears more than once, then the corresponding key in the reversed `dict` will point to a `None`.

	>>> _reverse_dict({"a":1, "b":2, "c":2})[1]
	'a'
	>>> _reverse_dict({"a":1, "b":2, "c":2})[2] is None
	True
	'''

	reversed:dict[Any, Any] = {}
	for key, val in old_dict.items():
		if val in reversed:
			reversed[val] = None
		else:
			reversed[val] = key
	return reversed

def _merge_iters(src, dst, *, key=lambda x: x):
	'''
	Repeatedly yields the next lowest element from either iterable, similar to how merge sort works.

	>>> list(_merge_iters([1,3,5], [2,4,6]))
	[(-1, 1, 2), (1, 3, 2), (-1, 3, 4), (1, 5, 4), (-1, 5, 6), (1, None, 6)]
	>>> list(_merge_iters([1,2], [2,2,3]))
	[(-1, 1, 2), (0, 2, 2), (1, None, 2), (1, None, 3)]
	>>> list(_merge_iters([(1,1)], [(1,1)]))
	[(0, (1, 1), (1, 1))]
	'''

	src_iter = iter(src)
	dst_iter = iter(dst)
	stopped  = object()

	try:
		try:
			s = next(src_iter)
			s_comp = key(s)
		except StopIteration:
			s = stopped
		try:
			d = next(dst_iter)
			d_comp = key(d)
		except StopIteration:
			d = stopped
		if s is stopped or d is stopped:
			raise StopIteration

		while True:
			while s_comp == d_comp:
				yield 0, s, d
				try:
					s = next(src_iter)
					s_comp = key(s)
				except StopIteration:
					s = stopped
				try:
					d = next(dst_iter)
					d_comp = key(d)
				except StopIteration:
					d = stopped
				if s is stopped or d is stopped:
					raise StopIteration

			while s_comp < d_comp:
				yield -1, s, d
				try:
					s = next(src_iter)
					s_comp = key(s)
				except StopIteration:
					s = stopped
					raise StopIteration

			while s_comp > d_comp:
				yield 1, s, d
				try:
					d = next(dst_iter)
					d_comp = key(d)
				except StopIteration:
					d = stopped
					raise StopIteration

	except StopIteration:
		if s is stopped and d is stopped:
			return
		if s is stopped:
			yield 1, None, d
			yield from ((1,None,d) for d in dst_iter)
		else:
			yield -1, s, None
			yield from ((-1,s,None) for s in src_iter)

def _convert_sep(path:str, src_sep:str, dst_sep:str):
	r'''
	Translates `src_sep` (path separators) in  `path` to `dst_sep`.

	>>> _convert_sep("\\a/b", "/", "\\")
	Traceback (most recent call last):
	...
	psync.errors.IncompatiblePathError: [Errno 1] Incompatible path for this system: '\\a/b'
	>>> _convert_sep("a/b", "/", "\\")
	'a\\b'
	>>> _convert_sep("\\a/b", "\\", "/")
	'/a/b'
	>>> _convert_sep("\\a/b", "/", "/")
	'\\a/b'
	>>> _convert_sep("\\a/b", "\\", "\\")
	'\\a\\b'
	'''

	if src_sep == dst_sep:
		if src_sep == "/":
			return path
		else:
			return path.replace("/", "\\")
	elif src_sep == "\\":
		return path.replace("\\", "/")
	else:
		if "\\" in path:
			raise IncompatiblePathError("Incompatible path for this system", str(path))
		else:
			return path.replace("/", "\\")

def _human_readable_size(n:int) -> str:
	'''
	Translates `n` bytes into a human-readable size.

	>>> _human_readable_size(1023)
	'+1023 bytes'
	>>> _human_readable_size(-1024)
	'-1 KB'
	>>> _human_readable_size(2.1 * 1024 * 1024)
	'+2 MB'
	'''

	if n < 0:
		sign = "-"
	elif n > 0:
		sign = "+"
	else:
		sign = ""

	n = abs(n)
	units = ["bytes", "KB", "MB", "GB", "TB", "PB"]
	i = 0
	while n >= 1024 and i < len(units) - 1:
		n //= 1024
		i += 1
	return f"{sign}{round(n)} {units[i]}"
