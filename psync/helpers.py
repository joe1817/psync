from typing import Any

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
