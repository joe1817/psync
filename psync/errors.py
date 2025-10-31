class MetadataUpdateError(OSError):
	'''Indicates a problem with updating file metadata (e.g., mtime) after a successful copy.'''
	pass

class DirDeleteError(OSError):
	'''Indicates a problem with deleting a directory.'''
	pass

class StateError(RuntimeError):
	'''Indicates the object is in an invalid state for the attempted operation.'''
	pass

class ImmutableObjectError(StateError):
	'''Indicates an attempt to modify an immutable object.'''
	pass

class UnsupportedOperationError(RuntimeError):
	'''Indicates the attempted action is not supported by design.'''
	pass
