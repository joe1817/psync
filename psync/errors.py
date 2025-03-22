class MetadataUpdateError(PermissionError):
	'''Indicates a problem with updating file metadata (e.g., mtime) after a successful copy.'''
	def __init__(self, strerror=None, filename=None):
		super().__init__(1, strerror, filename)

class BrokenSymlinkError(FileNotFoundError):
	'''Indicates a problem with a symlink's target path.'''
	def __init__(self, strerror=None, filename=None):
		super().__init__(2, strerror, filename)

class IncompatiblePathError(PermissionError):
	'''Indicates a problem converting or comparing paths.'''
	def __init__(self, strerror=None, filename=None):
		super().__init__(1, strerror, filename)

class NewerInDstError(FileExistsError):
	'''Indicates a newer file in the dst root and `Sync.force_update` is set to `False`.'''
	def __init__(self, strerror=None, filename=None):
		super().__init__(17, strerror, filename)	

class StateError(RuntimeError):
	'''Indicates the object is in (or would be set to) an invalid state.'''
	pass

class ImmutableObjectError(StateError):
	'''Indicates an attempt to modify an immutable object.'''
	pass

class UnsupportedOperationError(RuntimeError):
	'''Indicates the attempted action is not supported by design.'''
	pass
