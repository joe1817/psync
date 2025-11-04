class MetadataUpdateError(PermissionError):
	'''Indicates a problem with updating file metadata (e.g., mtime) after a successful copy.'''
	def init(self, strerr=None, filename=None):
		super().__init__(self, 1, strerr, filename)

class BrokenSymlinkError(FileNotFoundError):
	'''Indicates a problem with a symlink's target path.'''
	def init(self, strerr=None, filename=None):
		super().__init__(self, 2, strerr, filename)

class IncompatiblePathError(PermissionError):
	'''Indicates a problem converting or comparing paths.'''
	def init(self, strerr=None, filename=None):
		super().__init__(self, 1, strerr, filename)

class StateError(RuntimeError):
	'''Indicates the object is in (or would be set to) an invalid state.'''
	pass

class ImmutableObjectError(StateError):
	'''Indicates an attempt to modify an immutable object.'''
	pass

class UnsupportedOperationError(RuntimeError):
	'''Indicates the attempted action is not supported by design.'''
	pass
