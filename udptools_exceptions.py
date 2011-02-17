
class AlreadyRunningError(Exception):
    """
    Raised when a dump is attempted while the object is already dumping, or when
    a play is attempted when the object is already playing.
    """
