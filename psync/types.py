import os
from pathlib import Path

from .sftp import RemotePath

PathType = Path | RemotePath
PathLikeType = str | Path | RemotePath # os.PathLike[bytes] isn't supported, so don't use os.PathLike here
