# psync.py

psync is a simple utility to copy files from one folder to another.

## Features

- Can be used on the command line or as an imported module.
- Files that were renamed in the src folder can be renamed in the dst folder, without unnecessarily copying files.
- Can "recycle" extra files in the dst folder by moving them to a different folder of your choosing.
- Include/exclude files based on recursive glob patterns.
- Can copy to a remote server using SFTP.
- Log the results to a log file.
- `--dry-run` option to print would-be results without actually making changes to the file system.

## Requirements
- python 3.13+ (earlier versions can be used if you install the glob2 package)

## Dependencies
- paramiko (only if you intend to use SFTP)

## Examples

`python -m psync src dst`

&emsp; ↳ Recursively copies files inside `src/` to `dst/`, replacing files whose modtimes are newer in `src/`.

`python -m psync src dst -x`

&emsp; ↳ Recursively copies files inside `src/` to `dst/` and deletes extra files (i.e., files that exist in `dst/` but not `src/`).

`python -m psync src dst -t trash`

&emsp; ↳ Recursively copies files inside `src/` to `dst/` and moves extra files into `trash/`.

`python -m psync src user@192.168.1.100/dst`

&emsp; ↳ Sync using SFTP the local `src/` directory to the remote `dst/` directory.

`python -m psync src dst !args.txt`

&emsp; ↳ Recursively copies files inside `src/` to `dst/`, using arguments from the file, `args.txt`.

### Include/ Exclude

`psync.py` can include or exclude files and folders based on user-supplied, recursive glob patterns. Also note that the recursive glob, `**`, is treated like the non-recurisve glob, `*`, if it is adjacent to any character other than "\\" or "/".

To make use of this option, keep in mind the following rules. This feature is accessed by supplying a filter string. The string contains various space-separated patterns, preceded by include ("+") and exclude ("-") indicators (e.g., `- skip.txt skip2.txt + **`). The patterns are matched against the relative paths of files and folders inside `src/` and `dst/`. If a pattern has a trailing slash (e.g., `foo/`), then the pattern will only apply to folders. Otherwise, the pattern will only apply to files. (The only exception is `**`, which applies to both files and folders.)

The indicator preceding the first matching pattern determines whether to include or exclude the filesystem entry. Included files will be copied in the backup, whereas included folders will be searched and their contents tested against the filter string. The default filter string is `+ **/*/ **/*`, which searches all folders and copies all files. If a user-defined filter is supplied, then any unmatched file or folder will be ignored (neither copied nor "recycled"). An include indicator ("+") is assumed at the front of filter strings that don't start with "+" or "-".

When a nested file is included, e.g., `+ foo/**/bar.txt`, then parent folders are also automatically included. This example is equiavlent to `+ foo/ foo/**/ foo/**/bar.txt`.

`python -m psync src dst -f foo.txt`

&emsp; ↳ Copies the `foo.txt` file inside `src/` to `dst/`. Skips all other files and does not recurse into subfolders.

`python -m psync src dst -f + **/*.txt`

&emsp; ↳ Copies all .txt files inside `src/` to `dst/`.

`python -m psync src dst -f - "skip these/" + **/*.txt`

&emsp; ↳ Copies all .txt files inside `src/` to `dst/`, except those in the `src/skip these/` folder.

`python -m psync src dst -f - **/__pycache__/ + **`

&emsp; ↳ Copies all files inside `src/` to `dst/`, except for those inside folders named `__pycache__`.

`python -m psync src dst -f ./-`

&emsp; ↳ Copies all the `-` file inside `src/` to `dst/`.
