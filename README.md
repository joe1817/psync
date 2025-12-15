# psync.py

`psync` is a python package for copying files and synchronizing directories.

## Features

- Use on the command line or as an imported package.
- Rename files in the dst folder to match the src folder.
- "Recycle" extra files in the dst folder by moving them to a different folder of your choosing.
- Include/exclude files based on recursive glob patterns.
- Sync with a remote server using SFTP.
- Watch a directory and automatically sync updates to another directory.
- `--dry-run` option to print potential results without actually making filesystem changes.

## Requirements
- python 3.13+

## Dependencies
- `paramiko` (only if you intend to use SFTP)
- `watchdog` (only if you intend to watch for filesystem changes)

## Examples

`python -m psync src dst`

&emsp; ↳ Recursively copies files inside `src/` to `dst/`, replacing files whose modtimes are newer in `src/`.

`python -m psync src dst -xf`

&emsp; ↳ Recursively copies files inside `src/` to `dst/` and deletes extra files (i.e., files that exist in `dst/` but not `src/`).

`python -m psync src dst -t trash`

&emsp; ↳ Recursively copies files inside `src/` to `dst/` and moves extra files into `trash/`.

`python -m psync src dst -f *.txt`

&emsp; ↳ Copies .txt files inside `src/` to `dst/`. This is a non-recursive copy due to the filter matching only top-level files.

`python -m psync src user@192.168.1.100/dst`

&emsp; ↳ Sync the local `src/` directory to the remote `dst/` directory using SFTP.

`python -m psync src dst -w -xf -nhf`

&emsp; ↳ Watch the `src/` directory and sync it to `dst/` whenever there is a filesystem change. Delete files in `dst/`. Don't print header or footer for these sync operations.

`python -m psync src dst !args.txt`

&emsp; ↳ Recursively copies files inside `src/` to `dst/`, reading additional arguments from `args.txt`.

### Filtering

`psync.py` can *include* or *exclude* files and folders based on user-supplied, recursive glob patterns (not regular expressions).

To make use of this option, keep in mind the following rules. This feature is accessed by supplying a filter string. The string contains groups of space-separated patterns, and eahc group is preceded by either an *include* ("+") or *exclude* ("-") indicator (e.g., `- skip.txt skip2.txt + **`). The patterns are matched against the relative paths of files and folders inside `src/` and `dst/`. If a pattern has a trailing slash (e.g., `foo/`), then the pattern will only apply to folders. Otherwise, the pattern will only apply to files. (The only exception is `**`, which applies to both files and directories.)

The indicator preceding the first matching pattern determines whether to *include* or *exclude* the filesystem entry. Included files will be copied in the backup, whereas included folders will be searched and their contents tested against the filter string. If a user-defined filter is supplied, then any unmatched file or folder will be ignored (neither copied nor deleted). An *include* indicator ("+") is assumed at the front of filter strings that don't start with "+" or "-".

The default filter string is `**`, which searches all folders and copies all files.

When a nested file is included, e.g., `+ foo/**/bar.txt`, then parent folders are also automatically included. This example is equiavlent to `+ foo/ foo/**/ foo/**/bar.txt`.

Relative paths can be used to shorten long pattern groups by inferring parent directories. The filter string `some/very/long/dir/path/1.txt some/very/long/dir/path/2.txt` can be replaced with `some/very/long/dir/path/ ./1.txt ./2.txt`. This feature can only be used in *include* pattern groups, and relative paths won't match with parent directories located in a different group.

`python -m psync src dst -f foo.txt`

&emsp; ↳ Copies the `foo.txt` file inside `src/` to `dst/`. Skips all other files and does not recurse into subfolders.

`python -m psync src dst -f + **/*.txt`

&emsp; ↳ Copies all .txt files inside `src/` to `dst/`.

`python -m psync src dst -f - "skip these/" + **/*.txt`

&emsp; ↳ Copies all .txt files inside `src/` to `dst/`, except those in the `src/skip these/` folder.

`python -m psync src dst -f - **/__pycache__/ + **`

&emsp; ↳ Copies all files inside `src/` to `dst/`, except for those inside folders named `__pycache__`.

`python -m psync src dst -f ./-`

&emsp; ↳ Copies the `-` file inside `src/` to `dst/`.

`python -m psync src dst -f a/ b/c/ ./**/1.txt`

&emsp; ↳ Copies the all `1.txt` files from inside `src/a/` and `src/b/c/` to `dst/`.
