vdisk_lib
=========

vdisk.me (vdisk.weibo.com) api &amp; client implementation in Python

Released under MIT License.

Experimental. feel free to report problems on github. Fork is encouraged! :)

usage: vdisk.py [-h] [-u USER] [-p PASS] [--traverse] [--vdiskuser]
                [--path PATH] [--fid FID] [--ls] [--quota] [--upload]
                [--secretshare] [--getinfo]
                [files [files ...]]

VDisk CLI Client by fcicq, ver 0.1

positional arguments:
  files                 Files to be uploaded

optional arguments:
  -h, --help            show this help message and exit
  -u USER, --user USER  Account (Weibo or Vdisk)
  -p PASS, --pass PASS  Password
  --traverse            Use with --ls, to traverse all directory recursively
  --vdiskuser           Use Vdisk account (rather than weibo account)
  --path PATH           Upload Path, default is /, will be created if not
                        exist
  --fid FID             File ID
  --ls                  List files
  --quota               Check available space
  --upload              Upload file to vdisk
  --secretshare         Get secret sharing link
  --getinfo             Get file info (with direct download link)
