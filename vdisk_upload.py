# so switch to pycurl is possible ...
from fetch_httplib import fetch_httplib as fetch
from vdisk_lib import vdiskrpc, VDISK_S3HOST

# Please delete resumedata file after changing this value
# It is recommended to make one part transfer about 10 to 30 sec, due to TCP.
# Recommended value: 4M for most users, 16M for high upload bandwidth (> 10 Mbits/s) users.
DEFAULT_SPLITSIZE = 1048576 * 4

HASH_BLOCKSIZE = 65536
RPC_RETRIES = 3
UPLOAD_RETRIES = 3

import hashlib
def filehash(f, h=None):
  if h is None:
    h = hashlib.md5()
  buf = f.read(HASH_BLOCKSIZE)
  while buf:
    h.update(buf)
    buf = f.read(HASH_BLOCKSIZE)
  return h.hexdigest()

def filemd5sha1(f):
  h1 = hashlib.md5()
  h2 = hashlib.sha1()
  buf = f.read(HASH_BLOCKSIZE)
  while buf:
    h1.update(buf)
    h2.update(buf)
    buf = f.read(HASH_BLOCKSIZE)
  return h1.hexdigest(), h2.hexdigest()

def rangesplit(size, splitsize):
  ranges = range((size-1) / splitsize + 1)
  range_left = lambda x: x * splitsize
  range_right = lambda x: min((x+1)*splitsize-1, size-1)
  return zip(map(range_left, ranges), map(range_right, ranges))

# RangeFile, by fcicq(fcicq at fcicq dot net), 2012.5.5, Release under GPLv2
import os
import sys

class RangeFile(file):
  _limitstart = None
  _limitend = None
  def __init__(self, *args, **kwargs):
    file.__init__(self, *args, **kwargs)
    if sys.platform == 'win32' and self.mode.find('b') == -1:
      raise Exception, 'RangeFile requires binary mode(mode=rb) for win32'
      
  def limitrange(self, startpos, endpos):
    if not (isinstance(startpos, (int, long)) and isinstance(endpos, (int, long))):
      raise Exception, 'Wrong Pos'
    if startpos > endpos:
      raise Exception, 'Wrong Range'
    if startpos < 0:
      raise Exception, 'Wrong Start Range'
    file.seek(self, 0, os.SEEK_END)
    filelen = file.tell(self)
    if endpos >= filelen:
      raise Exception, 'Wrong End Range'
    self._limitstart = startpos
    self._limitend = endpos
    file.seek(self, startpos)
    return self # make RangeFile('file').limitrange(0,100).read() possible :)
  def read(self, size=-1):
    if self._limitstart is None:
      return file.read(self, size)
    currpos = self.tell()
    if size < 0:
      size = self._limitend - currpos + 1
    else:
      size = min(self._limitend - currpos + 1, size)
    return file.read(self, size)
  def seek(self, offset, whence=0):
    if self._limitstart is None:
      return file.seek(self, offset, whence)
    if whence == 0 and offset == 0: # optimize for seek begin
      return file.seek(self, self._limitstart)
    pos = self.tell()
    r = file.seek(self, offset, whence)
    currpos = self.tell()
    if currpos >= self._limitstart and currpos <= self._limitend:
      return r
    else:
      file.seek(self, pos)
      raise Exception, 'Range Exceeded'

# uri already contains upload host.
def vdisk_uploads3_put(fp, range_left, range_right, uri, headers=None):
  if not isinstance(fp, RangeFile): raise Exception, 'fp is not RangeFile'
  if not isinstance(headers, dict): headers = {}
  headers['Content-Length'] = range_right - range_left + 1
  headers['Content-Type'] = 'application/octet-stream'
  fp.limitrange(range_left, range_right)
  md5sum = filehash(fp)
  fp.seek(0)
  options = {'method': 'PUT', 'timeout': 360}
  response = fetch(uri, headers, fp, options)
  response.rethrow()
  return md5sum

def vdisk_dirid(token, path='/'):
  if path[0] != '/': path = '/' + path
  if path in ['/', '']: return 0 # FIXME: error handling
  rpc = vdiskrpc()
  return rpc.get_dirid_with_path(token=token, path=path, field='id')

def vdisk_mkdir(token, path):
  if not path: return {'errcode': 900, 'err_msg': 'bad path'} # should it be 0?
  path = path.rstrip('/')
  if not path: path = '/'
  if path[0] != '/': path = '/' + path
  id = vdisk_dirid(token, path) # find directly
  if not isinstance(id, dict): return id # FIXME: errno handling?
  # not found
  parentdir = os.path.dirname(path)
  basename = os.path.basename(path)
  id = vdisk_mkdir(token, parentdir)
  if isinstance(id, dict):
    return {'errcode': 900, 'err_msg': 'create failed'}
  rpc = vdiskrpc()
  return rpc.create_dir(token=token, parent_id=id, create_name=basename, field='dir_id')

WIN32_ENCODING='gbk' # FIXME: how to get the encoding?
def to_utf8(name):
  if isinstance(name, unicode):
    return name.encode('utf-8')
  if sys.platform == 'win32': # Non-unicode
    return unicode(name, WIN32_ENCODING).encode('utf-8')
  return name

def to_console(name):
  if sys.platform == 'win32': 
    try:
      if isinstance(name, unicode):
        return name.encode(WIN32_ENCODING)
      else:
        return unicode(name, 'utf-8').encode(WIN32_ENCODING)
    except UnicodeDecodeError:
      return '---DECODE_FAILED---'
  return name

def get_remote_filename(file, remote_filename):
  if remote_filename is None:
    return to_utf8(os.path.basename(file))
  return to_utf8(remote_filename)

def vdisk_ls_r(token):
  vdisk_ls_dirid(token, 0, True)

def vdisk_ls(token, path):
  dir_id = vdisk_dirid(token, path)
  return vdisk_ls_dirid(token, dir_id)

def vdisk_ls_dirid(token, dir_id, traverse=False):
  rpc = vdiskrpc()
  l = rpc.get_list(token=token, dir_id=dir_id) # FIXME: page
  q = []
  if isinstance(l.get('data', None), list):
    l = l.get('data', [])
    for i in l:
      try:
        if 'sha1' in i:
          print i['sha1'], int(i['id']), int(i['length']), to_console(i['name'])
        else:
          q.append(int(i['id']))
          print 'DIR', int(i['id']), 0, to_console(i['name'])
      except: pass
  if traverse:
    for i in q:
      print 'SUB_DIR', i
      vdisk_ls_dirid(token, i, True)

import time
from collections import deque
def timetick(data, pos): # record time & current pos to calculate speed
  if data is None:
    data = deque()
  try:
    d = data[-1]
    if d[1] == pos: data.pop()
  except IndexError:
    pass
  except ValueError:
    pass
  data.append((time.time(), pos))
  if len(data) > 10: data.popleft()
  return data

def getspeed(data): # in bytes per sec
  if not data: return 0 # None
  if len(data) < 2: return 0
  l, r = data[0], data[-1]
  return int((float(r[1] - l[1]) / (time.time() - l[0] + 0.0000001)))

def speed_humanreadable(speed):
  if speed < 1024: return 'Unknown'
  if speed < 1048576 * 10:
    return '%.2f kB/s' % (speed / 1024.0)
  return '%.2f MB/s' % (speed / 1048576.0)

# TODO: package as a class, for upload
import tempfile
import pickle
def tempfile_open(readonly=True):
  try:
    fd = file('temp-vdisk-resumedata.pickle.tmp', readonly and 'rb' or 'wb')
  except IOError:
    return None
  except OSError:
    return None
  return fd

def tempfile_read():
  f = tempfile_open(True)
  if not f: return
  try:
    data = pickle.load(f)
  except:
    data = None
  finally:
    f.close()
  return data

def tempfile_save(data):
  f = tempfile_open(False)
  if not f: return
  try:
    pickle.dump(data, f, 2)
  except:
    print 'Save Resume File Failed'
  finally:
    f.close()

def save_resumedata(token, upload_key, part_number, md5sum):
  data = tempfile_read()
  if not isinstance(data, dict): data = {}
  d = {'t': token, 'u': upload_key, 'p': part_number}
  data[md5sum] = d
  tempfile_save(data)

def load_resumedata(md5sum):
  data = tempfile_read()
  if not isinstance(data, dict): data = {}
  d = data.get(md5sum, None)
  if d:
    return {'token': d.get('t', None), 'upload_key': d.get('u', None),
	'part_number': d.get('p', None)}
  return None

def clear_resumedata(md5sum):
  data = tempfile_read()
  if not isinstance(data, dict): data = {}
  d = data.pop(md5sum, None)
  tempfile_save(data)

# Long term TODO: out of order upload? add a upload_state (rather than part_number)? failed_parts?
# but we have to duplicate fd first, or have to prefetch the data...
def continue_upload(token, upload_key, fp, remote_filename, filesize, part_number, split_size=DEFAULT_SPLITSIZE, dir_id=0, md5sum=None):
  # get the md5 of the entire file (ignore if calculated)
  if not isinstance(fp, RangeFile): raise Exception, 'fp is not RangeFile'
  if not md5sum:
    fp.limitrange(0, filesize - 1)
    md5sum = filehash(fp)

  rpc = vdiskrpc()
  timedata = None
  md5list = {}
  ranges = rangesplit(filesize, split_size)
  parts = len(ranges)
  for i in range(parts):
    if i >= part_number: save_resumedata(token, upload_key, i, md5sum)
    range_left, range_right = ranges[i][0], ranges[i][1]
    fp.limitrange(range_left, range_right)
    current_part = i+1

    if i < part_number: # already finished, just add its md5 to the list
      fp.seek(0)
      md5sum_part = filehash(fp)
      md5list[current_part] = md5sum_part
      continue

    # now begin to upload
    for rpc_tries in range(RPC_RETRIES):
      uripart = rpc.big_file_upload_part(token=token, upload_key=upload_key, part_number=current_part, field='URI')
      if not isinstance(uripart, dict): break
    if isinstance(uripart, dict):
      return {'errcode': 9999, 'err_msg': 'upload RPC failed, please retry'}

    # time tick here
    timedata = timetick(timedata, range_left - 1)
    curtime_str = time.strftime("[%H:%M:%S]")
    print '%s Uploading Part %d / %d (%.2f %%), Speed %s' % (curtime_str, current_part, parts, float(range_left) / filesize * 100.0, speed_humanreadable(getspeed(timedata)))
    uri = 'http://' + VDISK_S3HOST + uripart
    for upload_tries in range(UPLOAD_RETRIES):
      resultmd5 = None
      try:
        resultmd5 = vdisk_uploads3_put(fp, range_left, range_right, uri) # headers?
        if resultmd5: break
      except KeyboardInterrupt:
        print 'user cancelled'
        raise
      except Exception, e:
        print str(e)
      print 'part upload temporarily failed ... (tried: %d)' % (upload_tries + 1)
    if not resultmd5:
      return {'errcode': 9999, 'err_msg': 'part upload failed, please retry'}
    # time tick here
    timedata = timetick(timedata, range_right)
    # part upload ok :)
    md5list[current_part] = resultmd5

  curtime_str = time.strftime("[%H:%M:%S]")
  print '%s Parts upload finished, Speed %s' % (curtime_str, speed_humanreadable(getspeed(timedata)))
  print 'Now Merging ...'

  # finally merge!
  save_resumedata(token, upload_key, current_part, md5sum)
  if not len(md5list) == len(ranges):
    return {'errcode': 9999, 'err_msg': 'ranges not matched'}

  md5s = ','.join(md5list.values())
  for i in range(RPC_RETRIES):
    try:
      result = rpc.big_file_upload_merge(token=token, upload_key=upload_key, file_name=remote_filename, 
                            dir_id=dir_id, md5=md5sum, md5s=md5s) # force is filled by library
      fid = rpc.getdata(result, 'fid')
      if isinstance(fid, dict):
        print 'looks failed? retry...'
        continue
      print 'Merge OK, fid: ', fid
      clear_resumedata(md5sum)
      return {'errcode': 0, 'fid': fid}
    except KeyboardInterrupt:
      return {'errcode': 9999, 'err_msg': 'interrupted by keyboard, resume: ' + cmdline}      

  return {'errcode': 9999, 'err_msg': 'Merger failed, retry with: ' + cmdline}

# actually only sha1 & path parsing is added in this func,
# and this func does the upload_key request.
def upload_bigfile(token, filename, remote_filename=None, path=None, dir_id=0, split_size=DEFAULT_SPLITSIZE):
  # check path
  try:
    dir_id = int(dir_id)
  except ValueError:
    dir_id = 0
  if path is not None and dir_id == 0:
    dir_id_new = vdisk_mkdir(token, path)
    if isinstance(dir_id_new, dict):
      return {'errcode': 9999, 'err_msg': 'directory create failed'}
    dir_id = dir_id_new
    print 'found dir_id for path %s: %s' % (path, dir_id)

  remote_filename = get_remote_filename(filename, remote_filename)
  try:
    # get filesize, 0 & return
    filesize = os.path.getsize(filename)
    if filesize == 0: return {'errcode': 9999, 'err_msg': 'filesize is 0'}

    # open the file, calculate md5 / sha1
    fp = RangeFile(filename, 'rb')
    md5sum, sha1sum = filemd5sha1(fp)
  except OSError, e:
    return {'errcode': 9999, 'err_msg': 'file error: ' + str(e)}
  print 'File: %s\nMD5 : %s\nSHA1: %s\nSize: %d' % (to_console(remote_filename), md5sum, sha1sum, filesize)
  rpc = vdiskrpc()

  # upload by sha1.
  for rpc_tries in range(RPC_RETRIES):
    match_result = rpc.upload_with_sha1(token=token, sha1=sha1sum, dir_id=dir_id, file_name=remote_filename, field='fid')
    if not isinstance(match_result, dict):
    # successed!
      print 'Matched by SHA1, fid: ', match_result
      return {'errcode': 0, 'fid': match_result}
    elif match_result.get('errcode') == 1: # Not found
      break

  # TODO: read the resume data by md5sum, remove upload_resume function
  resumedata = load_resumedata(md5sum)
  # (token, upload_key, current_part)
  if resumedata:
    return continue_upload(resumedata.get('token'), 
				resumedata.get('upload_key'),
				fp, remote_filename, filesize,
				resumedata.get('part_number'),
				split_size, dir_id, md5sum)


  # s3host will be filled by the library
  for rpc_retries in range(RPC_RETRIES):
    upload_key = rpc.big_file_upload(token=token, file_name=remote_filename, field='upload_key')
    if not isinstance(upload_key, dict): break
  if isinstance(upload_key, dict): # failed to get upload_key
    return upload_key
  return continue_upload(token, upload_key, fp, remote_filename, filesize, 0, split_size, dir_id, md5sum)

def get_token(user, passwd, vdisk_user=False):
  rpc = vdiskrpc()
  return rpc.get_token(user, passwd, vdisk_user=vdisk_user).get('data',{}).get('token', None)
