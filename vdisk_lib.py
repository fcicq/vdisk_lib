from fetch_httplib import fetch_response, fetch_httplib as fetch

import hmac, hashlib, time, urllib, json
VDISK_APIBASE = 'http://openapi.vdisk.me/'
VDISK_S3HOST = 'up-tj.sinastorage.com' 
VDISK_APPKEY = 2750066530
VDISK_SECRET = {2750066530: 'd859f00d266b8180fbf28162fc9bfd1d'}
VDISK_REQUIRE_LOGIN = ['token']
VDISK_REQUIRE_FID = ['fid', 'token']
VDISK_REQUIRE_DIR = ['dir_id', 'token']
#VDISK_REQUIRE_DOLOGID = ['dologid', 'token'] # Not used.
VDISK_NOTCHECKED = False
VDISK_NOAUTH = []

# ?a=COMMAND&m=TYPE. (type, [required items] or False(not checked))
VDISK_RPC_TABLE = {
  'get_token': ('auth', ['account', 'password', 'appkey', 'time', 'signature', 'app_type']),
  'keep_token': ('user', VDISK_REQUIRE_LOGIN),
  'get_dirid_with_path': ('dir', ['token', 'path']),
  'create_dir': ('dir', ['token', 'create_name', 'parent_id']),
  'getlist': ('dir', VDISK_REQUIRE_DIR), # Note: page is also available
  'get_list': ('dir', VDISK_REQUIRE_DIR),
  'list_items': ('dir', VDISK_REQUIRE_DIR),
  'delete_dir': ('dir', VDISK_REQUIRE_DIR),
  'get_quota': ('file', VDISK_REQUIRE_LOGIN),
  'rename_file': ('file', ['token', 'fid', 'new_name']),
  'delete_file': ('file', VDISK_REQUIRE_FID),
  'get_file_info': ('file', VDISK_REQUIRE_FID),
  'upload_with_sha1': ('file', ['token', 'file_name', 'sha1', 'dir_id']),
  'big_file_upload': ('file', ['token', 'file_name', 's3host']),
  'big_file_upload_part': ('file', ['token', 'upload_key', 'part_number']), # return URI and PUT with SINASTORAGE
  'big_file_upload_merge': ('file', ['token', 'upload_key', 'md5', 'md5s', 'file_name', 'dir_id', 'force']),
  'upload_sign': ('file', ['token', 'file_name']),
  # Note: multipart post is required for upload.
  'upload_back': ('file', ['token', 'key', 'file_name', 'dir_id', 'fover', 'date']),
  # client related
  'hot': ('file', ['page_no', 'page_size', 'cid']), # is cid optional?
  'share': ('search', ['keyword', 'page']),
  'get_file_info_anon': ('file', VDISK_REQUIRE_FID),
  'getList': ('cate', VDISK_NOAUTH),
  'save_file_to_my_box': ('file', ['token', 'fid', 'dir_id']),
  'own': ('search', ['token', 'keyword', 'page', 'page_size']), # search files
}
VDISK_RPC_TABLE_SPECIAL = {
  'secretshare': (VDISK_APIBASE + '1/linkcommon/new', VDISK_REQUIRE_FID),
  'keep': (VDISK_APIBASE + '?a=keep', VDISK_REQUIRE_LOGIN),
}
VDISK_ERRNO_RETRY = [6, 709, 710, 711, 9995, 9996, 9997, 9998, 9999]
# 6: S3 Error
# 709: Error putting to S3
# 710: upload_key error
# 711: Merger failed
VDISK_ERRNO_IGNORE = [0, 702, 721, 909] # meanings?
VDISK_ARG_DEFAULT = {'dir_id': 0, 'parent_id': 0, 'path': '/', 'appkey': VDISK_APPKEY,
		'force': 'yes', 'fover': 'rename', 's3host': VDISK_S3HOST,
		'page_no': 1, 'page': 1, 'page_size': 20, 'app_type': 'sinat'}

from functools import partial
class RunBase():
  def run(self, name, *args, **kwargs):
    raise NotImplementedError()
  def __getattr__(self, name):
    return partial(self.run, name)

def strip_json(a):
  if not isinstance(a, basestring): return ''
  lpos = a.find('{')
  rpos = a.rfind('}')
  if lpos == -1 or rpos == -1:
    # FIXME: logging?
    return a
  return a[lpos:rpos+1]

class vdiskrpc(RunBase):
  def _verify_args(self, name, **kwargs):
    if name in VDISK_RPC_TABLE:
      keylist = VDISK_RPC_TABLE[name][1]
    elif name in VDISK_RPC_TABLE_SPECIAL:
      keylist = VDISK_RPC_TABLE_SPECIAL[name][1]
    else:
      raise NotImplementedError()
    if keylist is False: return kwargs
    # Note: optional tags?!
    for i in keylist:
      if i not in kwargs:
        if i in VDISK_ARG_DEFAULT:
          kwargs[i] = VDISK_ARG_DEFAULT[i]
        if i == 'file_name' and 'sha1' in kwargs: 
          # fill default filename by sha1
          kwargs[i] = 'sha1_' + i
        if i in ['date', 'time']:
          kwargs[i] = int(time.time())
    if 'signature' in keylist:
      kwargs['signature'] = self._generate_sign(kwargs)
    if set(keylist) != set(kwargs.keys()): raise TypeError('Expected args: ' + ','.join(keylist))
    # do actual checks?
    return kwargs

  def _uri(self, name):
    if name in VDISK_RPC_TABLE:
      return VDISK_APIBASE + '?m=%s&a=%s' % (VDISK_RPC_TABLE[name][0], name)
    elif name in VDISK_RPC_TABLE_SPECIAL:
      return VDISK_RPC_TABLE_SPECIAL[name][0]
    else:
      raise NotImplementedError()

  def _generate_request(self, name, **kwargs):
    verified_kwargs = False
    try:
      verified_kwargs = self._verify_args(name, **kwargs)
    except NotImplementedError:
      print 'not implemented'
      return None
    except TypeError:
      print 'type error'
      return None
    except Exception, e:
      print 'Exception', str(e)
      return None
    if not isinstance(verified_kwargs, dict):
      return None
    uri = self._uri(name)
    postdata = urllib.urlencode(verified_kwargs)
    headers = {}
    if postdata:
      headers['Content-Type'] = 'application/x-www-form-urlencoded'
    else:
      postdata = None
    return (uri, headers, postdata)

  def _hmac_sha256(self, s, secret):
    return hmac.new(secret, s, hashlib.sha256).hexdigest()

  def _generate_sign(self, kwargs):
    if int(kwargs['appkey']) not in VDISK_SECRET:
      raise Exception, 'Appkey not found'
    postdata = [('account',kwargs['account']),('appkey',kwargs['appkey']),
                ('password',kwargs['password']),('time',kwargs['time'])]
    s = urllib.urlencode(postdata)
    return self._hmac_sha256(urllib.unquote(s), VDISK_SECRET[int(kwargs['appkey'])])

  def get_token(self, user, passwd, vdisk_user=False, callback=None, appkey=VDISK_APPKEY):
    app_type = 'sinat'
    if vdisk_user:
      app_type = 'local'
    # field?
    return self.run('get_token', account=user, password=passwd, app_type=app_type, appkey=appkey, callback=callback)
  
  def checkerror(self, data): # expect 0
    if not isinstance(data, dict): 
      data = {'errcode': 9999, 'err_msg': 'fetch data broken'}
    errno = 9998
    if 'err_code' in data:
      errno = int(data['err_code'])
    if 'errcode' in data:
      errno = int(data['errcode'])
    data['errcode'] = errno
    return data

  def displayerror(self, data):
    if 'err_msg' in data and errno != 9998 and errno not in VDISK_ERRNO_IGNORE:
      print "Error", errno, data.get('err_msg') # logging?

  def getdata(self, data, field):
    if not isinstance(data, dict): return {'errcode': 9999, 'err_msg': 'data is not dict'}
    if field in data: return data.get(field)
    subdata = data.get('data', {})
    if field in subdata: return subdata.get(field)
    return data

  def run(self, name, **kwargs):
    cb = kwargs.pop('callback', None)
    field = kwargs.pop('field', None)
    t = self._generate_request(name, **kwargs)
    if not t: return None
    uri, headers, postdata = t 
    response = fetch(uri, headers, postdata)
    if response.status == 200:
      data = response.body
    else:
      data = json.dumps({'errcode': 9999, 'err_msg': 'fetch failed'})
    try:
      data = strip_json(data)
      data = json.loads(data)
    except ValueError:
      data = {'errcode': 9999, 'err_msg': 'json decode failed'}
    data = self.checkerror(data)
    if field:
      return self.getdata(data, field)
    return data
