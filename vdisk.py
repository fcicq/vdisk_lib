#!/usr/bin/env python
import argparse
VDISK_LIB_VERSION = '0.1'
AVAILABLE_ACTIONS = {'upload': 'Upload file to vdisk',
                        'getinfo': 'Get file info (with direct download link)',
                        'secretshare': 'Get secret sharing link',
                        'ls': 'List files',
                        'quota': 'Check available space'}

def checkaction(argdict): # FIXME: add path_required / fid_required ...
  actions = AVAILABLE_ACTIONS.keys()
  cnt = 0
  for i in actions:
    if argdict[i]: cnt += 1
  if cnt > 1: raise Exception, 'More than one action are given'
  # FIXME: return the matched action?
  return (cnt == 1)

parser = argparse.ArgumentParser(description='VDisk CLI Client by fcicq, ver ' + VDISK_LIB_VERSION)
parser.add_argument('files', help='Files to be uploaded', action='store', nargs='*')
parser.add_argument('-u', '--user', help='Account (Weibo or Vdisk)')
parser.add_argument('-p', '--pass', help='Password')
parser.add_argument('--traverse', action='store_true', help='Use with --ls, to traverse all directory recursively')
parser.add_argument('--vdiskuser', action='store_true', help='Use Vdisk account (rather than weibo account)')
parser.add_argument('--path', help='Upload Path, default is /, will be created if not exist')
parser.add_argument('--fid', help='File ID')

for act, helpstr in AVAILABLE_ACTIONS.items():
  parser.add_argument('--' + act, action='store_true', help=helpstr)

# TODO: read config file
config = {}
config['default_user'] = ''
config['default_pass'] = ''

import vdisk_upload as vdisk
if __name__ == '__main__':
  args = parser.parse_args()
  argdict = args.__dict__

  if not checkaction(argdict): # FIXME
    raise Exception, 'No action is given'

  # login
  # TODO: may save token in config file.
  acc_user, acc_pass = argdict.get('user', config['default_user']), argdict.get('pass', config['default_pass'])
  if not acc_user or not acc_pass: raise Exception, 'User / Password Empty?'
  token = vdisk.get_token(acc_user, acc_pass, argdict['vdiskuser'])
  if not token: raise Exception, "Login Failed"
  print 'Login ok with user %s' % acc_user

  dir_id = 0
  if argdict['path']:
    dir_id = vdisk.vdisk_mkdir(token, argdict['path'])
    if isinstance(dir_id, dict): raise Exception, "Create Directory Failed"

  # commands
  if argdict['ls']:
    vdisk.vdisk_ls_dirid(token, dir_id, argdict['traverse'])

  rpc = vdisk.vdiskrpc()
  if argdict['upload']:
    for i in argdict['files']:
      fid = vdisk.upload_bigfile(token, i, dir_id=dir_id)
      if isinstance(fid, dict):
        if not fid.get('fid', None):
          print 'Upload Failed, errmsg: ', fid.get('err_msg', '-')

  if argdict['quota']:
    data = rpc.get_quota(token=token)
    if int(data.get('errcode')) == 0:
      q = data.get('data')
      per = float(q['used']) / float(q['total']) * 100
      print 'Used %d of %d MB Total (%.2f %%)' % (int(q['used']) / 1048576, int(q['total']) / 1048576, per)
    else:
      print 'Failed to get quota data.'
  
  if argdict['secretshare']:
    fid = argdict['fid']
    if not fid:
      print 'fid is required to get file link.'
    else:
      data = rpc.secretshare(token=token, fid=fid)
      if int(data.get('errcode')) == 0:
        ret = data.get('data', {})
        print 'Name: %s\nLink: %s\nPass: %s' % (
		vdisk.to_console(ret.get('name','-')),
		ret.get('url', '-'), ret.get('password', '-'))
      else:
        print 'get link failed: ' + data.get('err_msg', '-')

  if argdict['getinfo']:
    fid = argdict['fid']
    if not fid:
      print 'fid is required to get file info.'
    else:
      data = rpc.get_file_info(token=token, fid=fid)
      if data.get('errcode') == 0:
        data = data.get('data', {})
        print 'File: %s\nSize: %s\nMD5 : %s\nSHA1: %s\nPub : %s\nPriv: %s\nShared: %s\n' % (
		vdisk.to_console(data.get('name', '-')), 
		data.get('length', '-'), 
		data.get('md5', '-'), data.get('sha1', '-'),
		data.get('url', '-'), data.get('s3_url', '-'),
		(data.get('share', '-') == -1) and 'No' or 'Yes')
      else:
        print 'Failed to get info: ', data.get('err_msg', '')
