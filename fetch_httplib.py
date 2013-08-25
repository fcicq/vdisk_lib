try:
  from gevent import socket
except ImportError:
  print('fallback to typical socket')
  import socket
import httplib
from urllib import splittype, splithost

DEFAULT_TIMEOUT = None

class CustomHTTPException(Exception):
  pass

class fetch_response(object): # similar with tornado.httpclient.HTTPResponse
  def __init__(self, code, **kwargs):
    self.status = code
    self.headers = kwargs.get('headers', {})
    self.body = kwargs.get('body', None)
    self.error = kwargs.get('error', None) # default exception?
    # FIXME: handle gzip here?
  def rethrow(self):
    if self.error:
      raise self.error

# Note: postdata can be a fd.
def fetch_httplib(uri, headers=None, postdata=None, options=None):
  if not isinstance(options, dict): options = {}
  if not headers: headers = {}
  timeout = options.get('timeout', DEFAULT_TIMEOUT)
  method = options.get('method', None)
  proxy = options.get('proxy', None)
  if method is None:
    method = (postdata is None) and 'GET' or 'POST'
  try:
    #if proxy: # TODO
    urltype, hoststr = splittype(uri)
    host, path = splithost(hoststr)
    if urltype.lower() == 'https':
      conn = httplib.HTTPSConnection(host, timeout=timeout)
    else:
      conn = httplib.HTTPConnection(host, timeout=timeout)
    conn.request(method, path, postdata, headers)
    resp = conn.getresponse()    
    response = fetch_response(resp.status, body=resp.read(), headers=dict(resp.getheaders()))
  except KeyboardInterrupt:
    raise
  #except httplib.HTTPException, e:
  #  return fetch_response(504, error=CustomHTTPException(str(e)))
  #except socket.timeout, e:
  except Exception, e:
    return fetch_response(504, error=CustomHTTPException(str(e)))
  finally:
    if conn:
      conn.close()
  return response
