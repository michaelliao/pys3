#!/usr/bin/env python
# -*-coding: utf8 -*-

'''
Amazon S3 API by:

Michael Liao (askxuefeng@gmail.com)
'''

import re, os, sha, time, hmac, base64, hashlib, urllib, urllib2, mimetypes, logging

from datetime import datetime, timedelta, tzinfo
from StringIO import StringIO

def main():
    client = Client('access_key_id', 'access_key_secret', 'my.bucket.name')
    print client.put_object('path/hello.html', 'this is just a test and should return url')
    # http://my.bucket.name.s3.amazonaws.com/the/path/hello.html

    print client.get_object('the/path/hello.html')
    # file content as str...

    client.delete_object('the/path/hello.html')

    print client.get_object('the/path/hello.html')
    # Traceback (most recent call last):
    #   ...
    # StorageError: ('NoSuchKey', 'The specified key does not exist.')

_URL = 'http://%s.s3.amazonaws.com/%s'

_RE_URL1 = re.compile(r'^http\:\/\/([\.\w]+)\.s3[\-\w]*\.amazonaws\.com\/(.+)$')
_RE_URL2 = re.compile(r'^http\:\/\/s3[\-\w]*\.amazonaws\.com\/([\.\w]+)\/(.+)$')
_RE_URL3 = re.compile(r'^http\:\/\/([\.\-\w]+)\/(.+)$')

class StorageError(StandardError):
    pass

class Client(object):

    def __init__(self, access_key_id, access_key_secret, bucket=None, cname=False):
        '''
        Init an S3 client with:

        Args:
            access_key_id: the access key id.
            access_key_secret: the access key secret.
            bucket: (optional) the default bucket name, or None.
            cname: (optional) specify weather use cname or not when generate url after PUT.
        '''
        self._access_key_id = access_key_id
        self._access_key_secret = access_key_secret
        self._bucket = bucket
        self._cname = cname

    def _check_obj(self, obj):
        if not obj:
            raise StorageError('ObjectName', 'Object cannot be empty.')
        if isinstance(obj, unicode):
            obj = obj.encode('utf-8')
        if obj.startswith('/') or obj.startswith('\\'):
            raise StorageError('ObjectName', 'Object name cannot start with \"/\" or \"\\\"')
        return obj

    def _check_bucket(self, bucket):
        if bucket:
            return bucket
        if self._bucket:
            return self._bucket
        raise StorageError('BucketName', 'Bucket is required but no default bucket specified.')

    def names_from_url(self, url):
        '''
        get bucket and object name from url.

        >>> c = Client('key', 'secret')
        >>> c.names_from_url('http://sample.s3.amazonaws.com/test/hello.html')
        ('sample', 'test/hello.html')
        >>> c.names_from_url('http://www.sample.com.s3.amazonaws.com/test/hello.html')
        ('www.sample.com', 'test/hello.html')
        >>> c.names_from_url('http://sample.s3-ap-northeast-1.amazonaws.com/test/hello.html')
        ('sample', 'test/hello.html')
        >>> c.names_from_url('http://s3.amazonaws.com/sample/test/hello.html')
        ('sample', 'test/hello.html')
        >>> c.names_from_url('http://s3-ap-northeast-1.amazonaws.com/sample/test/hello.html')
        ('sample', 'test/hello.html')
        >>> c.names_from_url('http://www.amazon.com/hello.html')
        ('www.amazon.com', 'hello.html')
        >>> c.names_from_url('http://www.amazon.com/')
        (None, None)
        '''
        m = _RE_URL1.match(url)
        if m:
            return m.groups()
        m = _RE_URL2.match(url)
        if m:
            return m.groups()
        m = _RE_URL3.match(url)
        if m:
            return m.groups()
        return None, None

    def list_buckets(self):
        '''
        Get all buckets.
        '''
        r = _api(self._access_key_id, self._access_key_secret, 'GET', '', '')
        L = []
        pos = 0
        while True:
            s, pos = _mid(r, '<Name>', '</Name>', pos)
            if s:
                L.append(s)
            else:
                break
        return L

    def get_object(self, obj, bucket=None):
        '''
        Get file content.

        Args:
            obj: object name.
            bucket: (optional) using default bucket name or override.
        Returns:
            str as file content.
        '''
        return _api(self._access_key_id, self._access_key_secret, 'GET', self._check_bucket(bucket), self._check_obj(obj))

    def put_object(self, obj, payload, bucket=None):
        '''
        Upload file.

        Args:
            obj: Object name.
            payload: str or file-like object as file content.
            bucket: (optional) using default bucket name or override.
        Returns:
            the url of uploaded file.
        '''
        r = _api(self._access_key_id, self._access_key_secret, 'PUT', self._check_bucket(bucket), self._check_obj(obj), payload)
        if self._cname:
            return 'http://%s/%s' % r
        return 'http://%s.s3.amazonaws.com/%s' % r

    def delete_object(self, obj, bucket=None):
        '''
        Delete file.

        Args:
            obj: object name.
            bucket: (optional) using default bucket name or override.
        '''
        _api(self._access_key_id, self._access_key_secret, 'DELETE', self._check_bucket(bucket), self._check_obj(obj))

_TIMEDELTA_ZERO = timedelta(0)

class GMT(tzinfo):

    def utcoffset(self, dt):
        return _TIMEDELTA_ZERO

    def dst(self, dt):
        return _TIMEDELTA_ZERO

    def tzname(self, dt):
        return 'GMT'

_GMT = GMT()

def _current_datetime():
    return datetime.fromtimestamp(time.time(), _GMT).strftime('%a, %0d %b %Y %H:%M:%S +0000')

_APPLICATION_OCTET_STREAM = 'application/octet-stream'

def _guess_content_type(obj):
    n = obj.rfind('.')
    if n==(-1):
        return _APPLICATION_OCTET_STREAM
    return mimetypes.types_map.get(obj[n:], _APPLICATION_OCTET_STREAM)

def _signature(access_key_id, access_key_secret, bucket, obj, verb, content_md5, content_type, date, headers=None):
    '''
    Make signature for args.

    >>> access_key_id = 'AKIAIOSFODNN7EXAMPLE'
    >>> access_key_secret = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
    >>> _signature(access_key_id, access_key_secret, 'johnsmith', 'photos/puppy.jpg', 'PUT', '', 'image/jpeg', 'Tue, 27 Mar 2007 21:15:45 +0000')
    'MyyxeRY7whkBe+bq8fHCL/2kKUg='
    >>> _signature(access_key_id, access_key_secret, 'dictionary', 'fran%C3%A7ais/pr%c3%a9f%c3%a8re', 'GET', '', '', 'Wed, 28 Mar 2007 01:49:49 +0000')
    'DNEZGsoieTZ92F3bUfSPQcbGmlM='
    '''
    L = [verb, content_md5, content_type, date]
    if headers:
        L.extend(headers)
    L.append('/%s/%s' % (bucket, obj) if bucket else '/%s' % obj)
    str_to_sign = '\n'.join(L)
    h = hmac.new(access_key_secret, str_to_sign, sha)
    return base64.b64encode(h.digest())

_METHOD_MAP = dict(
        GET=lambda: 'GET',
        DELETE=lambda: 'DELETE',
        PUT=lambda: 'PUT')

def _mid(s, start_tag, end_tag, from_pos=0):
    '''
    Search string s to find substring starts with start_tag and ends with end_tag.

    Returns:
        The substring and next search position.
    '''
    n1 = s.find(start_tag, from_pos)
    if n1==(-1):
        return '', -1
    n2 = s.find(end_tag, n1 + len(start_tag))
    if n2==(-1):
        return '', -1
    return s[n1 + len(start_tag) : n2], n2 + len(end_tag)

def _httprequest(host, verb, path, payload, headers):
    data = None
    if payload:
        data = payload if isinstance(payload, str) else payload.read()
    opener = urllib2.build_opener(urllib2.HTTPHandler)
    request = urllib2.Request('http://%s%s' % (host, path), data=data)
    request.get_method = _METHOD_MAP[verb]
    if data:
        request.add_header('Content-Length', len(data))
    for k, v in headers.iteritems():
        request.add_header(k, v)
    try:
        response = opener.open(request)
        if verb=='GET':
            return response.read()
    except urllib2.HTTPError, e:
        xml = e.read()
        code = _mid(xml, '<Code>', '</Code>')[0]
        if code=='TemporaryRedirect':
            endpoint = _mid(xml, '<Endpoint>', '</Endpoint>')[0]
            # resend http request:
            logging.warn('resend http request to endpoint: %s' % endpoint)
            return _httprequest(endpoint, verb, path, payload, headers)
        msg = _mid(xml, '<Message>', '</Message>')[0]
        raise StorageError(code, msg)

def _api(access_key_id, access_key_secret, verb, bucket, obj, payload=None, headers=None):
    host = '%s.s3.amazonaws.com' % bucket if bucket else 's3.amazonaws.com'
    path = '/%s' % obj
    date = _current_datetime()
    content_md5 = ''
    content_type = '' if verb=='GET' else _guess_content_type(obj)
    authorization = _signature(access_key_id, access_key_secret, bucket, obj, verb, content_md5, content_type, date)
    if headers is None:
        headers = dict()
    if content_type:
        headers['Content-Type'] = content_type
    headers['Date'] = date
    headers['Authorization'] = 'AWS %s:%s' % (access_key_id, authorization)
    r = _httprequest(host, verb, path, payload, headers)
    if verb=='PUT':
        return (bucket, obj)
    return r

if __name__ == '__main__':
    import doctest
    doctest.testmod()
