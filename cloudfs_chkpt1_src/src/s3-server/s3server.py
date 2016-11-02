'''Implementation of an S3-like storage server based on local files.

Useful to test features that will eventually run on S3, or if you want to
run something locally that was once running on S3.

We don\'t support all the features of S3, but it does work with the
standard S3 client for the most basic semantics. To use the standard
S3 client with this module:

    c = S3.AWSAuthConnection("", "", server="localhost", port=8888,
                             is_secure=False)
    c.create_bucket("mybucket")
    c.put("mybucket", "mykey", "a value")
    print c.get("mybucket", "mykey").body

'''
import time
import bisect
import datetime
import hashlib
import os
import os.path
import subprocess
import urllib
import sys
import getopt
import signal
import logging
from tornado import escape
from tornado import httpserver
from tornado import ioloop
from tornado import web
from tornado.util import bytes_type

class TrafficMonitor:

    def __init__(self):
        self.num_requests = 0
        self.num_read_bytes = 0
        self.cur_usage = 0
        self.max_usage = 0


    def debug_out(self, op):
        out = '%d %d %d %d %d %s' % (int(time.time() * 1000000), self.num_requests, self.num_read_bytes, self.cur_usage, self.max_usage, op)
        return out


    def print_out(self):
        out = 'NumRequests NumReadBytes CurrentUsage MaxUsage\n %d %d %d %d\n' % (self.num_requests, self.num_read_bytes, self.cur_usage, self.max_usage)
        return out


tmon = TrafficMonitor()

def start(port, root_directory, logger, bucket_depth = 0):
    application = S3Application(root_directory, logger, bucket_depth)
    http_server = httpserver.HTTPServer(application)
    http_server.listen(port)
    ioloop.IOLoop.instance().start()


class S3Application(web.Application):
    """Implementation of an S3-like storage server based on local files.

    If bucket depth is given, we break files up into multiple directories
    to prevent hitting file system limits for number of files in each
    directories. 1 means one level of directories, 2 means 2, etc.
    """
    def __init__(self, root_directory, logger, bucket_depth=0):
        web.Application.__init__(self, [
            (r"/", RootHandler),
            (r"/admin/(.+)", AdminHandler),
            (r"/([^/]+)/(.+)", ObjectHandler),
            (r"/([^/]+)/", BucketHandler),
        ])
        self.logger = logger
        self.directory = os.path.abspath(root_directory)
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
        self.bucket_depth = bucket_depth


class BaseRequestHandler(web.RequestHandler):
    SUPPORTED_METHODS = ("PUT", "GET", "DELETE")

    def render_xml(self, value):
        assert isinstance(value, dict) and len(value) == 1
        self.set_header("Content-Type", "application/xml; charset=UTF-8")
        name = value.keys()[0]
        parts = []
        parts.append('<' + escape.utf8(name) +
                     ' xmlns="http://doc.s3.amazonaws.com/2006-03-01">')
        self._render_parts(value.values()[0], parts)
        parts.append('</' + escape.utf8(name) + '>')
        for p in parts:
            tmon.num_read_bytes += len(p)

        self.finish('<?xml version="1.0" encoding="UTF-8"?>\n' +
                    ''.join(parts))

    def _render_parts(self, value, parts=[]):
        if isinstance(value, (unicode, bytes_type)):
            parts.append(escape.xhtml_escape(value))
        elif isinstance(value, int) or isinstance(value, long):
            parts.append(str(value))
        elif isinstance(value, datetime.datetime):
            parts.append(value.strftime("%Y-%m-%dT%H:%M:%S.000Z"))
        elif isinstance(value, dict):
            for name, subvalue in value.iteritems():
                if not isinstance(subvalue, list):
                    subvalue = [subvalue]
                for subsubvalue in subvalue:
                    parts.append('<' + escape.utf8(name) + '>')
                    self._render_parts(subsubvalue, parts)
                    parts.append('</' + escape.utf8(name) + '>')
        else:
            raise Exception("Unknown S3 value type %r", value)
        return isinstance(value, long)


    def _object_path(self, bucket, object_name):
        if self.application.bucket_depth < 1:
            return os.path.abspath(os.path.join(
                self.application.directory, bucket, object_name))
        hash = hashlib.md5(object_name).hexdigest()
        path = os.path.abspath(os.path.join(
            self.application.directory, bucket))
        for i in range(self.application.bucket_depth):
            path = os.path.join(path, hash[:2 * (i + 1)])
        return os.path.join(path, object_name)



class RootHandler(BaseRequestHandler):

    def get(self):
        tmon.num_requests += 1
        #self.application.logger.debug('S3 Server: LIST Server')
        self.application.logger.debug(tmon.debug_out('LIST Server'))
        names = os.listdir(self.application.directory)
        buckets = []
        for name in names:
            path = os.path.join(self.application.directory, name)
            info = os.stat(path)
            buckets.append({
                "Name": name,
                "CreationDate": datetime.datetime.utcfromtimestamp(
                    info.st_ctime),
            })
        self.render_xml({"ListAllMyBucketsResult": {
            "Buckets": {"Bucket": buckets},
        }})


class BucketHandler(BaseRequestHandler):

    def get(self, bucket_name):
        tmon.num_requests += 1
        #self.application.logger.debug('S3 Server: LIST Bucket %s' % bucket_name)
        self.application.logger.debug(tmon.debug_out('LIST Bucket'))
        prefix = self.get_argument("prefix", u"")
        marker = self.get_argument("marker", u"")
        max_keys = int(self.get_argument("max-keys", 50000))
        path = os.path.abspath(os.path.join(self.application.directory,
                                            bucket_name))
        terse = int(self.get_argument("terse", 0))
        if not path.startswith(self.application.directory) or \
           not os.path.isdir(path):
            raise web.HTTPError(404)
        object_names = []
        for root, dirs, files in os.walk(path):
            for file_name in files:
                object_names.append(os.path.join(root, file_name))

        skip = len(path) + 1
        for i in range(self.application.bucket_depth):
            skip += 2 * (i + 1) + 1
        object_names = [n[skip:] for n in object_names]
        object_names.sort()
        contents = []

        start_pos = 0
        if marker:
            start_pos = bisect.bisect_right(object_names, marker, start_pos)

        if prefix:
            start_pos = bisect.bisect_left(object_names, prefix, start_pos)

        truncated = False
        for object_name in object_names[start_pos:]:
            if not object_name.startswith(prefix):
                break

            if len(contents) >= max_keys:
                truncated = True
                break

            object_path = self._object_path(bucket_name, object_name)
            c = {"Key": object_name}
            if not terse:
                info = os.stat(object_path)
                c.update({
                    "LastModified": datetime.datetime.utcfromtimestamp(
                        info.st_mtime),
                    "Size": info.st_size,
                })
            contents.append(c)
            marker = object_name
        self.render_xml({"ListBucketResult": {
            "Name": bucket_name,
            "Prefix": prefix,
            "Marker": marker,
            "MaxKeys": max_keys,
            "IsTruncated": truncated,
            "Contents": contents,
        }})

    def put(self, bucket_name):
        tmon.num_requests += 1
        #self.application.logger.debug('S3 Server: CREATE Bucket %s' % bucket_name)
        self.application.logger.debug(tmon.debug_out('CREATE Bucket'))
        path = os.path.abspath(os.path.join(
            self.application.directory, bucket_name))
        if not path.startswith(self.application.directory) or \
           os.path.exists(path):
            raise web.HTTPError(403)
        os.makedirs(path)
        self.finish()


    def delete(self, bucket_name):
        tmon.num_requests += 1
        #self.application.logger.debug('S3 Server: DELETE Bucket %s' % bucket_name)
        self.application.logger.debug(tmon.debug_out('DELETE Bucket'))
        path = os.path.abspath(os.path.join(
            self.application.directory, bucket_name))
        if not path.startswith(self.application.directory) or \
           not os.path.isdir(path):
            raise web.HTTPError(404)
        if len(os.listdir(path)) > 0:
            raise web.HTTPError(403)
        os.rmdir(path)
        self.set_status(204)
        self.finish()



class AdminHandler(BaseRequestHandler):

    def get(self, func_name):
        if func_name != 'stat':
            raise web.HTTPError(404)
        func_name != 'stat'
        #self.application.logger.debug(tmon.debug_out())
        self.finish(tmon.print_out())



class ObjectHandler(BaseRequestHandler):

    def get(self, bucket, object_name):
        tmon.num_requests += 1
        #self.application.logger.debug('S3 Server: GET Object %s/%s' % (bucket, object_name))
        object_name = urllib.unquote(object_name)
        path = self._object_path(bucket, object_name)
        if not path.startswith(self.application.directory) or \
           not os.path.isfile(path):
            raise web.HTTPError(404)
        info = os.stat(path)
        self.set_header("Content-Type", "application/unknown")
        self.set_header("Last-Modified", datetime.datetime.utcfromtimestamp(
            info.st_mtime))
        tmon.num_read_bytes += os.path.getsize(path)
        self.application.logger.debug(tmon.debug_out('GET'))
        object_file = open(path, "rb")

        try:
            self.finish(object_file.read())
        finally:
            object_file.close()



    def put(self, bucket, object_name):
        tmon.num_requests += 1
        #self.application.logger.debug('S3 Server: PUT Object %s/%s' % (bucket, object_name))
        object_name = urllib.unquote(object_name)
        bucket_dir = os.path.abspath(os.path.join(
            self.application.directory, bucket))
        if not bucket_dir.startswith(self.application.directory) or \
           not os.path.isdir(bucket_dir):
            raise web.HTTPError(404)
        path = self._object_path(bucket, object_name)
        if not path.startswith(bucket_dir) or os.path.isdir(path):
            raise web.HTTPError(403)
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        tmon.cur_usage += len(self.request.body)
        tmon.max_usage = max(tmon.cur_usage, tmon.max_usage)
        self.application.logger.debug(tmon.debug_out('PUT'))
        object_file = open(path, "w")
        object_file.write(self.request.body)
        object_file.close()
        self.finish()


    def delete(self, bucket, object_name):
        tmon.num_requests += 1
        #self.application.logger.debug('S3 Server: DELETE Object %s/%s' % (bucket, object_name))
        object_name = urllib.unquote(object_name)
        path = self._object_path(bucket, object_name)
        if not path.startswith(self.application.directory) or \
           not os.path.isfile(path):
            raise web.HTTPError(404)
        tmon.cur_usage -= os.path.getsize(path)
        self.application.logger.debug(tmon.debug_out('DELETE'))
        os.unlink(path)
        self.set_status(204)
        self.finish()



def exit_handler(signum, func = None):
    #print tmon.print_out()
    exit()


def usage():
    print 'python s3server.py [options]'
    print 'options:'
    print '--port, -p: The port that s3 server listens to'
    print '--host, -h: The hostname or ip address that s3 server binds to'
    print '--target, -t: The target directory where s3 server saves the data'
    print '--log, -l: The path of logfile, default value is ./s3server.log'
    print '--verbose, -v: Enable log information'


def main():

    try:
        (opts, args) = getopt.getopt(sys.argv[1:], 'p:h:t:l:v', [
            'port=',
            'host=',
            'target=',
            'log=',
            'verbose',
            'help'])
    except getopt.GetoptError:
        err = None
        print str(err)
        usage()
        sys.exit(2)

    port = 8888
    hostname = 'localhost'
    target = '/tmp/s3'
    logfile = './s3server.log'
    verbose = False
    for (o, a) in opts:
        if o == '--help':
            usage()
            sys.exit(2)
            continue
        if o in ('-p', '--port'):
            if a.isdigit():
                port = int(a)

        a.isdigit()
        if o in ('-h', '--host'):
            hostname = a
            continue
        if o in ('-t', '--target'):
            target = a
            continue
        if o in ('-l', '--log'):
            logfile = a
            continue
        if o in ('-v', '--verbose'):
            verbose = True
            continue

    signal.signal(signal.SIGTERM, exit_handler)
    signal.signal(signal.SIGINT, exit_handler)
    logger = logging.getLogger('s3server')
    filehdlr = logging.FileHandler(logfile, mode='w')
    formatter = logging.Formatter('%(message)s')
    filehdlr.setFormatter(formatter)
    stdouthdlr = logging.StreamHandler(sys.stderr)
    stdouthdlr.setFormatter(formatter)
    logger.addHandler(filehdlr)
    logger.addHandler(stdouthdlr)
    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.DEBUG)

    try:
        os.mkdir(target)
    except:
        pass


    try:
        p = subprocess.Popen([
            'du',
            '-c',
            target], stdout = subprocess.PIPE)
        output = p.stdout.readlines()
        tmon.cur_usage = int(output[-1].split()[0])
    except:
        tmon.cur_usage = 0

    start(port, target, logger, 0)

if __name__ == '__main__':
    main()
