#!/usr/bin/env python

import sys

import argparse
import ftplib

parser = argparse.ArgumentParser(description='Test File Creation in FTP servers')
parser.add_argument('--server', help='Server name or IP address')
parser.add_argument('--port', help='Server port', type=int, default=21)
parser.add_argument('--timeout', help='Timeout in seconds', type=int, default=90)
parser.add_argument('--max-depth', help='Max depth to recursively traverse folders', type=int, default=3)

import logging
from pprint import pprint
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')

import os
import uuid

from util import MyDirWalker

# @see http://stackoverflow.com/questions/1158076/implement-touch-using-python
def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)

# @see http://stackoverflow.com/questions/1854572/traversing-ftp-listing
def traverse(ftp, max_depth, depth=0):
    if depth > max_depth:
        logging.debug('depth > ' + str(max_depth))
        return
    walker = MyDirWalker()
    ftp.retrlines('LIST', walker.visit)
    for entry in (path for path in walker if path not in ('.', '..')):
        try:
            ftp.cwd(entry)
            traverse(ftp, max_depth, depth + 1)
            
            ftp.cwd('..')
        except ftplib.error_perm:
            pass
        except:
            pass

def main():
    args       = parser.parse_args()
    server     = args.server
    port       = args.port
    timeout    = args.timeout
    max_depth  = args.max_depth

    logging.info('Connecting to %s:%d' % (server, port))

    ftp = ftplib.FTP()
    ftp.connect(host=server, port=port, timeout=timeout)
    ftp.login()
    ftp.set_pasv(True)

    local_path = os.path.dirname(os.path.realpath(__file__))
    uid = uuid.uuid4()
    temp_file = local_path + '/' + str(uid)
    logging.debug("Touching temp file %s" % temp_file)

    try:
        touch(temp_file)
    except:
        logging.error("Unexpected error:", sys.exc_info()[0])
        sys.exit(1)

    try:
        traverse(ftp, max_depth)
    except:
        logging.error("Unexpected error:", sys.exc_info()[0])
        sys.exit(1)

    try:
        logging.debug("Deleting temp file...")
        os.remove(temp_file)
    except:
        pass

    logging.info('Closing connection')
    ftp.quit()
    ftp.close()
    logging.info('Bye')

if __name__ == '__main__':
    main()
