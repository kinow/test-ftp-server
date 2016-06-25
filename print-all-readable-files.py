#!/usr/bin/env python

import sys

import argparse
import ftplib

parser = argparse.ArgumentParser(description='Test File Creation in FTP servers')
parser.add_argument('--server', help='Server name or IP address', required=True)
parser.add_argument('--port', help='Server port', type=int, default=21)
parser.add_argument('--timeout', help='Timeout in seconds', type=int, default=90)
parser.add_argument('--max-depth', help='Max depth to recursively traverse folders', type=int, default=3)

import logging
from pprint import pprint
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

from util import MyDirWalker

# @see http://stackoverflow.com/questions/1854572/traversing-ftp-listing
def traverse(ftp, max_depth, stack, depth=0):
    if depth > max_depth:
        logging.debug('depth > ' + str(max_depth))
        return
    walker = MyDirWalker()
    ftp.retrlines('LIST', walker.visit)
    for entry in (path for path in walker if path not in ('.', '..')):
        try:
            if len(stack) > 0:
                current_directory = '/' + '/'.join(stack) + entry
                logging.info(current_directory)
            
            ftp.cwd(entry)
            stack.append(entry)
            traverse(ftp, max_depth, stack, depth + 1)
            stack.pop()
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

    try:
        traverse(ftp, max_depth, [])
    except:
        logging.error("Unexpected error:", sys.exc_info()[0])
        sys.exit(1)

    logging.info('Closing connection')
    ftp.quit()
    ftp.close()
    logging.info('Bye')

if __name__ == '__main__':
    main()
