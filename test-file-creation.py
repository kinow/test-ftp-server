#!/usr/bin/env python

import argparse

parser = argparse.ArgumentParser(description='Test File Creation in FTP servers')
parser.add_argument('--server', help='Server Name or IP Address')
parser.add_argument('--port', help='Server Port (defaults to 22)', type=int, default=22)


def main():
    args = parser.parse_args()
    print(args.port)

if __name__ == '__main__':
    main()
