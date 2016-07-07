#!/usr/bin/env python  
# -*- coding: utf-8 -*-  

# system and file handling
import os
import sys
from glob import glob

# logging
import logging
FORMAT = '%(asctime)s %(message)s'
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(FORMAT)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

from pprint import pprint

# datetime
from datetime import datetime

# regular expressions
import re

class TransactionRecord(object):
    """A transaction record, found in the xfer log file"""

    def __init__(self, pid):
        self.pid = pid
        # self.datetime = None
        # self.user = None
        # self.anonymous = False
        # self.ip = None
        # self.request = None
        # self.path = None
        # self.file = None
        self.buffer = list()
        self.login = False
        self.logout = False
        self.store_or_retrieve = False
        self.path = '/'

    def add(self, datetime_object, user, anonymous, ip, request, path, file):
        """Add a new transaction record entry. This is added into the buffer, that will be spilled to the
        stream later"""

        # For STOR and RETR commands, let's append the complete path too
        complete_path = ''
        if request == 'STOR' or request == 'RETR':
            self.store_or_retrieve = True
            complete_path = os.path.join(self.path, file)
        elif request == 'LOGIN':
            self.login = True
        elif request == 'LOGOUT':
            self.logout = True
        elif request == 'CWD':
            # We are discarding CWD entries for now
            return

        # PID|DATETIME|"USER"|ANONYMOUS|IP|REQUEST|"PATH"|"FILE"|"COMPLETE_PATH"
        entry = '%s|%s|"%s"|%s|%s|%s|"%s"|"%s"|"%s"' % (
            self.pid, 
            datetime_object.strftime("%Y-%m-%d %H:%M:%S"), 
            user,
            anonymous,
            ip,
            request,
            self.path,
            file,
            complete_path
        )
        self.buffer.append(entry)

    def is_valid(self):
        valid = self.login and self.logout
        return valid

    def is_empty(self):
        empty = not self.store_or_retrieve
        return empty

    def write_to_csv_file(self, csv):
        for csv_line in self.buffer:
            csv.write(csv_line + '\n')

    def __repr__(self):
        return """Transaction(%s):
%s\n""" % (self.pid, '\n'.join(self.buffer))

class XferParser(object):
    """
    A parser for FTP XFER log files.
    """

    def __init__(self):
        # Dictionary with current Transactions. Transactions here are hold as long as they are necessary.
        # Initially it is empty, until the parser finds a PID and immediatelly puts it here.
        # Then for each entry it keeps updating the pids values, until is finds a
        # line saying that the session has been closed.
        # That's when the parser persists the PID to a CSV file.
        # The CSV is formatted as:
        # PID | DATETIME | USER | ANONYMOUS | IP | REQUEST | PATH | FILE
        # Where - represents a missing value.
        self.transactions = dict()

        self.stat_total = 0
        self.stat_empty = 0
        self.stat_discarded = 0

        # Regex for a general log line in FTPxfer file
        self.LINE_REGEX='^\s*([a-zA-Z]{3})\s+([0-9]+)\s+([0-9:]+)\s+SERVER_NAME\s+ftpd\[([0-9]+)\]\s*:\s*(.*)$'
        self.LINE_PATTERN = re.compile(self.LINE_REGEX, flags=re.IGNORECASE)

        # Regex for finding the year in the log file name
        self.FILENAME_YEAR_REGEX='.*SERVER_NAME_xfer\.log\.([\d]{4})-.*'
        self.FILENAME_YEAR_PATTERN = re.compile(self.FILENAME_YEAR_REGEX, flags=re.IGNORECASE)

        # Regex for finding user login messages
        self.LOGIN_REGEX='(anonymous )?ftp login from ([0-9\.]+)[a-zA-Z0-9\[\]\s\.]+,\s*(.*)$'
        self.LOGIN_PATTERN = re.compile(self.LOGIN_REGEX, flags=re.IGNORECASE)

        self.STOR_REGEX='STOR\s+(.*)$'
        self.STOR_PATTERN = re.compile(self.STOR_REGEX, flags=re.IGNORECASE)

        self.RETR_REGEX='RETR\s+(.*)$'
        self.RETR_PATTERN = re.compile(self.RETR_REGEX, flags=re.IGNORECASE)

        self.CWD_REGEX='CWD\s+(.*)$'
        self.CWD_PATTERN = re.compile(self.CWD_REGEX, flags=re.IGNORECASE)

    def reset(self):
        """Clear stats and reset objects"""
        self.transactions = dict()
        # FIXME: there could have some elements in the transactions dict... what to do with them???

        self.stat_total = 0
        self.stat_empty = 0
        self.stat_discarded = 0

    def print_stats(self):
        logging.info("### PARSER STATS ###")
        logging.info("Total transactions:         %d" % self.stat_total)
        logging.info("Total processed   :         %d" % (self.stat_total - (self.stat_empty + self.stat_discarded)))
        logging.info("Total empty       :         %d" % self.stat_empty)
        logging.info("Total discarded   :         %d" % self.stat_discarded)


    def gulp_directory(self, glob_pattern):
        """Parse all FTP xfer log files found in a directory, matching a given
        glob pattern."""

        for file_name in glob(glob_pattern):
            self.parse_file(file_name)

    def parse_file(self, log_input_file):
        """Parse a FTP xfer log file, line by line"""
        try:
            logging.info("Parsing file %s" % log_input_file)
            logging.debug("Finding log file year (defaults to 2016)")
            year = self._get_year(log_input_file)
            logging.debug("Log file year: %s" % year)
            with open(log_input_file, encoding='iso-8859-1') as log_file:
                csv_output_file = log_input_file + '.csv'
                with open(csv_output_file, 'w') as  csv_file:
                    for line in log_file:
                        line_matches = self.LINE_PATTERN.match(line)
                        if line_matches:
                            month = line_matches.group(1)
                            day = line_matches.group(2)
                            time = line_matches.group(3)
                            
                            pid = line_matches.group(4)
                            message = line_matches.group(5).strip()
                            datetext = '%04d%s%02d %s' % (int(year), month, int(day), time)
                            datetime_object = datetime.strptime(datetext, '%Y%b%d %H:%M:%S')

                            # Put the PID in our dictionary if not there already
                            if pid not in self.transactions:
                                self.transactions[pid] = TransactionRecord(pid)
                                self.stat_total += 1

                            self.parse_line(pid, datetime_object, message, csv_file)
                        else:
                            logging.warning("Invalid xfer log line: %s" % line)
                self.print_stats()
                self.reset()
        except Exception as e:
            logging.warning("Failed to parse file %s: %s" % (log_file, e))
            self.reset()

    def _get_year(self, file_name):
        """Get the year from a file name with pattern similar to SERVER_NAME_xfer.log.$YEAR-00.
        Default value is 2016."""
        y = 2016

        m = self.FILENAME_YEAR_PATTERN.match(file_name)
        if m:
            y = m.group(1)

        return y

    def parse_line(self, pid, datetime, message, csv_file):
        """
        Parse a single line, from an FTP xfer log file
        Args:
          pid       a ftpd process ID, not unique in a day
          datetime  a datetime Python object
          message   the log message
        """

        # Here we will check each state, and if one matches, we return.

        #add(self, datetime_object, user, anonymous, ip, request, path, file):

        # Is it a logout message?
        if message == 'FTP session closed':
            self.transactions[pid].add(datetime, '', 0, '', 'LOGOUT', '', '')

            if not self.transactions[pid].is_valid():
                self.stat_discarded += 1
            elif self.transactions[pid].is_empty():
                self.stat_empty += 1
            else:
                self.transactions[pid].write_to_csv_file(csv_file)

            del self.transactions[pid]

            return

        # Is it a login message?
        login_matches = self.LOGIN_PATTERN.match(message)
        if login_matches:
            anonymous = login_matches.group(1)
            if anonymous != None:
                anonymous = 1
            else:
                anonymous = 0
            ip = login_matches.group(2).strip()
            login = login_matches.group(3).strip()

            self.transactions[pid].add(datetime, login, anonymous, ip, 'LOGIN', '', '')
            return

        # Is it a STOR message? i.e. saving a file?
        store_matches = self.STOR_PATTERN.match(message)
        if store_matches:
            file = store_matches.group(1)
            
            self.transactions[pid].add(datetime, '', '', '', 'STOR', '', file)
            return

        # Is it a RETR message? i.e. downloading a file?
        retrieve_matches = self.RETR_PATTERN.match(message)
        if retrieve_matches:
            file = retrieve_matches.group(1)
            
            self.transactions[pid].add(datetime, '', '', '', 'RETR', '', file)
            return

        # Is it a CWD message? i.e. changing directory?
        cwd_matches = self.CWD_PATTERN.match(message)
        if cwd_matches:
            directory = cwd_matches.group(1)
            
            self.transactions[pid].add(datetime, '', '', '', 'CWD', directory, '')

            if len(directory) > 0 and directory[0] == '/':
                self.transactions[pid].path = directory
            else:
                #self.transactions[pid].path += '/' + directory
               self.transactions[pid].path = os.path.join(self.transactions[pid].path, directory)

            return

def main():
    local_dir = os.path.dirname(os.path.realpath(__file__))

    parser = XferParser()
    parser.gulp_directory(local_dir + '/SERVER_NAME_xfer.log.2015-10')
       
if __name__ == '__main__':
    main()
