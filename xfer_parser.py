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

class XferParser(object):
    """
    A parser for FTP XFER log files.
    """

    def __init__(self):
        # Regex for a general log line in FTPxfer file
        self.LINE_REGEX='^([a-z]+)\s+([a-z]+)\s+([0-9]+)\s+([0-9:]+)\s+([\d]+)\s+([0-9]+)\s+([^s]+)\s+([0-9]+)\s+([^\s]+)\s+([a-z])\s+([a-z_])\s+([a-z])\s+([a-z])\s+([^\s]+)\s+([^\s]+)\s+([0-9])\s+([^\s]+)\s+([a-z])$'
        self.LINE_PATTERN = re.compile(self.LINE_REGEX, flags=re.IGNORECASE)

    def print_stats(self, input_file, total, processed):
        discarded           = total - processed
        processed_percent   = round((processed / total) * 100, 2)
        discarded_percet    = round((discarded / total) * 100, 2)

        logging.info("### PARSER STATS ###")
        logging.info("Input file        :         %s" % input_file)
        logging.info("Total lines       :         %d" % total)
        logging.info("Total processed   :         %d (%s %%)" % (processed, processed_percent))
        logging.info("Total discarded   :         %d (%s %%)" % (discarded, discarded_percet))

    def gulp_directory(self, glob_pattern):
        """Parse all FTP xfer log files found in a directory, matching a given
        glob pattern."""

        for file_name in glob(glob_pattern):
            self.parse_file(file_name)

    def parse_file(self, log_input_file):
        """Parse a FTP xfer log file, line by line"""
        try:
            logging.info("Parsing file %s" % log_input_file)

            lines_total         = 0
            lines_processed     = 0

            with open(log_input_file, encoding='iso-8859-1') as log_file:
                input_file_location = os.path.dirname(log_input_file)
                input_file_name     = os.path.basename(log_input_file)
                csv_output_file = input_file_location + '/output_' + input_file_name  + '.csv'
                with open(csv_output_file, 'w') as  csv_file:
                    csv_file.write("DATE|TRANSFER_SECONDS|USER_HOST|USER_NAME|USER_ID|AUTH_MODE|DIRECTION|ABSOLUTE_FILE_NAME|FILE_NAME|FILE_BYTES|TRANSFER_TYPE|ACCESS_MODE|SERVICE_NAME|COMPLETION_STATUS\n")
                    for line in log_file:
                        lines_total += 1
                        m = self.LINE_PATTERN.match(line)
                        if m:
                            lines_processed += 1
                            day_of_the_week     = m.group(1)    # discarded
                            month_name          = m.group(2)
                            day_of_month        = m.group(3)
                            time                = m.group(4)
                            year                = m.group(5)    # creates the datetime obj
                            seconds_to_transfer = m.group(6)
                            user_host_name      = m.group(7)
                            file_bytes          = m.group(8)
                            absolute_file_name  = m.group(9)
                            transfer_type       = m.group(10)
                            special_action      = m.group(11)   # discarded
                            direction           = m.group(12)
                            access_mode         = m.group(13)
                            username            = m.group(14)
                            service_name        = m.group(15)
                            authentication_mode = m.group(16)
                            user_id             = m.group(17)
                            completion_status   = m.group(18)

                            # Get datetime object, comprised of the other date-time fields
                            datetext            = '%04d%s%02d %s' % (int(year), month_name, int(day_of_month), time)
                            datetime_object     = datetime.strptime(datetext, '%Y%b%d %H:%M:%S')

                            # Get just the file name
                            file_name           = absolute_file_name
                            if absolute_file_name.count('/') > 0:
                                last_index = absolute_file_name.rfind('/') + 1
                                if last_index < len(absolute_file_name):
                                    file_name = absolute_file_name[last_index:]

                            csv_line = "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n" % (
                                datetime_object.strftime('%Y-%m-%d %H:%M:%S'),
                                seconds_to_transfer,
                                user_host_name,
                                username,
                                user_id,
                                authentication_mode,
                                direction,
                                absolute_file_name,
                                file_name,
                                file_bytes,
                                transfer_type,
                                access_mode,
                                service_name,
                                completion_status
                            )

                            csv_file.write(csv_line)
                        else:
                            logging.debug("Invalid xfer log line: %s" % line)
                self.print_stats(log_input_file, lines_total, lines_processed)
        except Exception as e:
            logging.warning("Failed to parse file %s: %s" % (log_input_file, e))

def main():
    local_dir = os.path.dirname(os.path.realpath(__file__))

    parser = XferParser()
    parser.gulp_directory(local_dir + '/xferlog*')
       
if __name__ == '__main__':
    main()
