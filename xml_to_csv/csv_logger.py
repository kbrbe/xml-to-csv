import logging
from datetime import datetime
import csv
import os

MESSAGE_TYPES = {
  'CONFIG_ERROR': 'script_configuration_error',
  'MULTIPLE_API_RESULTS': 'multiple_api_results',
  'EMPTY_API_RESPONSE': 'no_api_response',
  'INVALID_VALUE': 'invalid_value',
}

class CSVFileHandler(logging.Handler):
    def __init__(self, filename, logLevel, delimiter=",", filemode='w', writeHeader=True):
        logging.Handler.__init__(self)
        
        # Set up the CSV file and writer
        self.filename = filename
        self.filemode = filemode
        self.logFile = None
        self.delimiter = delimiter
        self.logLevel = logLevel
        self.writeHeader = writeHeader
        self._open_file()

    def _open_file(self):
        # Open the file in write mode ('w'), and create a CSV writer
        self.logFile = open(self.filename, mode=self.filemode, newline='', encoding='utf-8')
        self.writer = csv.writer(self.logFile, delimiter=self.delimiter, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        
        # Write the header (optional, can be omitted if not needed)
        if self.writeHeader:
          self.writer.writerow(['time', 'level', 'name', 'message_type', 'identifier', 'message'])

    def emit(self, record):
        try:
            timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
            # Split the log entry by comma, assuming a simple format: [time, level, name, message]
            identifierString = record.__dict__['identifier'] if 'identifier' in record.__dict__ else ''
            messageType = record.__dict__['message_type'] if 'message_type' in record.__dict__ else ''
            log_data = [f'{timestamp}.{int(record.msecs):03d}', record.levelname, record.name, messageType, identifierString, record.getMessage()]
            # Write the log data to the CSV file
            self.writer.writerow(log_data)
        except Exception as e:
            self.handleError(record)

    def close(self):
        # Close the file when done
        if self.logFile:
            self.logFile.close()
