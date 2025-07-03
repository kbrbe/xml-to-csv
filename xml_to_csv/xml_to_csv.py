#
# (c) 2024 Sven Lieber
# KBR Brussels
#
import lxml.etree as ET
import os
import sys
import json
import itertools
import logging
import hashlib
import csv
import re
import xml_to_csv.csv_logger as csv_logger
from xml_to_csv.csv_logger import CSVFileHandler
from contextlib import ExitStack
from argparse import ArgumentParser
from tqdm import tqdm
import xml_to_csv.utils as utils

NS_MARCSLIM = 'http://www.loc.gov/MARC21/slim'
ALL_NS = {'marc': NS_MARCSLIM}

LOGGER_NAME = "XML_TO_CSV.utils"
logger = logging.getLogger(LOGGER_NAME)

# -----------------------------------------------------------------------------
def main(inputFilenames, outputFilename, configFilename, dateConfigFilename, prefix, incrementalProcessing, logLevel='INFO', logFile=None):
  """This script reads XML files in and extracts several fields to create CSV files."""


  # read the config file
  #
  with open(configFilename, 'r') as configFile:
    config = json.load(configFile)

  # read the date config file
  #
  with open(dateConfigFilename, 'r') as dateConfigFile:
    dateConfig = json.load(dateConfigFile)
  
  # build a single numeric month lookup data structure
  monthMapping = utils.buildMonthMapping(dateConfig)

  setupLogging(logLevel, logFile)

  outputFolder = os.path.dirname(outputFilename)
  
  with open(outputFilename, 'w') as outFile:


    # Create a dictionary with file pointers
    # Because ExitStack is used, it is as of each of the file pointers has their own "with" clause
    # This is necessary, because the selected columns and thus possible output file pointers are variable
    # In the code we cannot determine upfront how many "with" statements we would need
    with ExitStack() as stack:
      files = utils.create1NOutputWriters(config, outputFolder, prefix)

      # define columns for the output based on config
      outputFields = [config["recordIDColumnName"]]
      for f in config["dataFields"]:
        if "keepOriginal" in f and f["keepOriginal"] == "true":
          columnNameOriginal = utils.getOriginalColumnName(f)
          outputFields.append(columnNameOriginal)
        outputFields.append(f["columnName"])

      outputWriter = csv.DictWriter(outFile, fieldnames=outputFields, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      
      # write the CSV header for the output file
      outputWriter.writeheader()

      # write the CSV header for the per-column output files (1:n relationships)
      if prefix != "":
        for filename, fileHandle  in files.items():
          fileHandle.writeheader()

      pbar = tqdm(position=0)


      # update progress bar every x records
      updateFrequency=5000

      config['counters'] = {
        'batchCounter': 0,
        'recordCounter': 0,
        'fileCounter': 0,
        'filteredRecordCounter': 0,
        'filteredRecordExceptionCounter': 0
      }
  
      # used for namespace-agnostic extraction of XML-parsed records
      recordTag = getRecordTagName(config)

      if incrementalProcessing:
        # used for initial string-based identification of start/end position of records
        recordTagString = config['recordTagString']

        # chunk and batch size can be configured per data source, hence part of the config
        #
        chunkSize = int(config["execution"]["byteChunkSize"]) if "execution" in config and "byteChunkSize" in config["execution"] else 1024*1024
        batchSize = int(config["execution"]["recordBatchSize"]) if "execution" in config and "recordBatchSize" in config["execution"] else 40000



      for inputFilename in inputFilenames:
        if inputFilename.endswith('.xml'):
          config['counters']['fileCounter'] += 1

          if incrementalProcessing:
            logger.info(f'incremental processing ...')

            # use record tag string, because for finding the positions there is no explicit namespace
            # later for record parsing we should use the namespace-agnostic name
            positions = utils.find_record_positions(inputFilename, recordTagString, chunkSize=chunkSize)

            # The first 6 arguments are related to the fast_iter function
            # everything afterwards will directly be given to processRecord
            utils.fast_iter_batch(inputFilename, positions, processRecord, recordTag, pbar, config, dateConfig, monthMapping, updateFrequency, batchSize, outputWriter, files, prefix)

          else:
            logger.info(f'regular iterative processing ...')

            context = ET.iterparse(inputFilename, tag=recordTag)
            utils.fast_iter(
              context, # the XML context
              processRecord, # the function that is called for every found recordTag
              pbar, # the progress bar that should be updated
              config, # configuration object with counters and other data
              dateConfig, # configuration object for date parsing
              monthMapping, # lookup of calendar months
              updateFrequency, # after how many records the progress bar should be updated
              outputWriter, # paramter for processRecord: CSV writer for main output file
              files, # parameter for processRecord: dictionary of CSV writers for each column 1:n relationships
              prefix # parameter for processRecord: prefix for output files
            )


# -----------------------------------------------------------------------------
def setupLogging(logLevel, logFile):

  logFormat = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
  if logFile:
    logger = logging.getLogger(LOGGER_NAME)
    # Debug: Print current handlers
    csvHandler = CSVFileHandler(logFile, logLevel=logLevel, delimiter=',', filemode='w')
    logger.addHandler(csvHandler)
  else:
    logging.basicConfig(level=logLevel, format=logFormat)
    logger = logging.getLogger(LOGGER_NAME)

# -----------------------------------------------------------------------------
def getRecordTagName(config):

  recordTagString = config['recordTag']
  recordTag = None
  if ':' in recordTagString:
    prefix, tagName = recordTagString.split(':')
    recordTag = ET.QName(ALL_NS[prefix], tagName)
  else:
    recordTag = recordTagString

  return recordTag


# -----------------------------------------------------------------------------
def processRecord(elem, config, dateConfig, monthMapping, outputWriter, files, prefix):

  if "recordFilter" in config:
    try:
      if not utils.passFilter(elem, config["recordFilter"]):
        config['counters']['filteredRecordCounter'] += 1
        return None
    except Exception as e:
        recordID = utils.getElementValue(elem.find(config['recordIDExpression'], ALL_NS))
        config['counters']['filteredRecordExceptionCounter'] += 1
        return None

  recordData = utils.getValueList(elem, config, "dataFields", dateConfig, monthMapping)

  identifierPrefix = config["recordIDPrefix"] if "recordIDPrefix" in config else ''

  # (1) write output to the general CSV file
  outputRow = {config["recordIDColumnName"]: identifierPrefix + recordData[config["recordIDColumnName"]]}
  for columnName, extractedValues in recordData.items():
    if columnName != config["recordIDColumnName"]:
      outputRow[columnName] = []
      if extractedValues:
        # there are one or more results for this column
        for valueDict in extractedValues:
          if columnName in valueDict and 'rule' not in valueDict:
            # the result contains a subfield with the same name as the column
            # i.e. not type json, but a regular column with possible original
            for valueColumnName, singleValue in valueDict.items():
              singleValue = singleValue if singleValue else ''
              if valueColumnName in outputRow:
                outputRow[valueColumnName].append(singleValue)
              else:
                outputRow[valueColumnName] = [singleValue]

          else:
            # the result contains subfields (i.e. type json), write as-is
            outputRow[columnName].append(valueDict)
      else:
        outputRow[columnName] = ''
  outputWriter.writerow(outputRow)

  # (2) Create a CSV output file for each selected columns to resolve 1:n relationships
  if prefix != "":
    recordID = recordData[config["recordIDColumnName"]]

    for columnName, valueList in recordData.items():

      if valueList and columnName != config["recordIDColumnName"]:

        # simple 1:n relationship: one row per value
        # but it is one dictionary per relationship, 
        # because we eventually have the parsed value and the original value
        for v in valueList:
          # skip if none, e.g. if {"birthDate": {"birthDate": None} }
          if any(v.values()):
            outputRow = v
            outputRow.update({config["recordIDColumnName"]: identifierPrefix + recordID})
            files[columnName].writerow(outputRow)

        #if isinstance(valueList, list):
        #  pass
        #elif isinstance(valueList, dict):
        #  # complex 1:n relationship: one row per value, but subfields require multiple columns
        #  # this data comes from valueType JSON
        #  valueList.update({config["recordIDColumnName"]: recordID})
        #  files[columnName].writerow(valueList)
          

# -----------------------------------------------------------------------------
def parseArguments():

  parser = ArgumentParser(description='This script reads an XML file in MARC slim format and extracts several fields to create a CSV file.')
  parser.add_argument('inputFiles', nargs='+', help='The input files containing XML records')
  parser.add_argument('-c', '--config-file', action='store', required=True, help='The config file with XPath expressions to extract')
  parser.add_argument('-d', '--date-config-file', action='store', required=True, help='The config file for date parsing')
  parser.add_argument('-p', '--prefix', action='store', required=False, default='', help='If given, one file per column with this prefix will be generated to resolve 1:n relationships')
  parser.add_argument('-o', '--output-file', action='store', required=True, help='The output CSV file containing extracted fields based on the provided config')
  parser.add_argument('-i', '--incremental', action='store_true', help='Optional flag to indicate if the input files should be read incremental (identifying records with string-parsing in chunks and parsing XML records in batch)')
  parser.add_argument('-l', '--log-file', action='store', help='The optional name of the logfile')
  parser.add_argument('-L', '--log-level', action='store', default='INFO', help='The log level, default is INFO')
  args = parser.parse_args()

  return args


if __name__ == '__main__':
  args = parseArguments()
  main(args.inputFiles, args.output_file, args.config_file, args.date_config_file, args.prefix, args.incremental, logLevel=args.log_level, logFile=args.log_file)
