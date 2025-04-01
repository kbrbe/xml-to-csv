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
def getValueList(elem, config, configKey, dateConfig, monthMapping):
  """This function extracts all values from the XML element elem according to the config
  Example output: {'isni': '', 'bnf': '', 'name': [{'name': 'Hayashi Motoharu'}], 'birthDate': [{'birthDate': '1858', 'rule': 'simplePattern'}], 'deathDate': [{'deathDate': None}], 'birthPlace': [{'birthTown': 'Osaka', 'birthCountry': 'Japon'}], 'deathPlace': '', 'autID': '6840'}

  * One dictionary key per "column" of the config.
  * empty strings for empty text values
  * subkeys for type date
  * date subkey with same name as parent key, but additional rule
  * date can have only one subkey which is none
  * in this example subkeys for places, because it is of type json

  """

  keyParts = []

  # first check if we can extract the data we should extract
  #
  if configKey not in config:
    logger.error(f'No key "{configKey}" in config!', extra={'message_type': csv_logger.MESSAGE_TYPES['CONFIG_ERROR']})
    return None

  recordID = utils.getElementValue(elem.find(config['recordIDExpression'], ALL_NS))

  # initialize the dictionary for the output CSV of this record
  recordData = {f["columnName"]: [] for f in config["dataFields"]}
  recordData[config["recordIDColumnName"]] = recordID

  # check each datafield description config entry
  #
  for p in config[configKey]:
    expression = p['expression']
    columnName = p['columnName']

    # extract the data by using xpath
    #
    values = elem.xpath(expression, namespaces=ALL_NS)

    # process all extracted data (possibly more than one value)
    #
    if values:
      for v in values: 

        if 'valueType' in p:
          valueType = p['valueType']

          if valueType == 'json':
            if "subfields" in p:
              subfieldConfigEntries = p['subfields']
              allSubfieldsData = {f["columnName"]: [] for f in subfieldConfigEntries}

              # collect subfield data in a dictionary
              #
              atLeastOneValue = False
              for subfieldConfig in subfieldConfigEntries:
                subfieldColumnName = subfieldConfig['columnName']
                subfieldExpression = subfieldConfig['expression']
                subfieldValueType = subfieldConfig['valueType']

                # we are not doing recursive calls here
                if subfieldValueType == 'json':
                  logger.error(f'type "json" not allowed for subfields', extra={'message_type': csv_logger.MESSAGE_TYPES['CONFIG_ERROR']})
                  continue
                subfieldValues = v.xpath(subfieldExpression, namespaces=ALL_NS)

                # a subfield should not appear several times
                # if it does, print a warning and concatenate output instead of using an array
                #
                subfieldDelimiter = ';'
                if len(subfieldValues) > 1:
                  logger.warning(f'multiple values for subfield {subfieldColumnName} in record {recordID} (concatenated with {subfieldDelimiter})', extra={'message_type': csv_logger.MESSAGE_TYPES['CONFIG_ERROR']})
                subfieldTextValues = [s.text for s in subfieldValues if s.text is not None]
          
                if subfieldTextValues:
                  atLeastOneValue = True
                allSubfieldsData[subfieldColumnName] = subfieldDelimiter.join(subfieldTextValues)
         
              if atLeastOneValue:
                # add the current dictionary of subfield lists to the value of this column
                # https://github.com/kbrbe/xml-to-csv/issues/13
                recordData[columnName].append(allSubfieldsData)
            else:
              logger.error(f'JSON specified, but no subfields given', extra={'message_type': csv_logger.MESSAGE_TYPES['CONFIG_ERROR']})
          else:
            # other value types require to analyze the text content
            # parsedValue could be None, this should handled appropriately
            parsedValue = utils.extractFieldValue(v.text, valueType, recordID, config, dateConfig, monthMapping, columnName)

            # add original value for current data field if necessary
            if "keepOriginal" in p and p["keepOriginal"] == "true":
              originalColumnName = utils.getOriginalColumnName(p)

              # bad practice: different types of return values
              # temporarily solution to additionally get parsing rule for dates
              if isinstance(parsedValue, dict):               
                dictToAppend = parsedValue
                dictToAppend.update({originalColumnName: v.text})
                recordData[columnName].append(dictToAppend)
              elif parsedValue is not None:
                # elif instead of else to avoid processing parsedValues that are None
                recordData[columnName].append({columnName: parsedValue, originalColumnName: v.text})
            else:
              # check if we did not already add the exact same name already (https://github.com/kbrbe/xml-to-csv/issues/14)
              # no keepOriginal check, because we don't expect this for names (possible bad practice to fix?)
              existingValues = [colDict[columnName] for colDict in recordData[columnName]]
              if parsedValue not in existingValues:
                # bad practice: different types of return values
                # temporarily solution to additionally get parsing rule for dates
                if isinstance(parsedValue, dict):               
                  dictToAppend = parsedValue
                  recordData[columnName].append(dictToAppend)
                else:
                  recordData[columnName].append({columnName: parsedValue})

        else:
          logger.error(f'No valueType given!', extra={'message_type': csv_logger.MESSAGE_TYPES['CONFIG_ERROR']})
    
  recordData = {k:"" if not v else v for k,v in recordData.items()}
  return recordData

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

  recordData = getValueList(elem, config, "dataFields", dateConfig, monthMapping)

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
