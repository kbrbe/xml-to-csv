#
# (c) 2024 Sven Lieber
# KBR Brussels
#
import lxml.etree as ET
import os
import sys
import json
import itertools
import enchant
import hashlib
import csv
import re
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from argparse import ArgumentParser
from tqdm import tqdm
import xml_to_csv.utils as utils
import stdnum

NS_MARCSLIM = 'http://www.loc.gov/MARC21/slim'
ALL_NS = {'marc': NS_MARCSLIM}


# -----------------------------------------------------------------------------
def main(inputFilenames, outputFilename, configFilename, prefix):
  """This script reads XML files in and extracts several fields to create CSV files."""


  with open(configFilename, 'r') as configFile:
    config = json.load(configFile)
  
  recordTag = getRecordTagName(config)
  recordTagString = config['recordTagString']

  outputFolder = os.path.dirname(outputFilename)
  
  with open(outputFilename, 'w') as outFile:


    # Create a dictionary with file pointers
    # Because ExitStack is used, it is as of each of the file pointers has their own "with" clause
    # This is necessary, because the selected columns and thus possible output file pointers are variable
    # In the code we cannot determine upfront how many "with" statements we would need
    with ExitStack() as stack:
      files = utils.create1NOutputWriters(config, outputFolder, prefix)

      outputFields = [config["recordIDColumnName"]] + [f["columnName"] for f in config["dataFields"]]
      outputWriter = csv.DictWriter(outFile, fieldnames=outputFields, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      
      # write the CSV header for the output file
      outputWriter.writeheader()

      # write the CSV header for the per-column output files (1:n relationships)
      if prefix != "":
        for filename, fileHandle  in files.items():
          fileHandle.writeheader()

      pbar = tqdm(position=0)
      updateFrequency=10000
      batchSize=40000
      config['counters'] = {
        'batchCounter': 0,
        'recordCounter': 0,
        'fileCounter': 0,
        'filteredRecordCounter': 0,
        'filteredRecordExceptionCounter': 0
      }

      for inputFilename in inputFilenames:
        if inputFilename.endswith('.xml'):
          config['counters']['fileCounter'] += 1

          print(type(recordTag))
          recordNamespace = recordTag.namespace
          recordName = recordTag.localname

          recordPrefixTag = None
          if recordNamespace in ALL_NS.values():
            prefix = list(ALL_NS)[list(ALL_NS.values()).index(recordNamespace)]
            recordPrefixTag = f'{prefix}:{recordName}'

          # use record tag string, because for finding the positions there is no explicit namespace
          # later for record parsing we should use the namespace-agnostic name
          print(f'recordPrefixTag: {recordPrefixTag}')
          positions = utils.find_record_positions(inputFilename, recordTagString, chunkSize=1024*1024)

          #print(f'len of positions = {len(positions)}')
          print(positions[0:20])

          # The first 6 arguments are related to the fast_iter function
          # everything afterwards will directly be given to processRecord
          utils.fast_iter_batch(inputFilename, positions, processRecord, recordTag, pbar, config, updateFrequency, batchSize, outputWriter, files, prefix)



# -----------------------------------------------------------------------------
def getValueList(elem, config, configKey):

  datePatterns = config["datePatterns"]
  keyParts = []

  # first check if we can extract the data we should extract
  #
  if configKey not in config:
    print(f'No key "{configKey}" in config!')
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
              for subfieldConfig in subfieldConfigEntries:
                subfieldColumnName = subfieldConfig['columnName']
                subfieldExpression = subfieldConfig['expression']
                subfieldValueType = subfieldConfig['valueType']

                # we are not doing recursive calls here
                if subfieldValueType == 'json':
                  print(f'type "json" not allowed for subfields')
                  continue
                subfieldValues = v.xpath(subfieldExpression, namespaces=ALL_NS)

                # a subfield should not appear several times
                # if it does, print a warning and concatenate output instead of using an array
                #
                subfieldDelimiter = ';'
                if len(subfieldValues) > 1:
                  print(f'Warning: multiple values for subfield {subfieldColumnName} in record {recordID} (concatenated with {subfieldDelimiter})')
                subfieldTextValues = [s.text for s in subfieldValues]
                allSubfieldsData[subfieldColumnName] = subfieldDelimiter.join(subfieldTextValues)

              # the dictionary of subfield lists becomes the JSON value of this column
              recordData[columnName] = allSubfieldsData
            else:
              print(f'JSON specified, but no subfields given')
          else:
            # other value types require to analyze the text content
            utils.extractFieldValue(v.text, valueType, recordData[columnName])
        else:
          print(f'No valueType given!')
    
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
def processRecord(elem, config, outputWriter, files, prefix):

  if "recordFilter" in config:
    try:
      if not utils.passFilter(elem, config["recordFilter"]):
        config['counters']['filteredRecordCounter'] += 1
        return None
    except Exception as e:
        recordID = utils.getElementValue(elem.find(config['recordIDExpression'], ALL_NS))
        config['counters']['filteredRecordExceptionCounter'] += 1
        return None

  recordData = getValueList(elem, config, "dataFields")
  outputWriter.writerow(recordData)

  # Create a CSV output file for each selected columns to resolve 1:n relationships
  if prefix != "":
    recordID = recordData[config["recordIDColumnName"]]
    for columnName, valueList in recordData.items():
      if valueList and columnName != config["recordIDColumnName"]:
        if isinstance(valueList, list):
          # simple 1:n relationship: one row per value
          for v in valueList:
            files[columnName].writerow({config["recordIDColumnName"]: recordID, columnName: v})
        elif isinstance(valueList, dict):
          # complex 1:n relationship: one row per value, but subfields require multiple columns
          valueList.update({config["recordIDColumnName"]: recordID})
          files[columnName].writerow(valueList)
          

# -----------------------------------------------------------------------------
def parseArguments():

  parser = ArgumentParser(description='This script reads an XML file in MARC slim format and extracts several fields to create a CSV file.')
  parser.add_argument('inputFiles', nargs='+', help='The input files containing XML records')
  parser.add_argument('-c', '--config-file', action='store', required=True, help='The config file with XPath expressions to extract')
  parser.add_argument('-p', '--prefix', action='store', required=False, default='', help='If given, one file per column with this prefix will be generated to resolve 1:n relationships')
  parser.add_argument('-o', '--output-file', action='store', required=True, help='The output CSV file containing extracted fields based on the provided config')
  args = parser.parse_args()

  return args


if __name__ == '__main__':
  args = parseArguments()
  main(args.inputFiles, args.output_file, args.config_file, args.prefix)
