#
# (c) 2024 Sven Lieber
# KBR Brussels
#
import lxml.etree as ET
import os
import json
import itertools
import enchant
import hashlib
import csv
from contextlib import ExitStack
from argparse import ArgumentParser
from tqdm import tqdm
import xml_to_csv.utils as utils
import stdnum

NS_MARCSLIM = 'http://www.loc.gov/MARC21/slim'
ALL_NS = {'marc': NS_MARCSLIM}


# -----------------------------------------------------------------------------
def main(inputFilename, outputFilename, configFilename, prefix):
  """This script reads an XML file in MARC slim format and extracts several fields to create a CSV file."""


  with open(configFilename, 'r') as configFile:
    config = json.load(configFile)
  
  recordTag = getRecordTagName(config)
  
  with open(outputFilename, 'w') as outFile:


    # Create a dictionary with file pointers
    # Because ExitStack is used, it is as of each of the file pointers has their own "with" clause
    # This is necessary, because the selected columns and thus possible output file pointers are variable
    # In the code we cannot determine upfront how many "with" statements we would need
    with ExitStack() as stack:
      files = { columnName : csv.DictWriter(open(f'{prefix}-{columnName}.csv', 'w'), fieldnames=[config["recordIDColumnName"], columnName], delimiter=',') for columnName in [field["columnName"] for field in config["dataFields"]] }

      outputFields = [config["recordIDColumnName"]] + [f["columnName"] for f in config["dataFields"]]
      outputWriter = csv.DictWriter(outFile, fieldnames=outputFields, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      
      # write the CSV header for the output file
      outputWriter.writeheader()

      # write the CSV header for the per-column output files (1:n relationships)
      if prefix != "":
        for filename, fileHandle  in files.items():
          fileHandle.writeheader()

      pbar = tqdm(position=0)

      context = ET.iterparse(inputFilename, events=('start', 'end'))
      #
      # The first 6 arguments are related to the fast_iter function
      # everything afterwards will directly be given to processRecord
      updateFrequency=1000
      config['counters'] = {
        'recordCounter': 0,
        'filteredRecordCounter': 0,
        'filteredRecordExceptionCounter': 0
      }
      utils.fast_iter(context, processRecord, recordTag, pbar, config, updateFrequency, outputWriter, files, prefix)




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
    values = None

    # extract the data by using xpath
    #
    values = elem.xpath(expression, namespaces=ALL_NS)

    # process all extracted data (possibly more than one value)
    #
    if values:
      for v in values:

        if 'valueType' in p:
          if valueType = 'json':
          else:
            # other value types require to analyze the text content
            vText = v.text
            vNorm = None
            if vText:
              # parse different value types, for example dates or regular strings
              #
              valueType = p['valueType']
              if valueType == 'date':
                vNorm = utils.parseDate(vText, datePatterns)
                recordData[columnName].append(vNorm)
              elif valueType == 'text':
                recordData[columnName].append(vText)
              elif valueType == 'isniURL':
                isniComponents = vText.split('isni.org/isni/')
                if len(isniComponents) > 1:
                  vNorm = isniComponents[1]
                  recordData[columnName].append(vNorm)
                else:
                  print(f'Warning: malformed ISNI URL for authority {recordID}: "{vText}"')
              elif valueType == 'bnfURL':
                bnfComponents = vText.split('ark:/12148/')
                if len(bnfComponents) > 1:
                  vNorm = bnfComponents[1]
                  recordData[columnName].append(vNorm)
                else:
                  print(f'Warning: malformed BnF URL for authority {recordID}: "{vText}"')
              
              else:
                print(f'Unknown value type "{valueType}"')

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
        #print(f'{recordID} {e}')
        config['counters']['filteredRecordExceptionCounter'] += 1
        return None

  recordData = getValueList(elem, config, "dataFields")
  outputWriter.writerow(recordData)

  # Create a CSV output file for each selected columns to resolve 1:n relationships
  if prefix != "":
    recordID = recordData[config["recordIDColumnName"]]
    for columnName, valueList in recordData.items():
      if valueList and columnName != config["recordIDColumnName"]:
        for v in valueList:
          files[columnName].writerow({config["recordIDColumnName"]: recordID, columnName: v})
          

# -----------------------------------------------------------------------------
def parseArguments():

  parser = ArgumentParser(description='This script reads an XML file in MARC slim format and extracts several fields to create a CSV file.')
  parser.add_argument('-i', '--input-file', action='store', required=True, help='The input file containing MARC SLIM XML records')
  parser.add_argument('-c', '--config-file', action='store', required=True, help='The config file with XPath expressions to extract')
  parser.add_argument('-p', '--prefix', action='store', required=False, default='', help='If given, one file per column with this prefix will be generated to resolve 1:n relationships')
  parser.add_argument('-o', '--output-file', action='store', required=True, help='The output CSV file containing extracted fields based on the provided config')
  args = parser.parse_args()

  return args


if __name__ == '__main__':
  args = parseArguments()
  main(args.input_file, args.output_file, args.config_file, args.prefix)
