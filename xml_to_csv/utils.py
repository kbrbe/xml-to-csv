from datetime import datetime
import gc
import lxml.etree as ET
import unicodedata as ud
from io import BytesIO
import enchant
import csv
import os
import re
from stdnum import isbn
from stdnum import exceptions

NS_MARCSLIM = 'http://www.loc.gov/MARC21/slim'
ALL_NS = {'marc': NS_MARCSLIM}

# -----------------------------------------------------------------------------
def updateProgressBar(pbar, config, updateFrequency):
  """This function updates the given progress bar based on the given update frequency."""

  message = "##### xml_to_csv #####"
  if "recordFilter" in config:
    passedFilter = config['counters']['recordCounter'] - config['counters']['filteredRecordCounter']
    pbar.set_description(f'{message} files: {config["counters"]["fileCounter"]}; records total: {config["counters"]["recordCounter"]}; passed filter: {passedFilter}; not passed filter: {config["counters"]["filteredRecordCounter"]}; could not apply filter: {config["counters"]["filteredRecordExceptionCounter"]}')
  else:
    pbar.set_description(f'{message} files: {config["counters"]["fileCounter"]}; records total: {config["counters"]["recordCounter"]}')
  pbar.update(updateFrequency)

def create_batches(positions, batch_size):
    """Splits the list of position tuples into batches."""
    for i in range(0, len(positions), batch_size):
        yield positions[i:i + batch_size]

def read_chunk(filename, start, end):
    """Reads a chunk of the file from start to end positions."""
    with open(filename, 'rb') as file:
        file.seek(start)
        return file.read(end - start)

# -----------------------------------------------------------------------------
def fast_iter_batch(inputFilename, positions, func, tagName, pbar, config, updateFrequency=100, batchSize=100, *args, **kwargs):
  """
  Adapted from http://stackoverflow.com/questions/12160418

  This function calls "func" for each parsed record with name "tagName".
  All name parameters of this function are used to initialize and update a progress bar.
  Other non-keyword arguments (args) and keyword arguments (kwargs) are provided to "func".
  """

  gc.disable()

  for batch in create_batches(positions, batchSize):
    start = batch[0][0]  # Start of the first tuple in the batch
    end = batch[-1][1]   # End of the last tuple in the batch

    # Read the chunk of the file corresponding to the batch
    chunk_data = read_chunk(inputFilename, start, end)
 
    print(f'tagName: {tagName}')
    context = ET.iterparse(BytesIO(b'<collection>' + chunk_data + b'</collection>'), tag=tagName)
    print(f'context: {context}')

    try:
      # We assume that context is configured to only fire 'end' events for tagName
      #
      for event, record in context:
        # call the given function and provide it the given parameters
        func(record, config, *args, **kwargs)

        # Update progress bar
        config['counters']['recordCounter'] += 1

        # clear to save RAM
        record.clear()
        # delete preceding siblings to save memory (https://lxml.de/3.2/parsing.html)
        while record.getprevious() is not None:
          del record.getparent()[0]

        if config['counters']['recordCounter'] % updateFrequency == 0:
          gc.collect()
          updateProgressBar(pbar, config, updateFrequency)
    except Exception as e:
      print(f'error for tuple ({start},{end})')
      #print(chunk_data)
      sys.exit(0)


  # update the remaining count after the loop has ended
  updateProgressBar(pbar, config, updateFrequency)

  gc.enable()

  # We are done
  del context


# -----------------------------------------------------------------------------
def fast_iter(context, func, tagName, pbar, config, updateFrequency=100, batchSize=100, *args, **kwargs):
  """
  Adapted from http://stackoverflow.com/questions/12160418

  This function calls "func" for each parsed record with name "tagName".
  All name parameters of this function are used to initialize and update a progress bar.
  Other non-keyword arguments (args) and keyword arguments (kwargs) are provided to "func".
  """

  gc.disable()

  batch = []

  # We assume that context is configured to only fire 'end' events for tagName
  #
  for event, elem in context:
    batch.append(elem)

    # process data in batches to decrease overhead
    #
    if len(batch) >= batchSize:
      for record in batch:
        # call the given function and provide it the given parameters
        func(record, config, *args, **kwargs)

        # Update progress bar
        config['counters']['recordCounter'] += 1

        # clear to save RAM
        record.clear()
        # delete preceding siblings to save memory (https://lxml.de/3.2/parsing.html)
        while record.getprevious() is not None:
          del record.getparent()[0]

      batch.clear()
      if config['counters']['recordCounter'] % updateFrequency == 0:
        gc.collect()
        updateProgressBar(pbar, config, updateFrequency)


  # Deal with the rest after we have finished parsing the input
  if batch:
    for record in batch:
        func(record, config, *args, **kwargs)
        config['counters']['recordCounter'] += 1
        record.clear()
        while record.getprevious() is not None:
            del record.getparent()[0]
    batch.clear()

  # update the remaining count after the loop has ended
  updateProgressBar(pbar, config, updateFrequency)

  gc.enable()

  # We are done
  del context


# -----------------------------------------------------------------------------
def parseDate(date, patterns):
  """"This function returns a string representing a date based on the input and a list of possible patterns.

  >>> parseDate('2021', ['%Y'])
  '2021'
  >>> parseDate('2021', ['(%Y)', '%Y'])
  '2021'
  >>> parseDate('(2021)', ['%Y', '(%Y)'])
  '2021'

  A correct date string for a correct input.
  >>> parseDate('1988-04-25', ['%Y-%m-%d'])
  '1988-04-25'

  A correct date string for dates with slash.
  >>> parseDate('25/04/1988', ['%Y-%m-%d', '%Y/%m/%d', '%Y/%m/%d', '%d/%m/%Y'])
  '1988-04-25'

  An empty value if the pattern is not found.
  >>> parseDate('25/04/1988', ['%Y-%m-%d', '%Y/%m/%d'])
  ''

  A correct date string for dates without delimiter.
  >>> parseDate('19880425', ['%Y-%m-%d', '%Y%m%d'])
  '1988-04-25'

  Only year and month are invalid.
  >>> parseDate('1988-04', ['%Y%m', '%Y-%m'])
  ''
  >>> parseDate('198804', ['%Y-%m', '%Y%m'])
  ''

  Keep year if this is the only provided information.
  >>> parseDate('1988', ['%Y-%m-%d', '%Y'])
  '1988'

  Keep year if it is in round or square brackets or has a trailing dot.
  >>> parseDate('[1988]', ['%Y', '[%Y]'])
  '1988'
  >>> parseDate('(1988)', ['(%Y)'])
  '1988'
  >>> parseDate('1988.', ['%Y', '%Y.'])
  '1988'


  """

  parsedDate = None
  for p in patterns:

    try:
      # try if the value is a year
      tmp = datetime.strptime(date, p).date()
      if len(date) == 4:
        parsedDate = str(tmp.year)
      elif len(date) > 4 and len(date) <= 7:
        if any(ele in date for ele in ['(', '[', ')', ']', '.']):
          parsedDate = str(tmp.year)
        else:
          parsedDate = ''
      else:
        parsedDate = str(tmp)
      break
    except ValueError:
      pass

  if parsedDate == None:
    return ''
  else:
    return parsedDate



# -----------------------------------------------------------------------------
def getElementValue(elem, sep=';'):
  """This function returns the value of the element if it is not None, otherwise an empty string.

  The function returns the 'text' value if there is one
  >>> class Test: text = 'hello'
  >>> obj = Test()
  >>> getElementValue(obj)
  'hello'

  It returns nothing if there is no text value
  >>> class Test: pass
  >>> obj = Test()
  >>> getElementValue(obj)
  ''

  And the function returns a semicolon separated list in case the argument is a list of objects with a 'text' attribute
  >>> class Test: text = 'hello'
  >>> obj1 = Test()
  >>> obj2 = Test()
  >>> getElementValue([obj1,obj2])
  'hello;hello'

  In case one of the list values is empty
  >>> class WithContent: text = 'hello'
  >>> class WithoutContent: text = None
  >>> obj1 = WithContent()
  >>> obj2 = WithoutContent()
  >>> getElementValue([obj1,obj2])
  'hello'
  """
  if elem is not None:
    if isinstance(elem, list):
      valueList = list()
      for e in elem:
        if hasattr(e, 'text'):
          if e.text is not None:
            valueList.append(e.text)
      return sep.join(valueList)
    else:
      if hasattr(elem, 'text'):
        return elem.text
  
  return ''

# -----------------------------------------------------------------------------
def getNormalizedString(s):
  """This function returns a normalized copy of the given string.

  >>> getNormalizedString("HeLlO")
  'hello'
  >>> getNormalizedString("judaïsme, islam, christianisme, ET sectes apparentées")
  'judaisme islam christianisme et sectes apparentees'
  >>> getNormalizedString("chamanisme, de l’Antiquité…)")
  'chamanisme de lantiquite)'

  >>> getNormalizedString("Abe Ce De ?")
  'abe ce de'
  >>> getNormalizedString("Abe Ce De !")
  'abe ce de'
  >>> getNormalizedString("Abe Ce De :")
  'abe ce de'

  >>> getNormalizedString("A. W. Bruna & zoon")
  'a w bruna & zoon'
  >>> getNormalizedString("A.W. Bruna & Zoon")
  'aw bruna & zoon'

  >>> getNormalizedString("---")
  ''

  >>> getNormalizedString("c----- leopard")
  'c leopard'
  
  """
  charReplacements = {
    '.': '',
    ',': '',
    '?': '',
    '!': '',
    ':': '',
    '-': '',
    ';': ''
  }

  # by the way: only after asci normalization the UTF character for ... becomes ...
  asciiNormalized = ud.normalize('NFKD', s).encode('ASCII', 'ignore').lower().strip().decode("utf-8")

  normalized = ''.join([charReplacements.get(char, char) for char in asciiNormalized])
  noDots = normalized.replace('...', '')
  # remove double whitespaces using trick from stackoverflow.com/questions/8270092/remove-all-whitespace-in-a-string
  return " ".join(noDots.split())
  
  
# -----------------------------------------------------------------------------
def passFilter(elem, filterConfig):
  """This function checks if the given element passes the specified filter condition.
     If the expression of the filter finds several elements, all have to pass the filter.

  The filter expression equals checks for equality
  >>> filterPseudonym = {"expression":"./datafield", "condition": "equals", "value": "p"}
  >>> elem0 = ET.fromstring("<root><datafield>p</datafield></root>")
  >>> passFilter(elem0, filterPseudonym)
  True

  >>> elem1 = ET.fromstring("<root><datafield>other value</datafield></root>")
  >>> passFilter(elem1, filterPseudonym)
  False

  An exception is thrown if the filter expression is not found
  >>> elem2 = ET.fromstring("<root><otherField>other value</otherField></root>")
  >>> passFilter(elem2, filterPseudonym)
  Traceback (most recent call last):
      ...
  Exception: Element with filter criteria not found, expected ./datafield

  An exception is thrown if multiple elements where found, but not all match the filter criteria
  >>> elem3 = ET.fromstring("<root><datafield>p</datafield><datafield>o</datafield></root>")
  >>> passFilter(elem3, filterPseudonym)
  Traceback (most recent call last):
      ...
  Exception: Multiple elements found and not all of them passed the filter: ['p', 'o'], equals p

  >>> elem4 = ET.fromstring("<root><datafield>o</datafield><datafield>p</datafield></root>")
  >>> passFilter(elem4, filterPseudonym)
  Traceback (most recent call last):
      ...
  Exception: Multiple elements found and not all of them passed the filter: ['o', 'p'], equals p

  If multiple elements where found, but all match the criteria all is good
  >>> elem5 = ET.fromstring("<root><datafield>p</datafield><datafield>p</datafield></root>")
  >>> passFilter(elem5, filterPseudonym)
  True

  The filter expression exists checks if the given element exists
  >>> filterExist = {"expression":"./datafield", "condition": "exists"}
  >>> elem6 = ET.fromstring("<root><datafield>p</datafield></root>")
  >>> passFilter(elem6, filterExist)
  True

  >>> elem7 = ET.fromstring("<root><otherField>p</otherField></root>")
  >>> passFilter(elem7, filterExist)
  False
  """

  filterExpression = filterConfig["expression"]
  condition = filterConfig["condition"]

  values = elem.xpath(filterExpression, namespaces=ALL_NS)
  if condition == "exists" or condition == "exist":
    if values:
      return True
    else:
      return False
  else:
    if values:
      filterPassed = []
      foundValues = []
      for value in values:
        foundValues.append(value.text)
        if condition == "equals" or condition == "equal":
          expectedValue = filterConfig["value"]
          if value.text == expectedValue:
            filterPassed.append(True)
          else:
            filterPassed.append(False)
          
      if all(filterPassed):
        return True
      else:
        if len(filterPassed) > 1:
          raise Exception(f'Multiple elements found and not all of them passed the filter: {foundValues}, {condition} {expectedValue}')
        else:
          return filterPassed[0]
    else:
      raise Exception(f'Element with filter criteria not found, expected {filterExpression}')

  
# -----------------------------------------------------------------------------
def extractFieldValue(value, valueType, columnData):

  vNorm = None
  if value:
    # parse different value types, for example dates or regular strings
    #
    if valueType == 'date':
      vNorm = utils.parseDate(value, datePatterns)
      columnData.append(vNorm)
    elif valueType == 'text':
      columnData.append(value)
    elif valueType == 'isniURL':
      isniComponents = value.split('isni.org/isni/')
      if len(isniComponents) > 1:
        vNorm = isniComponents[1]
        columnData.append(vNorm)
      else:
        print(f'Warning: malformed ISNI URL for authority {recordID}: "{value}"')
    elif valueType == 'bnfURL':
      bnfComponents = value.split('ark:/12148/')
      if len(bnfComponents) > 1:
        vNorm = bnfComponents[1]
        columnData.append(vNorm)
      else:
        print(f'Warning: malformed BnF URL for authority {recordID}: "{value}"')
    
    else:
      print(f'Unknown value type "{valueType}"')


# -----------------------------------------------------------------------------
def create1NOutputWriters(config, outputFolder, prefix):
  """This function returns a dictionary where each key is a column name and its value is a csv.DictWriter initialized with correct fieldnames.
     The function replaces the previous nested dictionary and list comprehension: it became to cluttered and adding subfield headings was difficult.
  """
  outputWriters = {}
  for field in config["dataFields"]:
    columnName = field["columnName"]
    if field["valueType"] == 'json':
      allColumnNames = [config["recordIDColumnName"]] + [subfield["columnName"] for subfield in field["subfields"]]
    else:
      allColumnNames = [config["recordIDColumnName"], columnName]
    outputFilename = os.path.join(outputFolder, f'{prefix}-{columnName}.csv')
    outputWriters[field["columnName"]] = csv.DictWriter(open(outputFilename, 'w'), fieldnames=allColumnNames, delimiter=',') 

  return outputWriters

# -----------------------------------------------------------------------------
def find_record_positions(filename, tagName, chunk_size=1024*1024):
    """
    Find the start and end positions of records in a large XML file.

    Parameters:
    - filename: The path to the large XML file.
    - tagName: The tag of the records to locate.
    - chunk_size: The size of each chunk to read from the file.

    Returns:
    - A list of tuples where each tuple contains the start and end byte positions of a record.
    """
    record_start_pattern = re.compile(fr'<{tagName}.*?>'.encode('utf-8'))
    record_end_pattern = re.compile(fr'</{tagName}>'.encode('utf-8'))
    
    positions = []
    current_position = 0
    buffer = b''
    pending_start = None
    started_pending = False
    last_position = (-1, -1)
    
    with open(filename, 'rb') as file:

        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break

            # Keep last buffer tail and track absolute positions
            buffer += chunk

            # Handle the case where records might be split across chunks
            if pending_start is not None:
                # Search for the end tag in the combined buffer
                end_match = record_end_pattern.search(buffer)
                if end_match:
                    end_pos = end_match.end() + current_position - len(buffer) + len(chunk)
                    if (pending_start, end_pos) != last_position:
                      positions.append((pending_start, end_pos))
                      last_position = (pending_start, end_pos)
                    pending_start = None

            # Search for start and end positions in the current buffer
            for match_start in record_start_pattern.finditer(buffer):
                if pending_start is None:
                    # If no pending start, mark the start position
                    pending_start = match_start.start() + current_position - len(buffer) + len(chunk)
                
                # Look for the corresponding end tag after the start tag
                end_pos_search_start = match_start.end()
                end_match = record_end_pattern.search(buffer, end_pos_search_start)
                if end_match:
                    # If an end tag is found, calculate the absolute position and store
                    end_pos = end_match.end() + current_position - len(buffer) + len(chunk)
                    if (pending_start, end_pos) != last_position:
                      positions.append((pending_start, end_pos))
                      last_position = (pending_start, end_pos)
                    pending_start = None

            # Update the current position to reflect the amount of the file read so far
            current_position += len(chunk)
            
            # Retain the last part of the buffer (to handle cases where tags span chunks)
            buffer_overlap = len(record_end_pattern.pattern)
            buffer = buffer[-buffer_overlap:]

    print(f'{chunk_size}: {positions}')
    return positions



# -----------------------------------------------------------------------------
if __name__ == "__main__":
  import doctest
  doctest.testmod()
