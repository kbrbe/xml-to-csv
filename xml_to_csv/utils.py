from datetime import datetime
import time
import gc
import lxml.etree as ET
import unicodedata as ud
import logging
from io import BytesIO
import csv
import os
import re
from . import csv_logger as csv_logger

NS_MARCSLIM = 'http://www.loc.gov/MARC21/slim'
ALL_NS = {'marc': NS_MARCSLIM}

LOGGER_NAME = "XML_TO_CSV.utils"
logger = logging.getLogger(LOGGER_NAME)

# -----------------------------------------------------------------------------
def updateProgressBar(pbar, config, updateFrequency):
  """This function updates the given progress bar based on the given update frequency."""

  message = "##### xml_to_csv #####"
  if "recordFilter" in config:
    passedFilter = config['counters']['recordCounter'] - config['counters']['filteredRecordCounter']
    pbar.set_description(f'{message} files: {config["counters"]["fileCounter"]}; batches: {config["counters"]["batchCounter"]}; records total: {config["counters"]["recordCounter"]}; passed filter: {passedFilter}; not passed filter: {config["counters"]["filteredRecordCounter"]}; could not apply filter: {config["counters"]["filteredRecordExceptionCounter"]}')
  else:
    pbar.set_description(f'{message} files: {config["counters"]["fileCounter"]}; batches: {config["counters"]["batchCounter"]}; records total: {config["counters"]["recordCounter"]}')
  pbar.update(updateFrequency)

# -----------------------------------------------------------------------------
def create_batches(positions, batch_size):
    """Splits the list of position tuples into batches."""
    batches = []
    for i in range(0, len(positions), batch_size):
        batches.append(positions[i:i + batch_size])
    return batches

# -----------------------------------------------------------------------------
def read_chunk(filename, start, end):
    """Reads a chunk of the file from start to end positions."""
    with open(filename, 'rb') as file:
        file.seek(start)
        return file.read(end - start)

# -----------------------------------------------------------------------------
def fast_iter_batch(inputFilename, positions, func, tagName, pbar, config, dateConfig, monthMapping, updateFrequency=100, batchSize=100, *args, **kwargs):
  """
  Adapted from http://stackoverflow.com/questions/12160418

  This function calls "func" for each parsed record with name "tagName".
  All name parameters of this function are used to initialize and update a progress bar.
  Other non-keyword arguments (args) and keyword arguments (kwargs) are provided to "func".
  """

  # disable automatic garbage collection as we will do it manually
  gc.disable()

  # Given all the start/end positions of records, create larger batches containing multiple records
  batches = create_batches(positions, batchSize)
   
  for batch in batches:
    config["counters"]["batchCounter"] += 1
    start = batch[0][0]  # Start of the first tuple in the batch
    end = batch[-1][1]   # End of the last tuple in the batch

    # Read the chunk of the file from the beginning of the batch to the end of the batch
    chunk_data = read_chunk(inputFilename, start, end)

    # we need to store the byte stream in a variable so we can clear it later
    bytesStream = BytesIO(b'<collection>' + chunk_data + b'</collection>')

    # only fire for end events (default) and additionally only fire for tagName elements
    context = ET.iterparse(bytesStream, tag=tagName)

    try:
      # We assume that context is configured to only fire 'end' events for tagName
      #
      for event, record in context:
        # call the given function and provide it the given parameters
        func(record, config, dateConfig, monthMapping, *args, **kwargs)

        config['counters']['recordCounter'] += 1

        # clear to save RAM
        record.clear()

        # delete preceding siblings to save memory (https://lxml.de/3.2/parsing.html)
        while record.getprevious() is not None:
          del record.getparent()[0]

        if config['counters']['recordCounter'] % updateFrequency == 0:
          updateProgressBar(pbar, config, updateFrequency)

      # free up RAM after parsing all recors of the batch
      bytesStream.close()
      gc.collect()
    except Exception as e:
      logger.error(f'batch processing error for tuple ({start},{end})')
      sys.exit(0)

    # update the remaining count after the loop has ended
    updateProgressBar(pbar, config, updateFrequency)

    # We are done
    del context

  # re-enable automatic gargabe collection
  gc.enable()


# -----------------------------------------------------------------------------
def fast_iter(context, func, pbar, config, dateConfig, monthMapping, updateFrequency=100, *args, **kwargs):
  """
  Adapted from http://stackoverflow.com/questions/12160418

  This function calls "func" for each parsed record in context.
  All name parameters of this function are used to initialize and update a progress bar.
  Other non-keyword arguments (args) and keyword arguments (kwargs) are provided to "func".
  """

  # We assume that context is configured to only fire 'end' events for tagName
  #
  for event, record in context:

    # call the given function and provide it the given parameters
    func(record, config, dateConfig, monthMapping, *args, **kwargs)

    # Update progress bar
    config['counters']['recordCounter'] += 1

    # clear to save RAM
    record.clear()
    # delete preceding siblings to save memory (https://lxml.de/3.2/parsing.html)
    while record.getprevious() is not None:
      del record.getparent()[0]

    if config['counters']['recordCounter'] % updateFrequency == 0:
      updateProgressBar(pbar, config, updateFrequency)

  # update the remaining count after the loop has ended
  updateProgressBar(pbar, config, updateFrequency)

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

  An exception is thrown if the date could not be parsed with the given patterns
  >>> parseDate('25/04/1988', ['%Y-%m-%d', '%Y/%m/%d'])
  Traceback (most recent call last):
      ...
  Exception: Could not parse date "25/04/1988" with the given patterns "['%Y-%m-%d', '%Y/%m/%d']"

  A correct date string for dates without delimiter.
  >>> parseDate('19880425', ['%Y-%m-%d', '%Y%m%d'])
  '1988-04-25'

  Only year and month are invalid.
  >>> parseDate('1988-04', ['%Y%m', '%Y-%m'])
  Traceback (most recent call last):
      ...
  Exception: Could not parse date "1988-04" with the given patterns "['%Y%m', '%Y-%m']"

  >>> parseDate('198804', ['%Y-%m', '%Y%m'])
  Traceback (most recent call last):
      ...
  Exception: Could not parse date "198804" with the given patterns "['%Y-%m', '%Y%m']"

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

  >>> parseDate('780', ['%Y'])
  '780'

  >>> parseDate('93', ['%Y'])
  '93'

  >>> parseDate('19340417', ["%Y", "(%Y)", "[%Y]", "%Y-%m-%d", "%Y--%m-%d", "%Y--%m--%d", "%d/%m/%Y", "%Y/%m/%d", "%Y%m%d", "%Y----", "%Y.%m.%d", "%d.%m.%Y"])
  '1934-04-17'

  >>> parseDate('1980----', ["%Y", "(%Y)", "[%Y]", "%Y-%m-%d", "%Y--%m-%d", "%Y--%m--%d", "%d/%m/%Y", "%Y/%m/%d", "%Y%m%d", "%Y----", "%Y.%m.%d", "%d.%m.%Y"])
  '1980'

  >>> parseDate('1233?', ["%Y", "(%Y)", "[%Y]", "%Y-%m-%d", "%Y--%m-%d", "%Y--%m--%d", "%d/%m/%Y", "%Y/%m/%d", "%Y%m%d", "%Y----", "%Y.%m.%d", "%d.%m.%Y"]) 
  Traceback (most recent call last):
      ...
  Exception: Could not parse date "1233?" with the given patterns "['%Y', '(%Y)', '[%Y]', '%Y-%m-%d', '%Y--%m-%d', '%Y--%m--%d', '%d/%m/%Y', '%Y/%m/%d', '%Y%m%d', '%Y----', '%Y.%m.%d', '%d.%m.%Y']"

  """

  # handle years before the year 1000
  if len(date) < 4 and date.isdigit():
    date = date.zfill(4)

  parsedDate = None
  for p in patterns:

    try:
      # try if the value is a year
      tmp = datetime.strptime(date, p).date()
      if len(date) <= 4:
        parsedDate = str(tmp.year)
      elif len(date) > 4 and len(date) <= 7:
        if any(ele in date for ele in ['(', '[', ')', ']', '.']):
          parsedDate = str(tmp.year)
        else:
          parsedDate = None
      elif len(date) == 8 and date.endswith('----'):
        parsedDate = str(tmp.year)
      else:
        parsedDate = str(tmp)
      break
    except ValueError:
      pass

  if parsedDate == None:
    raise Exception(f'Could not parse date "{date}" with the given patterns "{patterns}"')
  else:
    return parsedDate

# -----------------------------------------------------------------------------
def roman_to_century(roman_numeral):
    roman_map = {
        'I': 1,
        'V': 5,
        'X': 10,
        'L': 50,
        'C': 100,
        'D': 500,
        'M': 1000
    }
    
    total = 0
    prev_value = 0
    
    for char in reversed(roman_numeral):
        value = roman_map[char]
        if value < prev_value:
            total -= value
        else:
            total += value
        prev_value = value
        
    century = (total - 1) // 100 + 1  # Convert to century
    return f"{century}00"  # Return in the form of '17xx'

# -----------------------------------------------------------------------------
def compile_pattern(pattern, components):
    """Replaces placeholders in the pattern with actual regex components.
    >>> config = {
    ...    "components": {
    ...      "keywords": {
    ...        "before": r"\b(?:before|avant|Avant)\b",
    ...        "after": r"\b(?:after|après|Après)\b",
    ...        "and": r"\b(?:and|et|Et)\b",
    ...      },
    ...      "months": {
    ...        "English": { "November": "11", "April": "04" }
    ...      },
    ...      "year": r"(\d{4})",
    ...      "roman_numeral": r"(X{0,3}|IX|V?I{0,3})"
    ...    },
    ...    "rules": {
    ...      "roman_century": {
    ...          "pattern": r"%(roman_numeral)s(e|e|ème|e siècle)",
    ...          "template": "%s~"
    ...       },
    ...      "before_year": { "pattern": r"%(keywords.before)s\s+%(year)s", "template": "%s~" },
    ...      "range_with_and": { "pattern": r"%(keywords.before)s\s+%(month)s\s+%(year)s\s+%(keywords.and)s\s+%(keywords.after)s\s+%(month)s\s+%(year)s", "template": "%s-%s/%s-%s"}
    ...    }
    ... }
    >>> pattern = compile_pattern(r"%(keywords.before)s\\s+%(year)s", config["components"])
    >>> re.match(pattern, "Avant 2023").group(0)
    'Avant 2023'

    >>> pattern = compile_pattern(r"%(keywords.before)s\\s+(%(months.generic)s)\\s+%(year)s\\s+%(keywords.and)s\\s+%(keywords.after)s\\s+(%(months.generic)s)\\s+%(year)s", config["components"])
    >>> re.match(pattern, "Before November 2004 and after April 2002").group(0)
    'Before November 2004 and after April 2022'

    """ 
    placeholders = re.findall(r'%\(([^)]+)\)s', pattern)
    component_map = {}

    for ph in placeholders:
        if ph == 'months.generic':
            # Create a regex for all month names across languages
            month_patterns = []
            for language, months in components['months'].items():
                month_patterns.extend(months.keys())
            month_names = '|'.join(re.escape(getNormalizedDateString(month)) for month in month_patterns)
            component_map[ph] = rf"\b{month_names}"
        else:
            keys = ph.split('.')
            if len(keys) == 2:
                main_key, sub_key = keys
                component_map[ph] = components[main_key][sub_key]
            else:
                component_map[ph] = components.get(ph)

            if component_map[ph] is None:
              raise ValueError(f'Missing component for {ph} in date configuration')

    # Replace placeholders in the pattern
    return pattern % component_map

# -----------------------------------------------------------------------------
def buildMonthMapping(config):
  """ Creates a single lookup data structure for multilingual month names.
  >>> buildMonthMapping({'components': {'months': {'English': {'January': '01', 'November': '11'}, 'German': {'Januar': '01', 'November': '11'} }}})
  {'january': '01', 'november': '11', 'januar': '01'}
  """
  monthMapping = {}
  for lang, langDict in config['components']['months'].items():
    for monthString, monthNumerical in langDict.items():
      if monthString not in monthMapping:
        monthMapping[getNormalizedDateString(monthString)] = monthNumerical
  return monthMapping

# -----------------------------------------------------------------------------
def getNumericMonth(monthString, monthMapping):
  """ Gets the numerical representation of a month string.
  >>> getNumericMonth("November", {"Januar": "01", "November": "11"})
  '11'
  >>> getNumericMonth("December", {"Januar": "01", "November": "11"})
  Traceback (most recent call last):
      ...
  KeyError: 'No numerical value for month "December" found in config'
  """
  if monthString in monthMapping:
    return monthMapping[monthString]
  else:
    raise KeyError(f'No numerical value for month "{monthString}" found in config')

# -----------------------------------------------------------------------------
def parseComplexDate(input_str, config, monthMapping):
    """Parse a date string based on the provided configuration.
    
    Args:
        input_str (str): The date string to parse.
        config (dict): The configuration dictionary containing components and rules.

    Returns:
        str: The standardized date in EDTF format, or None if no match is found.

    >>> config = {
    ...    "components": {
    ...      "keywords": {
    ...        "before": r"\\b(?:before|avant|Avant)\\b",
    ...        "after": r"\\b(?:after|après|Après)\\b",
    ...        "and": r"\\b(?:and|et|Et)\\b",
    ...      },
    ...      "months": {
    ...        "English": { "November": "11", "April": "04" }
    ...      },
    ...      "year": r"(\d{4})"
    ...    },
    ...    "rules": {
    ...      "before_year": { "pattern": r"%(keywords.before)s\s+%(year)s", "template": "%s~" },
    ...      "range_with_and_month": { "pattern": r"%(keywords.before)s\s+(%(months.generic)s)\s+%(year)s\s+%(keywords.and)s\s+%(keywords.after)s\s+(%(months.generic)s)\s+%(year)s", "template": "%s-%s/%s-%s"}
    ...    }
    ... }
    >>> parseComplexDate("avant 1980", config, {"november": "11", "april": "04"})
    ('[..1980]', 'before_year')
    >>> parseComplexDate("before November 1980 and after April 1978", config, {"november": "11", "april": "04"})
    ('november-1980/april-1978', 'range_with_and_month')
    """
    # Normalize input
    norm_input = getNormalizedDateString(input_str)

    results = []

    #print(f'input string: {norm_input}')
    for rule_name, rule in config["rules"].items():
        pattern_str = compile_pattern(rule["pattern"], config["components"])
        pattern = re.compile(pattern_str, re.IGNORECASE)
        match = pattern.search(norm_input)

        if match:
            #print(f'rule "{rule_name}" match with pattern "{pattern_str}" for input string "{norm_input}". Groups are {match.groups()}')

            # Check for more specific rules first
            if rule_name == "range_with_and_written_month":
                before_year = match.group(4)  # Year before "and"
                after_year = match.group(2)  # Year after "and"
                before_month = match.group(3)  # Month before "and"
                after_month = match.group(1)  # Month after "and"
                
                beforeMonthNumeric = getNumericMonth(before_month, monthMapping)
                afterMonthNumeric = getNumericMonth(after_month, monthMapping)
                # Use the template for formatting
                result_str = rule["template"] % (before_year, beforeMonthNumeric, after_year, afterMonthNumeric)
                return result_str, rule_name

            elif rule_name == "range_with_and_year":
                before_year = match.group(1)  # Year before "and"
                after_year = match.group(2)  # Year after "and"
                
                # Use the template for formatting
                result_str = rule["template"] % (before_year, after_year)
                return result_str, rule_name

            elif rule_name == "before_written_month_year":
                year = match.group(2) 
                month = match.group(1) 
                monthNumeric = getNumericMonth(month, monthMapping)
                return f"[..{year}-{monthNumeric}]", rule_name

            elif rule_name == "before_dash_date":
                year = match.group(3) 
                month = match.group(2) 
                day = match.group(1)
                return f"[..{year}-{month}-{day}]", rule_name

            elif rule_name == "years_slash_abbreviation":
                year = match.group(1) 
                alternateYearAbbreviation = match.group(2) 
                otherYear = year[:-len(alternateYearAbbreviation)] + alternateYearAbbreviation
                return f"[{year},{otherYear}]", rule_name

            elif rule_name == "written_month_year":
                year = match.group(2) 
                month = match.group(1) 
                monthNumeric = getNumericMonth(month, monthMapping)
                return f"{year}-{monthNumeric}", rule_name

            elif rule_name == "after_written_month_year":
                year = match.group(2) 
                month = match.group(1)
                monthNumeric = getNumericMonth(month, monthMapping)
                return f"[{year}-{monthNumeric}..]", rule_name

            elif rule_name == "before_year":
                year = match.group(1)
                return f"[..{year}]", rule_name

            elif rule_name == "roman_century":
                roman_numeral = match.group(1)  # Capture the Roman numeral
                century_str = roman_to_century(roman_numeral)
                return century_str, rule_name
            else:
                groups = match.groups()
                result_str = rule["template"] % groups
                return result_str, rule_name
            

    return None, None


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
def getNormalizedDateString(s):
  """This function returns a normalized copy of the given string.

  >>> getNormalizedDateString("HeLlO")
  'hello'
  >>> getNormalizedDateString("judaïsme, islam, christianisme, ET sectes apparentées")
  'judaisme, islam, christianisme, et sectes apparentees'
  >>> getNormalizedDateString("chamanisme, de l’Antiquité…)")
  'chamanisme, de lantiquite...)'

  """
  # by the way: only after asci normalization the UTF character for ... becomes ...
  asciiNormalized = ud.normalize('NFKD', s).encode('ASCII', 'ignore').lower().strip().decode("utf-8")

  return asciiNormalized
 
  
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
def extractFieldValue(value, valueType, recordID, config, dateConfig, monthMapping, columnName):

  vNorm = None
  if value:
    # parse different value types, for example dates or regular strings
    #
    value = value.strip()

    if valueType == 'date':
      parsedDate, parsingRule = handleTypeDate(recordID, value, dateConfig, monthMapping)
      # only add dates to the output that were parsed by any rule, otherwise they are part of the log
      # the handleTypeDate function will log it
      # vNorm will stay None and the calling function should handle it properly
      if parsedDate is not None:
        vNorm = {columnName: parsedDate, "rule": parsingRule}

    elif valueType == 'text':
      vNorm = value

    elif valueType == 'isniURL':
      vNorm = handleTypeISNIURL(recordID, value)

    elif valueType == 'bnfURL':
      vNorm = handleTypeBnFURL(recordID, value)

    else:
      logger.error(f'Unknown value type in config "{valueType}", should be "date", "text", "isniURL", or "bnfURL"', extra={'message_type': csv_logger.MESSAGE_TYPES['SCRIPT_ERROR']})

  return vNorm

# -----------------------------------------------------------------------------
def handleTypeDate(recordID, value, dateConfig, monthMapping):

  datePatterns = dateConfig['datePatterns']

  vNorm = None
  rule = 'placeholder_value'
  try:
    vNorm = parseDate(value, datePatterns)
    rule = 'simplePattern'
  except Exception as e:
    # if the following is not true we simply go to the return statement that will have the default rule 'placeholder_value'
    if value.replace('-','') == '': 
      logger.warning(f'{recordID}: placeholder value "{value}" found instead of real data', extra={'identifier': recordID, 'message_type': csv_logger.MESSAGE_TYPES['INVALID_VALUE']})
    if not value.replace('-','') == '': 
      vNorm, rule  = parseComplexDate(value, dateConfig, monthMapping)
      # log a warning, but also ensure that the value will not become part of the output
      if not vNorm:
        logger.error(f'{recordID}: no match with parseDate or parseComplexDate for {value}', extra={'identifier': recordID, 'message_type': csv_logger.MESSAGE_TYPES['INVALID_VALUE']})
        
  return vNorm, rule

# -----------------------------------------------------------------------------
def handleTypeISNIURL(recordID, value):

  vNorm = None
  isniComponents = value.split('isni.org/isni/')
  if len(isniComponents) > 1:
    vNorm = isniComponents[1]
  else:
    logger.warning(f'record {recordID}: malformed ISNI URL "{value}"', extra={'identifier': recordID, 'message_type': csv_logger.MESSAGE_TYPES['INVALID_VALUE']})

  return vNorm

# -----------------------------------------------------------------------------
def handleTypeBnFURL(recordID, value):

  vNorm = None
  bnfComponents = value.split('ark:/12148/')
  if len(bnfComponents) > 1:
    vNorm = bnfComponents[1]
  else:
    logger.warning(f'record {recordID}: malformed BnF URL "{value}"', extra={'identifier': recordID, 'message_type': csv_logger.MESSAGE_TYPES['INVALID_VALUE']})

  return vNorm

# -----------------------------------------------------------------------------
def getOriginalColumnName(columnConfig):
  return columnConfig["columnName"] + "-original"

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
      if "keepOriginal" in field and field["keepOriginal"] == "true":
        allColumnNames.append(getOriginalColumnName(field))
      if field["valueType"] == 'date':
        allColumnNames.append('rule')
    outputFilename = os.path.join(outputFolder, f'{prefix}-{columnName}.csv')
    outputWriters[field["columnName"]] = csv.DictWriter(open(outputFilename, 'w'), fieldnames=allColumnNames, delimiter=',') 

  return outputWriters

# -----------------------------------------------------------------------------
def find_record_positions(filename, tagName, chunkSize=1024*1024):
    """
    Find the start and end positions of records in a large XML file.

    Parameters:
    - filename: The path to the large XML file.
    - tagName: The tag of the records to locate.
    - chunkSize: The size of each chunk to read from the file.

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
            chunk = file.read(chunkSize)
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

    return positions

# -----------------------------------------------------------------------------
def needs_encoding_fixing(text):
    try:
        # Attempt a round-trip encode-decode cycle
        encoded = text.encode('latin1')
        decoded = encoded.decode('utf-8')
        # Return True if the decoded text looks different from the original
        return text != decoded
    except (UnicodeEncodeError, UnicodeDecodeError):
        # Likely already correct UTF-8
        return False

# -----------------------------------------------------------------------------
def fix_encoding(text):
    try:
        # Decode from Latin-1 (or Windows-1252) and re-encode as UTF-8
        fixed_text = text.encode('latin1').decode('utf-8')
        return fixed_text
    except UnicodeDecodeError:
        # Return the original text if decoding fails
        return text

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

  Testing that text values are correctly encoded
  >>> config0 = {"recordIDExpression": "./id", "recordIDColumnName": "id", "dataFields": [{"columnName": "alternateName", "expression": "./alternateName", "valueType": "text"}]}
  >>> configKey = "dataFields"
  >>> elem0 = ET.fromstring("<record><id>1</id><alternateName>Ecole des Pays-Bas mÃ©ridionaux</alternateName></record>")
  >>> getValueList(elem0, config0, configKey, {}, {})
  {'alternateName': [{'alternateName': 'Ecole des Pays-Bas méridionaux'}], 'id': '1'}

  Testing that subfield text values are correctly encoded
  >>> config1 = {"recordIDExpression": "./id", "recordIDColumnName": "id", "dataFields": [{"columnName": "name", "expression": "./name", "valueType": "json", "subfields": [{"columnName": "lastName", "expression": "./lastName", "valueType": "text"}]}]}
  >>> elem1 = ET.fromstring("<record><id>1</id><name><firstName>Jean</firstName><lastName>MÃ©ridionaux</lastName></name></record>")
  >>> getValueList(elem1, config1, configKey, {}, {})
  {'name': [{'lastName': 'Méridionaux'}], 'id': '1'}
  """

  keyParts = []

  # first check if we can extract the data we should extract
  #
  if configKey not in config:
    logger.error(f'No key "{configKey}" in config!', extra={'message_type': csv_logger.MESSAGE_TYPES['CONFIG_ERROR']})
    return None

  recordID = getElementValue(elem.find(config['recordIDExpression'], ALL_NS))

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
                subfieldTextValues = [fix_encoding(s.text) if needs_encoding_fixing(s.text) else s.text for s in subfieldValues if s.text is not None]
          
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
            if needs_encoding_fixing(v.text):
              v.text = fix_encoding(v.text)
            parsedValue = extractFieldValue(v.text, valueType, recordID, config, dateConfig, monthMapping, columnName)

            # add original value for current data field if necessary
            if "keepOriginal" in p and p["keepOriginal"] == "true":
              originalColumnName = getOriginalColumnName(p)

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
if __name__ == "__main__":
  import doctest
  doctest.testmod()
