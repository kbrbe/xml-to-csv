import unittest
import doctest
import json
import csv
import time
import re
import tempfile

import test.helpers as helpers
import lxml.etree as ET
import xml_to_csv.utils as utils
from test.position_test_cases import PositionTestCases

# Don't show the traceback of an AssertionError, because the AssertionError already says what the issue is!
__unittest = True


class TestOnlyWantedRecords(PositionTestCases, unittest.TestCase):
  NUMBER_RECORDS = 10
  FIRST_THREE_START_POSITIONS=[15,66,117]
  LAST_THREE_START_POSITIONS = [372,423,474]

  # ---------------------------------------------------------------------------
  def getPositionsChunk110(self):
    return (TestOnlyWantedRecords.positionsChunk110, TestOnlyWantedRecords.NUMBER_RECORDS, TestOnlyWantedRecords.FIRST_THREE_START_POSITIONS, TestOnlyWantedRecords.LAST_THREE_START_POSITIONS)

  # ---------------------------------------------------------------------------
  def getPositionsChunk200(self):
    return (TestOnlyWantedRecords.positionsChunk200, TestOnlyWantedRecords.NUMBER_RECORDS, TestOnlyWantedRecords.FIRST_THREE_START_POSITIONS, TestOnlyWantedRecords.LAST_THREE_START_POSITIONS)

  # ---------------------------------------------------------------------------
  def getPositionsChunk1500(self):
    return (TestOnlyWantedRecords.positionsChunk1500, TestOnlyWantedRecords.NUMBER_RECORDS, TestOnlyWantedRecords.FIRST_THREE_START_POSITIONS, TestOnlyWantedRecords.LAST_THREE_START_POSITIONS)

  # ---------------------------------------------------------------------------
  @classmethod
  def setUpClass(cls):
    cls.positionsChunk110 = utils.find_record_positions('test/resources/10-records.xml', 'record', chunkSize=110)

    cls.positionsChunk200 = utils.find_record_positions('test/resources/10-records.xml', 'record', chunkSize=200)

    cls.positionsChunk1500 = utils.find_record_positions('test/resources/10-records.xml', 'record', chunkSize=1500)




class TestMixedCollectionRecords(PositionTestCases, unittest.TestCase):

  NUMBER_RECORDS = 10
  FIRST_THREE_START_POSITIONS=[15,66,181]
  LAST_THREE_START_POSITIONS = [628,743,794]

  # ---------------------------------------------------------------------------
  def getPositionsChunk110(self):
    return (TestMixedCollectionRecords.positionsChunk110, TestMixedCollectionRecords.NUMBER_RECORDS, TestMixedCollectionRecords.FIRST_THREE_START_POSITIONS, TestMixedCollectionRecords.LAST_THREE_START_POSITIONS)

  # ---------------------------------------------------------------------------
  def getPositionsChunk200(self):
    return (TestMixedCollectionRecords.positionsChunk200, TestMixedCollectionRecords.NUMBER_RECORDS, TestMixedCollectionRecords.FIRST_THREE_START_POSITIONS, TestMixedCollectionRecords.LAST_THREE_START_POSITIONS)

  # ---------------------------------------------------------------------------
  def getPositionsChunk1500(self):
    return (TestMixedCollectionRecords.positionsChunk1500, TestMixedCollectionRecords.NUMBER_RECORDS, TestMixedCollectionRecords.FIRST_THREE_START_POSITIONS, TestMixedCollectionRecords.LAST_THREE_START_POSITIONS)

  # ---------------------------------------------------------------------------
  @classmethod
  def setUpClass(cls):
    cls.positionsChunk110 = utils.find_record_positions('test/resources/10-records-with-unrelated-records.xml', 'record', chunkSize=110)

    cls.positionsChunk200 = utils.find_record_positions('test/resources/10-records-with-unrelated-records.xml', 'record', chunkSize=200)

    cls.positionsChunk1500 = utils.find_record_positions('test/resources/10-records-with-unrelated-records.xml', 'record', chunkSize=1500)


class TestDateParsing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Load the config from JSON file
        with open("test/resources/date-mapping.json", "r") as file:
            cls.config = json.load(file)

    def test_compile_pattern(self):
        # Test a specific pattern compilation
        pattern_str = self.config["rules"]["before_written_month_year"]["pattern"]
        compiled_pattern = utils.compile_pattern(pattern_str, self.config["components"])
        
        # Ensure that the pattern matches as expected
        test_string = "before november 1980"
        match = re.match(compiled_pattern, test_string)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(0), test_string)

    def test_parseComplexDate(self):
        # Test complex date parsing
        monthMapping = utils.buildMonthMapping(self.config)
        result = utils.parseComplexDate("before November 1980 and after April 1978", self.config, monthMapping)
        
        # Check the result to match EDTF format expectation
        self.assertEqual(result[0], "1978-04/1980-11")
        self.assertEqual(result[1], "range_with_and_written_month")

class TestEncoding(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.testStrings = {
            'Ecole des Pays-Bas mÃ©ridionaux': 'Ecole des Pays-Bas méridionaux',
            'Milieu XVe siÃ¨cle': 'Milieu XVe siècle'
        }

    def test_encoding_fixing_detection_needed(self):
        results = {}
        for wrong, correct in TestEncoding.testStrings.items():
            results[wrong] = utils.needs_encoding_fixing(wrong)

        errors = {key: value for key, value in results.items() if value is not True}
        self.assertEqual(len(errors), 0, msg=f'The following wrongly encoded strings were not detected: {errors}')

    # cannot be part of the testStrings dict, because the way we test with 'is not True' also reports empty strings
    def test_encoding_fixing_detection_empty(self):
        self.assertFalse(utils.needs_encoding_fixing(''), msg=f'Empty input is not handled correctly')


    def test_encoding_fixing_detection_invalid_type_None(self):
        self.assertFalse(utils.needs_encoding_fixing(None), msg=f'None as input is not handled correctly')


    def test_encoding_fixing_detection_invalid_type_list(self):
        self.assertFalse(utils.needs_encoding_fixing([]), msg=f'Empty list as input is not handled correctly')

    def test_encoding_fixing_detection_invalid_type_dict(self):
        self.assertFalse(utils.needs_encoding_fixing({}), msg=f'Empty dict as input is not handled correctly')



    def test_encoding_fixing_correct(self):
        results = {}
        for wrong, correct in TestEncoding.testStrings.items():
            results[wrong] = utils.fix_encoding(wrong)

        self.assertDictEqual(TestEncoding.testStrings, results, msg='Some encoding values were not correctly fixed: {results}')

    def test_encoding_fixing_invalid_type_list(self):
        self.assertEqual(utils.fix_encoding([]), [], msg='list is not handled properly')

    def test_encoding_fixing_invalid_type_dict(self):
        self.assertEqual(utils.fix_encoding({}), {}, msg='dict is not handled properly')


class TestRecordProcessing(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.singleElementWithSingleValue = ET.fromstring("<record><id>1</id><field>value</field></record>")
        cls.singleElementWithMultipleValues = ET.fromstring("<record><id>1</id><field>value1 ; value2</field></record>")
        cls.multipleElementsWithSingleValue = ET.fromstring("<record><id>1</id><field>value1</field><field>value2</field></record>")
        cls.multipleElementsWithMultipleValues = ET.fromstring("<record><id>1</id><field>value1 ; value2</field><field>value3</field></record>")
        cls.elementWithMissingSplit = ET.fromstring("<record><id>1</id><field>value1 ; </field></record>")

        cls.multipleElementsWithSubfields = ET.fromstring("<record><id>1</id><location><place>Ghent ; Gent</place><country>Belgium ; België</country></location><location><place>Brussels</place><country>Belgium</country></location></record>")

        with open('test/resources/splitConfig.json', 'r') as configFile:
          cls.splitConfig = json.load(configFile)

        with open('test/resources/nonSplitConfig.json', 'r') as configFile:
          cls.nonSplitConfig = json.load(configFile)

        with open('test/resources/date-mapping.json', 'r') as dateConfigFile:
          cls.dateConfig = json.load(dateConfigFile)
  
        # build a single numeric month lookup data structure
        cls.monthMapping = utils.buildMonthMapping(cls.dateConfig)


    # -------------------------------------------------------------------------
    def _run_record_processing(self, xml_element, config):
        """Runs processRecord with the given XML and returns the extracted field records."""

        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as mainOutFile, \
             tempfile.NamedTemporaryFile(mode='w+', delete=False) as locationFile, \
             tempfile.NamedTemporaryFile(mode='w+', delete=False) as fieldFile:

            mainOutputWriter = csv.DictWriter(mainOutFile, fieldnames=['autID', 'field', 'location'])
            locationWriter = csv.DictWriter(fieldFile, fieldnames=['autID', 'place', 'country'])
            fieldWriter = csv.DictWriter(fieldFile, fieldnames=['autID', 'field'])

            mainOutputWriter.writeheader()
            fieldWriter.writeheader()

            utils.processRecord(
                xml_element,
                config,
                self.dateConfig,
                self.monthMapping,
                mainOutputWriter,
                {'field': fieldWriter, 'location': locationWriter},
                'prefix'
            )

            mainOutFile.flush()
            fieldFile.flush()

            # Parse and return the actual processed field data
            return helpers.getRecordsAsDict(mainOutFile.name), helpers.getRecordsAsDict(fieldFile.name)


 
    # -------------------------------------------------------------------------
    def test_record_single_field_single_value_no_split(self):

        resultMain, resultField  = self._run_record_processing(TestRecordProcessing.singleElementWithSingleValue, TestRecordProcessing.nonSplitConfig)
        
        self.assertEqual(len(resultField),1, msg='There should be only one record, but found {len(resultField)}')
        self.assertEqual(resultField[0]['field'], 'value', msg=f'Extracted value should be "value", but is {resultField[0]["field"]}')

    # -------------------------------------------------------------------------
    def test_record_single_field_multiple_values_no_split(self):

        resultMain, resultField = self._run_record_processing(TestRecordProcessing.singleElementWithMultipleValues, TestRecordProcessing.nonSplitConfig)
        
        self.assertEqual(len(resultField),1, msg='There should be only one record, but found {len(resultField)}')
        self.assertEqual(resultField[0]['field'], 'value1 ; value2', msg=f'Extracted value should be "value", but is {resultField[0]["field"]}')

    # -------------------------------------------------------------------------
    def test_record_single_field_multiple_values_split(self):

        resultMain, resultField = self._run_record_processing(TestRecordProcessing.singleElementWithMultipleValues, TestRecordProcessing.splitConfig)
        
        self.assertEqual(len(resultField),2, msg='There should be two records (from a single field), but found {len(resultField)}')
        self.assertEqual(resultField[0]['field'], 'value1', msg=f'Extracted value should be "value1", but is {resultField[0]["field"]}')
        self.assertEqual(resultField[1]['field'], 'value2', msg=f'Extracted value should be "value2", but is {resultField[1]["field"]}')


    # -------------------------------------------------------------------------
    def test_record_multiple_fields_single_value_split(self):

        resultMain, resultField = self._run_record_processing(TestRecordProcessing.multipleElementsWithSingleValue, TestRecordProcessing.splitConfig)

        self.assertEqual(len(resultField),2, msg='There should be two records (from two fields), but found {len(resultField)}')
        self.assertEqual(resultField[0]['field'], 'value1', msg=f'Extracted value should be "value1", but is {resultField[0]["field"]}')
        self.assertEqual(resultField[1]['field'], 'value2', msg=f'Extracted value should be "value2", but is {resultField[1]["field"]}')

    # -------------------------------------------------------------------------
    def test_record_multiple_fields_single_value_no_split(self):

        resultMain, resultField = self._run_record_processing(TestRecordProcessing.multipleElementsWithSingleValue, TestRecordProcessing.nonSplitConfig)

        self.assertEqual(len(resultField),2, msg='There should be two records (from two fields), but found {len(resultField)}')
        self.assertEqual(resultField[0]['field'], 'value1', msg=f'Extracted value should be "value1", but is {resultField[0]["field"]}')
        self.assertEqual(resultField[1]['field'], 'value2', msg=f'Extracted value should be "value2", but is {resultField[1]["field"]}')


    # -------------------------------------------------------------------------
    def test_record_multiple_fields_multiple_values_no_split(self):

        resultMain, resultField = self._run_record_processing(TestRecordProcessing.multipleElementsWithMultipleValues, TestRecordProcessing.nonSplitConfig)

        self.assertEqual(len(resultField),2, msg='There should be two records (from two fields), but found {len(resultField)}')
        self.assertEqual(resultField[0]['field'], 'value1 ; value2', msg=f'Extracted value should be "value1 ; value2", but is {resultField[0]["field"]}')
        self.assertEqual(resultField[1]['field'], 'value3', msg=f'Extracted value should be "value3", but is {resultField[1]["field"]}')

    # -------------------------------------------------------------------------
    def test_record_multiple_fields_multiple_values_split(self):

        resultMain, resultField = self._run_record_processing(TestRecordProcessing.multipleElementsWithMultipleValues, TestRecordProcessing.splitConfig)

        self.assertEqual(len(resultField),3, msg='There should be two records (from two fields), but found {len(resultField)}')
        self.assertEqual(resultField[0]['field'], 'value1', msg=f'Extracted value should be "value1", but is {resultField[0]["field"]}')
        self.assertEqual(resultField[1]['field'], 'value2', msg=f'Extracted value should be "value2", but is {resultField[1]["field"]}')
        self.assertEqual(resultField[2]['field'], 'value3', msg=f'Extracted value should be "value3", but is {resultField[2]["field"]}')

    # -------------------------------------------------------------------------
    def test_record_single_field_multiple_values_broken_split(self):

        resultMain, resultField = self._run_record_processing(TestRecordProcessing.elementWithMissingSplit, TestRecordProcessing.splitConfig)

        self.assertEqual(len(resultField),1, msg='There should be only one record, but found {len(resultField)}')
        self.assertEqual(resultField[0]['field'], 'value1', msg=f'Extracted value should be "value", but is {resultField[0]["field"]}')

    # -------------------------------------------------------------------------
    def test_record_multiple_fields_multiple_values_subfields_split(self):

        resultMain, resultField = self._run_record_processing(TestRecordProcessing.multipleElementsWithSubfields, TestRecordProcessing.splitConfig)
        print(resultField)
        self.assertEqual(len(resultField),4, msg='There should be four records, but found {len(resultField)}')
        self.assertEqual(resultField[0]['place'], 'Ghent', msg=f'Extracted value should be "Ghent", but is {resultField[0]["place"]}')
        self.assertEqual(resultField[0]['country'], 'Belgium', msg=f'Extracted value should be "Belgium", but is {resultField[0]["country"]}')
        self.assertEqual(resultField[1]['place'], 'Ghent', msg=f'Extracted value should be "Ghent", but is {resultField[1]["place"]}')
        self.assertEqual(resultField[1]['country'], 'België', msg=f'Extracted value should be "België", but is {resultField[1]["country"]}')

        self.assertEqual(resultField[2]['place'], 'Gent', msg=f'Extracted value should be "Ghent", but is {resultField[2]["place"]}')
        self.assertEqual(resultField[2]['country'], 'Belgium', msg=f'Extracted value should be "Belgium", but is {resultField[2]["country"]}')
        self.assertEqual(resultField[3]['place'], 'Gent', msg=f'Extracted value should be "Ghent", but is {resultField[3]["place"]}')
        self.assertEqual(resultField[3]['country'], 'België', msg=f'Extracted value should be "België", but is {resultField[3]["country"]}')




# -----------------------------------------------------------------------------
def load_tests(loader, tests, ignore):
  tests.addTests(doctest.DocTestSuite(utils, optionflags=doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS))
  return tests

