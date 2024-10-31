import unittest
import json
import re
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
        with open("date-mapping.json", "r") as file:
            cls.config = json.load(file)

    def test_compile_pattern(self):
        # Test a specific pattern compilation
        pattern_str = self.config["rules"]["before_month_year"]["pattern"]
        compiled_pattern = utils.compile_pattern(pattern_str, self.config["components"])
        
        # Ensure that the pattern matches as expected
        test_string = "before November 1980"
        match = re.match(compiled_pattern, test_string)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(0), test_string)

    def test_parseComplexDate(self):
        # Test complex date parsing
        monthMapping = utils.buildMonthMapping(self.config)
        result = utils.parseComplexDate("before November 1980 and after April 1978", self.config, monthMapping)
        
        # Check the result to match EDTF format expectation
        self.assertEqual(result, "1978-04/1980-11")


