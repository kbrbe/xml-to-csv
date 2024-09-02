import unittest

FIRST_THREE_START_POSITIONS=[15,66,117]
LAST_THREE_START_POSITIONS = [372,423,474]

class PositionTestCases():

  # ---------------------------------------------------------------------------
  def testCorrectNumberOfStartPositionsOnlyRecords110(self):
    positions = self.getPositionsChunk110()
    numberFound = len(positions)
    numberExpected = 10
    self.assertEqual(numberFound, numberExpected, msg=f'Found {numberFound} instead of {numberExpected}')

  # ---------------------------------------------------------------------------
  def testCorrectNumberOfStartPositionsMixedCollection110(self):
    positions = self.getPositionsChunk110()
    numberFound = len(positions)
    numberExpected = 10
    self.assertEqual(numberFound, numberExpected, msg=f'Found {numberFound} instead of {numberExpected}')


  # ---------------------------------------------------------------------------
  def testCorrectFirstThreeStartPositions(self):
    positions = self.getPositionsChunk110()
    positionsFound = [positions[0][0], positions[1][0], positions[2][0]]
    self.assertEqual(positionsFound, FIRST_THREE_START_POSITIONS, msg=f'Found start positions {positionsFound} instead of {FIRST_THREE_START_POSITIONS}')

  # ---------------------------------------------------------------------------
  def testCorrectFirstThreeStartPositionsWithStartCut(self):
    positions = self.getPositionsChunk110()
    positionsFound = [positions[0][0], positions[1][0], positions[2][0]]
    self.assertEqual(positionsFound, FIRST_THREE_START_POSITIONS, msg=f'Found start positions {positionsFound} instead of {FIRST_THREE_START_POSITIONS}')

  # ---------------------------------------------------------------------------
  def testCorrectFirstThreeStartPositionsWithEndCut(self):
    positions = self.getPositionsChunk110()
    positionsFound = [positions[0][0], positions[1][0], positions[2][0]]
    self.assertEqual(positionsFound, FIRST_THREE_START_POSITIONS, msg=f'Found start positions {positionsFound} instead of {FIRST_THREE_START_POSITIONS}')

  # ---------------------------------------------------------------------------
  def testCorrectFirstThreeStartPositionsInOnego(self):
    positions = self.getPositionsChunk1500()
    positionsFound = [positions[0][0], positions[1][0], positions[2][0]]
    self.assertEqual(positionsFound, FIRST_THREE_START_POSITIONS, msg=f'Found start positions {positionsFound} instead of {FIRST_THREE_START_POSITIONS}')





  # ---------------------------------------------------------------------------
  def testCorrectLastThreeStartPositions(self):
    positions = self.getPositionsChunk110()
    positionsFound = [positions[-3][0], positions[-2][0], positions[-1][0]]
    self.assertEqual(positionsFound, LAST_THREE_START_POSITIONS, msg=f'Found start positions {positionsFound} instead of {LAST_THREE_START_POSITIONS}')

  # ---------------------------------------------------------------------------
  def testCorrectLastThreeStartPositionsWithStartCut(self):
    positions = self.getPositionsChunk110()
    positionsFound = [positions[-3][0], positions[-2][0], positions[-1][0]]
    self.assertEqual(positionsFound, LAST_THREE_START_POSITIONS, msg=f'Found start positions {positionsFound} instead of {LAST_THREE_START_POSITIONS}')

  # ---------------------------------------------------------------------------
  def testCorrectLastThreeStartPositionsWithEndCut(self):
    positions = self.getPositionsChunk110()
    print(positions)
    positionsFound = [positions[-3][0], positions[-2][0], positions[-1][0]]
    self.assertEqual(positionsFound, LAST_THREE_START_POSITIONS, msg=f'Found start positions {positionsFound} instead of {LAST_THREE_START_POSITIONS}')

  # ---------------------------------------------------------------------------
  def testCorrectLastThreeStartPositionsInOneGo(self):
    positions = self.getPositionsChunk1500()
    positionsFound = [positions[-3][0], positions[-2][0], positions[-1][0]]
    self.assertEqual(positionsFound, LAST_THREE_START_POSITIONS, msg=f'Found start positions {positionsFound} instead of {LAST_THREE_START_POSITIONS}')


