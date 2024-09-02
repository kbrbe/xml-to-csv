import unittest

firstThreeStartPositions=[15,66,117]
lastThreeStartPositions = [372,423,474]

class PositionTestCases():

  # ---------------------------------------------------------------------------
  def testCorrectNumberOfStartPositionsOnlyRecords110(self):
    (positions, numberRecords, firstThreeStartPositions, lastThreeStartPositions) = self.getPositionsChunk110()
    numberFound = len(positions)
    numberExpected = 10
    self.assertEqual(numberFound, numberExpected, msg=f'Found {numberFound} instead of {numberExpected}')

  # ---------------------------------------------------------------------------
  def testCorrectNumberOfStartPositionsMixedCollection110(self):
    (positions, numberRecords, firstThreeStartPositions, lastThreeStartPositions) = self.getPositionsChunk110()
    numberFound = len(positions)
    self.assertEqual(numberFound, numberRecords, msg=f'Found {numberFound} instead of {numberRecords}')


  # ---------------------------------------------------------------------------
  def testCorrectFirstThreeStartPositions(self):
    (positions, numberRecords, firstThreeStartPositions, lastThreeStartPositions) = self.getPositionsChunk110()
    positionsFound = [positions[0][0], positions[1][0], positions[2][0]]
    self.assertEqual(positionsFound, firstThreeStartPositions, msg=f'Found start positions {positionsFound} instead of {firstThreeStartPositions}')

  # ---------------------------------------------------------------------------
  def testCorrectFirstThreeStartPositionsWithStartCut(self):
    (positions, numberRecords, firstThreeStartPositions, lastThreeStartPositions) = self.getPositionsChunk110()
    positionsFound = [positions[0][0], positions[1][0], positions[2][0]]
    self.assertEqual(positionsFound, firstThreeStartPositions, msg=f'Found start positions {positionsFound} instead of {firstThreeStartPositions}')

  # ---------------------------------------------------------------------------
  def testCorrectFirstThreeStartPositionsWithEndCut(self):
    (positions, numberRecords, firstThreeStartPositions, lastThreeStartPositions) = self.getPositionsChunk110()
    positionsFound = [positions[0][0], positions[1][0], positions[2][0]]
    self.assertEqual(positionsFound, firstThreeStartPositions, msg=f'Found start positions {positionsFound} instead of {firstThreeStartPositions}')

  # ---------------------------------------------------------------------------
  def testCorrectFirstThreeStartPositionsInOnego(self):
    (positions, numberRecords, firstThreeStartPositions, lastThreeStartPositions) = self.getPositionsChunk1500()
    positionsFound = [positions[0][0], positions[1][0], positions[2][0]]
    self.assertEqual(positionsFound, firstThreeStartPositions, msg=f'Found start positions {positionsFound} instead of {firstThreeStartPositions}')





  # ---------------------------------------------------------------------------
  def testCorrectLastThreeStartPositions(self):
    (positions, numberRecords, firstThreeStartPositions, lastThreeStartPositions) = self.getPositionsChunk110()
    positionsFound = [positions[-3][0], positions[-2][0], positions[-1][0]]
    self.assertEqual(positionsFound, lastThreeStartPositions, msg=f'Found start positions {positionsFound} instead of {lastThreeStartPositions}')

  # ---------------------------------------------------------------------------
  def testCorrectLastThreeStartPositionsWithStartCut(self):
    (positions, numberRecords, firstThreeStartPositions, lastThreeStartPositions) = self.getPositionsChunk110()
    positionsFound = [positions[-3][0], positions[-2][0], positions[-1][0]]
    self.assertEqual(positionsFound, lastThreeStartPositions, msg=f'Found start positions {positionsFound} instead of {lastThreeStartPositions}')

  # ---------------------------------------------------------------------------
  def testCorrectLastThreeStartPositionsWithEndCut(self):
    (positions, numberRecords, firstThreeStartPositions, lastThreeStartPositions) = self.getPositionsChunk110()
    positionsFound = [positions[-3][0], positions[-2][0], positions[-1][0]]
    self.assertEqual(positionsFound, lastThreeStartPositions, msg=f'Found start positions {positionsFound} instead of {lastThreeStartPositions}')

  # ---------------------------------------------------------------------------
  def testCorrectLastThreeStartPositionsInOneGo(self):
    (positions, numberRecords, firstThreeStartPositions, lastThreeStartPositions) = self.getPositionsChunk1500()
    positionsFound = [positions[-3][0], positions[-2][0], positions[-1][0]]
    self.assertEqual(positionsFound, lastThreeStartPositions, msg=f'Found start positions {positionsFound} instead of {lastThreeStartPositions}')


