import csv

def getRecordsAsDict(filename):
  with open(filename, 'r') as fIn:
    reader = csv.DictReader(fIn)
    return list(reader)


