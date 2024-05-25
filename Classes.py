import datetime
import uuid

class Column:
    def __init__(self, name, rows=[]):
        self.name = name
        self.rows = rows

class ColumnFamily:
    def __init__(self, name, columns=None):
        self.name = name
        self.columns = columns if columns else []

class Value:
    def __init__(self, value):
        self.creationDate = datetime.datetime.now().timestamp()
        self.value = value

class Cell:
    def __init__(self, columnFamily, column, value, rowKey):
        self.rowKey = rowKey
        self.values = [Value(value)]

    def update(self, newValue):
        self.values.append(Value(newValue))

class Table:
    def __init__(self, columnFamilies=None):
        self.columnFamilies = columnFamilies if columnFamilies else []
        self.cells = []
        self.rowKeyCounter = 0

    def generateRowKey(self):
        # Using UUID for unique row keys
        return str(uuid.uuid4())

    def insertOne(self, rowData):
        pass