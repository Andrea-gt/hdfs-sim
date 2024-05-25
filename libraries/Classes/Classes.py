import datetime
import uuid
from typing import Dict, List, Any
import pandas as pd
import tabulate

class Column:
    def __init__(self, name, rows: Dict[str, Any]={}):
        self.name = name
        self.rows: Dict[str, 'Cell'] = {rowKey: Cell(rows[rowKey]) for rowKey in rows}

    def insertRow(self, rowKey, value):
        self.rows[rowKey] = Cell(value)

    def obtainColumnInfoWithMetadata(self):
        data = [ [rowKey]+ self.rows[rowKey].obtainActualVersion() for rowKey in self.rows]
        return data

    def obtainColumnInfo(self):
        data = {rowKey: self.rows[rowKey].getActualValue() for rowKey in self.rows}
        return data

class ColumnFamily:
    def __init__(self, name: str, columns:List[str]=[]):
        self.name = name
        self.columns = [Column(column) for column in columns]

    def insertColumn(self, column: str):
        self.columns.append(Column(column))
    
    def insertRow(self, rowKey, values: Dict[str, Any]):
        for column in self.columns:
            if column.name in values:
                column.insertRow(rowKey, values[column.name])

    def obtainColumnFamilyInfo(self):
        rows = {}
        for column in self.columns:
            for rowKey in column.obtainColumnInfo():
                if rowKey not in rows:
                    rows[rowKey] = {}
                rows[rowKey][f'{self.name}{':' if self.name != '' else ''}{column.name}'] = column.rows[rowKey].getActualValue()

        return rows
    
    def obtainColumnFamilyInfoWithMetadata(self):
        data = []
        for column in self.columns:
            metadataColumn = column.obtainColumnInfoWithMetadata()
            # insert the column name in the second position
            for row in metadataColumn:
                row.insert(1, f'{self.name}{':' if self.name != '' else ''}{column.name}')
                data.append(row)
            data+=metadataColumn

        return data
            
        

class Value:
    def __init__(self, value):
        self.creationDate = datetime.datetime.now().timestamp()
        self.value = value

    def obtainVersion(self):
        return [self.creationDate, self.value]

class Cell:
    def __init__(self, value):
        self.values = [Value(value)]

    def update(self, newValue):
        self.values.append(Value(newValue))

    def getActualValue(self):
        return self.values[-1].value
    
    def obtainActualVersion(self):
        return self.values[-1].obtainVersion()

class Table:
    def __init__(self, columns:Dict[str, List[str]]):
        self.columnFamilies = []
        for cf in columns:
            self.columnFamilies.append(ColumnFamily(cf.strip(), [c for c in columns[cf]]))
        self.rowKeyCounter = 0
        self.isEnable = True

    def generateRowKey(self):
        # Using UUID for unique row keys
        return str(uuid.uuid4())

    def insertOne(self, rowData: Dict[str, Dict[str, Any]]):
        rowKey = self.generateRowKey()
        for cf in self.columnFamilies:
            if cf.name in rowData:
                cf.insertRow(rowKey, rowData[cf.name])


    def obtainTableInfo(self):
        data = {}
        for cf in self.columnFamilies:
            for rowKey, rowValues in cf.obtainColumnFamilyInfo().items():
                if rowKey not in data:
                    data[rowKey] = {}
                for columnFamily, columnValue in rowValues.items():
                    data[rowKey][columnFamily] = columnValue

        # Make the data a pandas dataframe, where the rowKey is the index
        data = pd.DataFrame(data).T

        print(tabulate.tabulate(data, headers='keys', tablefmt='grid'))
        return data
    
    def obtainTableInfoWithMetadata(self):
        data = []
        for cf in self.columnFamilies:
            metadataCF = cf.obtainColumnFamilyInfoWithMetadata()
            for row in metadataCF:
                data.append(row)
            data+=metadataCF

        headers = ['Row Key', 'CF:Column', 'Timestamp', 'Value']
        data = pd.DataFrame(data, columns=headers)
        print(tabulate.tabulate(data, headers='keys', tablefmt='grid'))
        return data




