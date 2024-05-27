import datetime
import uuid
from typing import Dict, List, Any
import pandas as pd
import tabulate

class IndexedNode:
    def __init__(self, value):
        self.value = value
        self.left = None
        self.right = None

    def searchNode(self, rowKey):
        if self.value.rowKey == rowKey:
            return self.value
        if rowKey < self.value.rowKey:
            if self.left is None:
                return None
            return self.left.searchNode(rowKey)
        if self.right is None:
            return None
        return self.right.searchNode(rowKey)

class IndexTree:
    def __init__(self, cellList:List['Cell']):
        self.sortedCellList = sorted(cellList, key=lambda x: x.rowKey)
        self.root = IndexedNode(self.sortedCellList[len(self.sortedCellList)//2])
        self.root = self.createIndexNode(self.sortedCellList)

    def createIndexNode(self, cellList:List['Cell']):
        if len(cellList) == 0:
            return None
        mid = len(cellList)//2
        root = IndexedNode(cellList[mid])
        root.left = self.createIndexNode(cellList[:mid])
        root.right = self.createIndexNode(cellList[mid+1:])
        return root
    
    def search(self, rowKey):
        return self.searchNode(self.root, rowKey)
    
    def searchNode(self, node, rowKey):
        if node is None:
            return None
        if node.value.rowKey == rowKey:
            return node.value
        if rowKey < node.value.rowKey:
            return self.searchNode(node.left, rowKey)
        return self.searchNode(node.right, rowKey)
    
    def add(self, rowKey:'Cell'):
        self.root = self.addNode(self.root, rowKey)
    
    def addNode(self, rowKey:'Cell',  node=None):
        if node is None:
            return IndexedNode(rowKey)
        if rowKey < node.value.rowKey:
            node.left = self.addNode(node.left, rowKey)
        elif rowKey > node.value.rowKey:
            node.right = self.addNode(node.right, rowKey)
        return node

class Column:
    def __init__(self, name, rows: Dict[str, Any]={}, indexed=False):
        self.name = name
        self.rows: List['Cell'] = [Cell(rows[rowKey], rowKey) for rowKey in rows]
        self.indexed = indexed
        self.tree = None if not indexed else IndexTree(self.rows)

    def setIndexed(self, indexed=False):
        self.indexed = indexed
        if self.indexed:
            self.tree = IndexTree(self.rows)
        else:
            self.tree = None

    def searchRow(self, rowKey):
        if self.indexed:
            return self.tree.search(rowKey)
        for cell in self.rows:
            if cell.rowKey == rowKey:
                return cell
        return None

    def insertRow(self, rowKey, value):
        row = self.searchRow(rowKey)
        if row:
            row.update(value)
        else:
            newVal=Cell(value, rowKey)
            self.rows.append(newVal)
            if self.indexed:
                self.tree.add(newVal)
        

    def obtainColumnInfoWithMetadata(self):
        data = [ [rowKey.rowKey]+ rowKey.obtainActualVersion() for rowKey in self.rows]
        return data
    
    def obtainColumnInfoWithMetadataRowkey(self, rowKey):
        rowKey = self.searchRow(rowKey)
        if rowKey is None:
            return []
        data = [ [rowKey.rowKey]+ rowKey.obtainActualVersion()]
        return data

    def obtainColumnInfo(self):
        data = {rowKey.rowKey: rowKey.getActualValue() for rowKey in self.rows}
        return data
    
    def insertOrUpdateRow(self, rowKey, value):
        row = self.searchRow(rowKey)
        if row:
            row.update(value)
        else:
            self.insertRow(rowKey, value)
        return True
    
    def maxNumberOfVersions(self):
        if len(self.rows) == 0:
            return 0
        return max([rowKey.tiemsStamp() for rowKey in self.rows])
    
    def minNumberOfVersions(self):
        if len(self.rows) == 0:
            return 0
        return min([rowKey.tiemsStamp() for rowKey in self.rows])


class ColumnFamily:
    def __init__(self, name: str, columns:List[str]=[], indexed=False):
        self.name = name
        self.columns = {column:Column(column) for column in columns}
        self.isIndexed = indexed

        if indexed:
            for column in self.columns:
                self.columns[column].setIndexed()

    def insertColumn(self, column: str):
        self.columns[column] = Column(column)
    
    def insertRow(self, rowKey, values: Dict[str, Any]):
        for column in values:
            if column in self.columns:
                self.columns[column].insertRow(rowKey, values[column])
            else:
                self.insertColumn(column)
                self.columns[column].insertRow(rowKey, values[column])

    def searchRow(self, rowKey, column=None):
        if column is None:
            rowData = {}
            for column in self.columns:
                rowData[column] = self.columns[column].searchRow(rowKey)
            return rowData
        else:
            rowData = {}
            if column in self.columns:
                rowData[column] = self.columns[column].searchRow(rowKey)
        return None

    def setIndexed(self, indexed=False):
        self.isIndexed = indexed
        for column in self.columns:
            self.columns[column].setIndexed(self.isIndexed)

    def obtainColumnFamilyInfo(self):
        rows = {}
        for column in self.columns.values():
            for rowKey in column.obtainColumnInfo():
                if rowKey not in rows:
                    rows[rowKey] = {}
                rows[rowKey][f'{self.name}{':' if self.name != '' else ''}{column.name}'] = column.searchRow(rowKey).getActualValue()

        return rows
    
    def obtainColumnFamilyInfoWithMetadata(self):
        data = []
        for column in self.columns.values():
            metadataColumn = column.obtainColumnInfoWithMetadata()
            # insert the column name in the second position
            for row in metadataColumn:
                row.insert(1, f'{self.name}{':' if self.name != '' else ''}{column.name}')
                data.append(row)
            data+=metadataColumn

        return data
    
    def obtainColumnFamilyInfoRowkeyWithMetadata(self, rowkey, column=None):
        data = []
        if column is None:
            for column in self.columns.values():
                metadataColumn = column.obtainColumnInfoWithMetadataRowkey(rowkey)
                for row in metadataColumn:
                    row.insert(1, f'{self.name}{':' if self.name != '' else ''}{column.name}')
                    data.append(row)
                data+=metadataColumn
        else:
            if column in self.columns:
                metadataColumn = self.columns[column].obtainColumnInfoWithMetadataRowkey(rowkey)
                for row in metadataColumn:
                    row.insert(1, f'{self.name}{':' if self.name != '' else ''}{column}')
                    data.append(row)
                data+=metadataColumn
        return data
    
    def insertOrUpdateRow(self, rowKey, column, value:str):
        saveValue = None
        if value.isdigit():
            saveValue = int(value)
        elif value.replace('.','',1).isdigit():
            saveValue = float(value)
        elif value.lower() == 'true' or value.lower() == 'false':
            saveValue = bool(value)
        else:
            saveValue = value

        if column in self.columns:
            self.columns[column].insertOrUpdateRow(rowKey, saveValue)
        else:
            self.insertColumn(column)
            self.columns[column].insertOrUpdateRow(rowKey, saveValue)

    def maxNumberOfVersions(self):
        if len(self.columns) == 0:
            return 0
        
        return max([column.maxNumberOfVersions() for column in self.columns.values()])
    
    def minNumberOfVersions(self):
        if len(self.columns) == 0:
            return 0
        return min([column.minNumberOfVersions() for column in self.columns.values()])

class Value:
    def __init__(self, value):
        self.creationDate = datetime.datetime.now().timestamp()
        self.value = value if not isinstance(value, list) else str(value)

    def obtainVersion(self):
        return [self.creationDate, self.value]

class Cell:
    def __init__(self, value, rowKey):
        self.values = [Value(value)]
        self.rowKey = rowKey

    def update(self, newValue):
        self.values.append(Value(newValue))

    def getActualValue(self):
        if isinstance(self.values[-1].value, list):
            return str(self.values[-1].value)
        return self.values[-1].value
    
    def obtainActualVersion(self):
        return self.values[-1].obtainVersion()

    def isEmpty(self):
        return len(self.values) == 0
    
    def tiemsStamp(self):
        return len(self.values)
    
    def __eq__(self, value: object) -> bool:
        if isinstance(value, str):
            return self.rowKey == value
        if isinstance(value, Cell):
            return value.rowKey == self.rowKey
        
        return False
        

class Table:
    def __init__(self, columns:Dict[str, List[str]], indexed=False):
        self.indexed = indexed
        self.columnFamilies:List['ColumnFamily'] = [ColumnFamily('', indexed=indexed)]

        for cf in columns:
            if cf != '':
                self.columnFamilies.append(ColumnFamily(cf.strip(), [c for c in columns[cf]], indexed))
            else:
                self.columnFamilies[0] = ColumnFamily(cf.strip(), [c for c in columns[cf]], indexed)
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
        return data
    
    def setIndexed(self):
        self.indexed = not self.indexed
        print(self.indexed)
        for cf in self.columnFamilies:
            cf.setIndexed(self.indexed)

    
    def obtainTableInfoWithMetadata(self):
        data = []
        for cf in self.columnFamilies:
            metadataCF = cf.obtainColumnFamilyInfoWithMetadata()
            for row in metadataCF:
                data.append(row)
            data+=metadataCF

        headers = ['Row Key', 'CF:Column', 'Timestamp', 'Value']
        data = pd.DataFrame(data, columns=headers)
        # Delete duplicate rows
        data = data.drop_duplicates()
        return data
    
    def obtainTableInfoRowkeyWithMetadata(self, rowkey, columnFamily, column=None):
        data = []
        if columnFamily is None:
            for cf in self.columnFamilies:
                metadataCF = cf.obtainColumnFamilyInfoRowkeyWithMetadata(rowkey)
                for row in metadataCF:
                    data.append(row)
                data+=metadataCF
        else:
            for cf in self.columnFamilies:
                if cf.name == columnFamily:
                    metadataCF = cf.obtainColumnFamilyInfoRowkeyWithMetadata(rowkey, column)
                    for row in metadataCF:
                        data.append(row)
                    data+=metadataCF
        headers = ['Row Key', 'CF:Column', 'Timestamp', 'Value']
        data = pd.DataFrame(data, columns=headers)
        # Delete duplicate rows
        data = data.drop_duplicates()
        return data
    
    def insertOrUpdateRow(self, rowKey, columnFamily, column, value):
        for cf in self.columnFamilies:
            print(cf.name)
            if cf.name.strip() == columnFamily.strip():
                cf.insertOrUpdateRow(rowKey, column, value)
                return True 
        return False
    

    def addColumnFamily(self, columnFamilyName, columns:List[str]=[]):
        self.columnFamilies.append(ColumnFamily(columnFamilyName, columns))

    def describeTable(self):
        data = {}
        rowKeys = set()
        for cf in self.obtainTableInfo().index:
            rowKeys.add(cf)
        data['Row keys'] = len(rowKeys)
        data['Column Families'] = str([cf.name  for cf in self.columnFamilies if cf.name!=''])
        data['isEnable'] = self.isEnable
        data['Max number of versions'] = max([cf.maxNumberOfVersions() for cf in self.columnFamilies])
        data['Min number of versions'] = min([cf.minNumberOfVersions() for cf in self.columnFamilies])
        data['Is indexed'] = self.indexed 
        return data
    
    def insertMany(self, rows:Dict[str, Dict[str, Dict[str, Any]]]):
        for row in rows:
            for cf in self.columnFamilies:
                print(cf.name in rows[row], 'cf name in rows', cf.name, rows[row])
                if cf.name in rows[row]:
                    cf.insertRow(row, rows[row][cf.name])

    def searchDataRow(self, rowKey, columnFamily=None, column=None):
        if columnFamily is None:
            rowData = {}
            for cf in self.columnFamilies:
                rowData[cf.name] = cf.searchRow(rowKey)
            return rowData
        
        else:
            rowData = {}
            for cf in self.columnFamilies:
                if cf.name == columnFamily:
                    rowData[cf.name] = cf.searchRow(rowKey, column)
            return rowData
