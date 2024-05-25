from .Classes import Table
import pickle
import os
from typing import Dict, List
import pandas as pd
import time

class TableManager:
    def __init__(self, tableDirectory:str) -> None:
        self.tableDirectory = tableDirectory
        # Verify if the directory exists and files are stored in it and save in a list
        self.tables:Dict[str, Table] = {}
        if os.path.exists(tableDirectory):
            for file in os.listdir(tableDirectory):
                if file.endswith('.hfile'):
                    self.tables[file.split('.')[0]] = pickle.load(open(f"{tableDirectory}/{file}", 'rb'))
                    self.tables[file.split('.')[0]].obtainTableInfoWithMetadata()
        else:
            os.mkdir(tableDirectory)
            raise Exception(f"Table directory {tableDirectory} does not exist. Created a new directory.")
        
        print(f"Tables: {self.tables}")

    def scan(self, table:str):
        if table in self.tables:
            return self.tables[table].obtainTableInfoWithMetadata()
        else:
            return pd.DataFrame({"Error": ["Table not found"]})
    
    def list_(self):
        return pd.DataFrame({"Tables": list(self.tables.keys())})
    
    def disable(self, table:str):
        initTime = time.perf_counter()
        if table in self.tables:
            self.tables[table].isEnable = False
            endTime = time.perf_counter()
            total = endTime - initTime
            # Config total time to show the time in seconds or milliseconds and only use 4 decimal places
            if total < 1:
                return f"Table '{table}' disabled. Time: {total * 1000:.4f} ms"
            else:
                return f"Table '{table}' disabled. Time: {total:.4f} s"
        else:
            return f"Table '{table}' not found."
        
    def enable(self, table:str):
        initTime = time.perf_counter()
        if table in self.tables:
            self.tables[table].isEnable = True
            endTime = time.perf_counter()
            total = endTime - initTime
            # Config total time to show the time in seconds or milliseconds and only use 4 decimal places
            if total < 1:
                return f"Table '{table}' enabled. Time: {total * 1000:.4f} ms"
            else:
                return f"Table '{table}' enabled. Time: {total:.4f} s"
        else:
            return f"Table '{table}' not found."
        
    def isEnable(self, table:str):
        initTime = time.perf_counter()
        if table in self.tables:
            endTime = time.perf_counter()
            total = endTime - initTime
            # Config total time to show the time in seconds or milliseconds and only use 4 decimal places
            if total < 1:
                return f"Table '{table}' is {'enabled' if self.tables[table].isEnable else 'disabled'}. Time: {total * 1000:.4f} ms"
            else:
                return f"Table '{table}' is {'enabled' if self.tables[table].isEnable else 'disabled'}. Time: {total:.4f} s"
        else:
            return f"Table '{table}' not found."
        
    def createTable(self, name, columsFamilys:List[str]):
        initTime = time.perf_counter()
        try:
            newTable = Table({cf: [] for cf in columsFamilys})
            self.tables[name] = newTable

            with open(f"{self.tableDirectory}/{name}.hfile", 'wb') as file:
                pickle.dump(newTable, file)

            endTime = time.perf_counter()
            total = endTime - initTime
            # Config total time to show the time in seconds or milliseconds and only use 4 decimal places
            if total < 1:
                return f"Table '{name}' created. Time: {total * 1000:.4f} ms"
            else:
                return f"Table '{name}' created. Time: {total:.4f} s"
        except Exception as e:
            return f"Error: {e}"
        
    def get(self, table:str, rowKey:str):
        if rowKey.strip() == '':
            return pd.DataFrame({"Error": ["RowKey is empty"]})
        
        if table in self.tables:
            data = self.tables[table].obtainTableInfoWithMetadata()
            print(data.columns, rowKey, type(rowKey))
            data = data[data['Row Key']== rowKey ]
            return data
        else:
            return pd.DataFrame({"Error": ["Table not found"]})


        

        


        



