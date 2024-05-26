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

    def outputFormatter(self, time, rows):
        """
        Formats the output message based on the execution time and number of rows.

        Args:
            time (float): The execution time in seconds.
            rows (int): The number of rows.

        Returns:
            str: A formatted message indicating the number of rows and the execution time.
        """
        # Determine the time unit and format accordingly.
        time_str = f"{time * 1000:.4f} ms" if time < 1 else f"{time:.4f} s"
        return f"{rows} row(s) in {time_str}"

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
        
    def count(self, table: str):
        """
        Counts the number of unique rows in the specified table.

        Args:
            table (str): The name of the table to count rows in.

        Returns:
            str: A message indicating the number of unique rows and the time taken to count them,
                or an error message if the table does not exist.
        """
        # Initialize a set to store unique row identifiers.
        unique_rows = set()
        
        # Record the start time for performance measurement.
        initTime = time.perf_counter()

        # Check if the specified table exists in the database.
        if table in self.tables:
            # Retrieve the data for the specified table.
            data = self.tables[table]
            
            # Iterate through each column family in the table.
            for family in data.columnFamilies:
                # Iterate through each column in the column family.
                for column in family.columns.keys():
                    # Get the rows associated with the current column.
                    rows = family.columns[column].rows.keys()
                    # Add the rows to the set of unique rows.
                    unique_rows.update(rows)
            
            # Calculate the total time taken for the operation.
            time_taken = time.perf_counter() - initTime
            
            # Return the count of unique rows and the time taken in milliseconds.
            return self.outputFormatter(time_taken, len(unique_rows))
        
        else:
            # Return an error message if the table does not exist.
            return f"Error: The table '{table}' could not be found."

    def drop(self, table: str):
        """
        Drops the specified table from the database.

        Args:
            table (str): The name of the table to be dropped.

        Returns:
            str: A message indicating the result of the drop operation,
                or an error message if the table does not exist.
        """
        # Record the start time for performance measurement.
        initTime = time.perf_counter()

        # Check if the specified table exists in the database.
        if table in self.tables:
            # Check if the table is currently enabled.
            if self.tables[table].isEnable:
                # Return a message indicating the table must be disabled before dropping.
                return f"Action required: The table '{table}' must be disabled before it can be dropped."
            else:   
                # Construct the file path for the table's file.
                table_file_path = os.path.join(self.tableDirectory, f"{table}.hfile")
                    
                # Check if the table's file exists in the file system.
                if os.path.exists(table_file_path):
                    # Delete the table's file.
                    os.remove(table_file_path)
                    # Remove the table from the database's tables dictionary.
                    del self.tables[table]
                    # Calculate the total time taken for the operation.
                    time_taken = time.perf_counter() - initTime

                    return self.outputFormatter(time_taken, 0)
        else:
            # Return an error message if the table does not exist.
            return f"Error: The table '{table}' could not be found."

        

        


        



