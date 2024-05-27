# Standard library imports
import os   # Provides a way of using operating system dependent functionality like reading or writing to the file system
import time # Provides various time-related functions
import re   # Provides support for regular expressions

import pickle  # Provides functions for serializing and deserializing Python object structures

# Third-party library imports
import pandas as pd  # Provides data structures and data analysis tools (if needed, this should be installed via pip)

# Local application/library specific imports
from .Classes import Table  # Imports the Table class from the local Classes module

# Typing imports for type hinting
from typing import Dict, List, Union  # Provides support for type hints, Dict and List in this case


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
        
    def createTable(self, name, columnFamilies:List[str]):
        initTime = time.perf_counter()
        try:
            newTable = Table(columns={cf: [] for cf in columnFamilies}, indexed=False)
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
        
    def get(self, table:str, rowKey:str, column_family=None, column=None):

        if rowKey.strip() == '':
            return pd.DataFrame({"Error": ["RowKey is empty"]})
        
        
        if table in self.tables:
            data = self.tables[table].obtainTableInfoRowkeyWithMetadata(rowKey, column_family, column)
            if len(data) == 0:
                return pd.DataFrame({"Error": ["Row not found"]})
            
            if len(data) == 1:
                return pd.DataFrame(data, index=[0])
            return pd.DataFrame(data)
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
        
    def dropAll(self, regex: str):
        """
        Drops all tables matching the given regular expression pattern.

        Args:
            regex (str): The regular expression pattern to match table names.

        Returns:
            str: A formatted message indicating the total time taken for the operation.
        """
        # Record the start time for performance measurement.
        initTime = time.perf_counter()
        
        # Compile the regular expression pattern.
        pattern = re.compile(regex)
        
        # Find all table names that match the regular expression pattern.
        regex_match = [key for key in self.tables.keys() if pattern.match(key)]

        results = []

        # Iterate through each matched table name and drop the table.
        for table in regex_match:
            result = self.drop(table)
            if "Action" in result or "Error" in result:
                results.append(result)
        
        # Calculate the total time taken for the operation.
        time_taken = time.perf_counter() - initTime

        if results:
            # Format and return the message indicating the total time taken and the results.
            return f"{"\n".join(results)}"
        
        # Format and return the message indicating the total time taken.
        return self.outputFormatter(time_taken, 0)
    
    def delete(self, table: str, row: str, column_name: str, timestamp: float):
        """
        Deletes an entry from the table based on the provided parameters.

        Args:
            table (str): The name of the table from which to delete the entry.
            row (str): The row identifier of the entry to delete.
            column_name (str): The column name of the entry to delete.
            timestamp (int): The timestamp associated with the entry to delete.

        Returns:
            str: A formatted message indicating the total time taken for the operation.
        """
        # Record the start time for performance measurement.
        initTime = time.perf_counter()

        # Split the column_name parameter to retrive the column family and column name.
        family_name, column_name = column_name.split(':')

        # Check if the specified table exists in the database.
        if table in self.tables:
            # Retrieve the data for the specified table.
            data = self.tables[table]
            
            found = False  # Initialize a flag to indicate if the value is found

            try:
                # Iterate through each column family in the data
                for family in data.columnFamilies:
                    # Check if the column family matches the specified family name
                    if family.name == family_name:

                        # Iterate through each column in the column family
                        for column in family.columns.keys():
                            # Check if the column matches the specified column name
                            if column == column_name:

                                # Get the rows associated with the current column
                                cell_values = family.columns[column].searchRow(row).values

                                # Iterate through each value in the cell
                                for i, value in enumerate(cell_values):
                                    # Check if the value's creation date matches the specified timestamp
                                    if value.creationDate == timestamp:

                                        # Remove the value from the list of values
                                        family.columns[column].searchRow(row).values.pop(i)
                                        found = True  # Set the flag to True as the value is found

                                # If the row is empty after removing the value, delete the row
                                if family.columns[column].searchRow(row).isEmpty():
                                    family.columns[column].rows.remove(row)

            except KeyError:
                # Handle the case where the specified row key is not found
                return f"Error: The specified row key was not found in the table."

            # Check if the value was not found during the iteration
            if not found:
                return f"Error: The value could not be found."
            else:
                # Save the updated data back to the file if the value was found and removed
                with open(f"{self.tableDirectory}/{table}.hfile", 'wb') as outf:
                    pickle.dump(data, outf)

            
            # Calculate the total time taken for the operation.
            time_taken = time.perf_counter() - initTime
            
            # Return the time taken in milliseconds.
            return self.outputFormatter(time_taken, 0)

        else:
            # Return an error message if the table does not exist.
            return f"Error: The table '{table}' could not be found."
        
    def deleteAll(self, table: str, row: str):
        # Record the start time for performance measurement.
        initTime = time.perf_counter()

        # Check if the specified table exists in the database.
        if table in self.tables:
            # Retrieve the data for the specified table.
            data = self.tables[table]
            found = False  # Initialize a flag to indicate if the value is found
            # Iterate through each column family in the data
            for family in data.columnFamilies:
                # Iterate through each column in the column family
                for column in family.columns.keys():
                    result = family.columns[column].searchRow(row)
                    if result:
                        family.columns[column].rows.remove(result)
                        found = True

            # Check if the value was not found during the iteration
            if not found:
                return f"Error: The value could not be found."
            else:
                # Save the updated data back to the file if the value was found and removed
                with open(f"{self.tableDirectory}/{table}.hfile", 'wb') as outf:
                    pickle.dump(data, outf)

            # Calculate the total time taken for the operation.
            time_taken = time.perf_counter() - initTime
            
            # Return the time taken in milliseconds.
            return self.outputFormatter(time_taken, 0)

        else:
            # Return an error message if the table does not exist.
            return f"Error: The table '{table}' could not be found."
        
    def truncate(self, table: str):
        """
        Truncates the specified table by disabling, dropping, and recreating it.

        Args:
            table (str): The name of the table to truncate.

        Returns:
            str: A message indicating the result of the truncation operation along with the time taken.
        """
        # Record the start time for performance measurement.
        initTime = time.perf_counter()

        # Initial message indicating the start of the truncation process.
        init_str = 'Truncating \'one\' table (it may take a while):'

        # Check if the specified table exists in the database.
        if table in self.tables:
            # Retrieve the data for the specified table.
            data = self.tables[table]
            
            # Extract and join all column family names into a single string.
            family_names_str = ', '.join([name for family in data.columnFamilies for name in family.name])

            # Messages indicating the steps of the truncation process.
            disable_str = '-Disabling table...'
            truncate_str = '-Truncating table...'

            # Disable, drop, and recreate the table.
            self.disable(table=table)
            self.drop(table=table)
            self.createTable(name=table, columnFamilies=family_names_str)

            # Join all the messages together.
            messages = [init_str, disable_str, truncate_str]

            # Calculate the total time taken for the operation.
            time_taken = time.perf_counter() - initTime

            # Return the joined messages along with the time taken.
            return '\n'.join(messages) + '\n' + self.outputFormatter(time_taken, 0)

        else:
            # Return an error message if the table does not exist.
            return f"Error: The table '{table}' could not be found."

    def put(self, table:str, rowKey:str, column_family:str, column:str, value:str ):
        """
        Inserts or updates a value in the specified table.

        Args:
            table (str): The name of the table.
            rowKey (str): The row key where the value will be inserted or updated.
            column_family (str): The column family where the value will be inserted or updated.
            column (str): The column where the value will be inserted or updated.
            value (str): The value to be inserted or updated.

        Returns:
            str: A message indicating the result of the operation along with the time taken.
        """
        # Record the start time for performance measurement.
        initTime = time.perf_counter()
        
        # Check if the specified table exists in the database.
        if table in self.tables:
            # Call the insertOrUpdateRow method on the specified table.
            if self.tables[table].insertOrUpdateRow(rowKey, column_family, column, value):

                # Save the updated data back to the file.
                with open(f"{self.tableDirectory}/{table}.hfile", 'wb') as outf:
                    pickle.dump(self.tables[table], outf)
                # Calculate the total time taken for the operation.
                time_taken = time.perf_counter() - initTime
                # Return the time taken in milliseconds.
                return self.outputFormatter(time_taken, 0)
            else:
                # Return an error message if the specified column family is not found.
                return f"Error: Column family '{column_family}' could not be found."
        else:
            total = time.perf_counter() - initTime
            if total < 1:
                return f"Error: Table '{table}' not found. Time: {total * 1000:.4f} ms"
            else:
                return f"Error: Table '{table}' not found. Time: {total:.4f} s"
            
    def alter(self, table:str, args:Dict[str, Union[str, List[str]]]):
        initialTime = time.perf_counter()
        if table in self.tables:
            if 'delete' in args:
                for column in self.tables[table].columnFamilies:
                    if column.name == args['delete']:
                        self.tables[table].columnFamilies.remove(column)
                        break
            elif 'name' in args:
                if 'method' in args:
                    method = args['method']
                    if method == 'delete':
                        for column in self.tables[table].columnFamilies:
                            if column.name == args['name']:
                                self.tables[table].columnFamilies.remove(column)
                                break
                else:
                    self.tables[table].addColumnFamily(args['name'])

            elif 'index' in args:
                self.tables[table].setIndexed()


            # Save the updated data back to the file
            with open(f"{self.tableDirectory}/{table}.hfile", 'wb') as outf:
                pickle.dump(self.tables[table], outf)

            finalTime = time.perf_counter()
            total = finalTime - initialTime
            if total < 1:
                return f"Table '{table}' altered. Time: {total * 1000:.4f} ms"
            else:
                return f"Table '{table}' altered. Time: {total:.4f} s"
            

        else:
            total = time.perf_counter() - initialTime
            if total < 1:
                return f"Error: Table '{table}' not found. Time: {total * 1000:.4f} ms"
            else:
                return f"Error: Table '{table}' not found. Time: {total:.4f} s"
            

    def describe(self, table:str):
        if table in self.tables:
            data = self.tables[table].describeTable()
            data['Name'] = table
            return pd.DataFrame(data, index=[0])
        else:
            return pd.DataFrame({"Error": ["Table not found"]})
        
    
    def insertMany(self, file):
        for table, rows in file.items():
            if table in self.tables:
                self.tables[table].insertMany(rows)
                with open(f"{self.tableDirectory}/{table}.hfile", 'wb') as outf:
                    pickle.dump(self.tables[table], outf)


        return "Data inserted successfully"

          
        
