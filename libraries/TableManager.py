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

    def scan(self, table: str, nversions:int = 1, version:float = None):
        """
        Scan the specified table and retrieve its data along with metadata.

        Parameters:
            table (str): The name of the table to scan.

        Returns:
            pd.DataFrame: A DataFrame containing the table's data and metadata, 
                        or an error message if the table does not exist.
        """
        # Check if the specified table exists in the database.
        if table in self.tables:
            # Retrieve and return the table's data along with metadata.
            return self.tables[table].obtainTableInfoWithMetadata(versions=nversions, version=version)

        else:
            # Return an error message if the table does not exist.
            return pd.DataFrame({"Error": ["Table not found"]})

    def list_(self):
        """
        List all tables in the database.

        Returns:
            pd.DataFrame: A DataFrame containing the names of all tables.
        """
        # Create and return a DataFrame with a single column 'Tables' containing the names of all tables.
        return pd.DataFrame({"Tables": list(self.tables.keys())})

    def disable(self, table: str):
        """
        Disable the specified table if it exists in the database and measure the time taken for the operation.

        Parameters:
            table (str): The name of the table to disable.

        Returns:
            str: A formatted string indicating the time taken to disable the table, or an error message if the table does not exist.
        """
        # Start a timer to measure the time taken for the operation.
        init_time = time.perf_counter()

        # Check if the specified table exists in the database.
        if table in self.tables:
            # Verify if table is disable
            if not self.tables[table].isEnable:
                return f"Error: The table '{table}' is already disabled."

            # Disable the table by setting its isEnable attribute to False.
            self.tables[table].isEnable = False

            # Save table
            with open(f"{self.tableDirectory}/{table}.hfile", 'wb') as file:
                pickle.dump(self.tables[table], file)

            # Calculate the total time taken for the operation.
            time_taken = time.perf_counter() - init_time

            # Format and return the time taken.
            return self.outputFormatter(time_taken, 0)
        
        else:
            # Return an error message if the table does not exist.
            return f"Error: The table '{table}' could not be found."

    def enable(self, table: str):
        """
        Enable the specified table if it exists in the database and measure the time taken for the operation.

        Parameters:
            table (str): The name of the table to enable.

        Returns:
            str: A formatted string indicating the time taken to enable the table, or an error message if the table does not exist.
        """
        # Start a timer to measure the time taken for the operation.
        init_time = time.perf_counter()
        
        # Check if the specified table exists in the database.
        if table in self.tables:
            # Verify if table is enable
            if self.tables[table].isEnable:
                return f"Error: The table '{table}' is already enabled."
            # Enable the table by setting its isEnable attribute to True.
            self.tables[table].isEnable = True

            #Save table
            with open(f"{self.tableDirectory}/{table}.hfile", 'wb') as file:
                pickle.dump(self.tables[table], file)
            
            # Calculate the total time taken for the operation.
            time_taken = time.perf_counter() - init_time
            
            # Format and return the time taken.
            return self.outputFormatter(time_taken, 0)
        else:
            # Return an error message if the table does not exist.
            return f"Error: The table '{table}' could not be found."
        
    def isEnabled(self, table: str):
        """
        Check if the specified table exists in the database and measure the time taken for the operation.

        Parameters:
            table (str): The name of the table to check.

        Returns:
            str: A formatted string indicating the time taken for the operation if the table exists,
                or an error message if the table does not exist.
        """
        # Start a timer to measure the time taken for the operation.
        init_time = time.perf_counter()
        str_list = [str(self.tables[table].isEnable)]
        # Check if the specified table exists in the database.
        if table in self.tables:
            # Calculate the total time taken for the operation.
            time_taken = time.perf_counter() - init_time
            
            # Format and return the time taken.
            str_list.append(self.outputFormatter(time_taken, 0))
            return "\n".join(str_list)
        else:
            # Return an error message if the table does not exist.
            return f"Error: The table '{table}' could not be found."
        
    def create(self, name: str, column_families: List[str]):
        """
        Create a new table with specified column families and save it to a file.

        Parameters:
            name (str): The name of the new table to be created.
            column_families (List[str]): A list of column families to be included in the new table.

        Returns:
            str: A formatted string indicating the time taken to create the table or an error message.
        """
        # Start a timer to measure the time taken to create the table.
        init_time = time.perf_counter()

        try:
            # Create a new Table object with specified column families.
            newTable = Table(columns={cf: [] for cf in column_families}, indexed=False)
            
            # Add the new table to the tables dictionary.
            self.tables[name] = newTable

            # Save the new table to a file using pickle.
            with open(f"{self.tableDirectory}/{name}.hfile", 'wb') as file:
                pickle.dump(newTable, file)

            # Calculate the total time taken for the operation.
            time_taken = time.perf_counter() - init_time

            # Format and return the time taken.
            return self.outputFormatter(time_taken, 0)

        except Exception as e:
            # Return an error message if an exception occurs.
            return f"Error: {e}"

    def get(self, table: str, row: str, column_family=None, column_name=None, nversions:int = 1, version:float = None):
        """
        Retrieve data from a specified table in the database.

        Parameters:
            table (str): The name of the table from which to retrieve data.
            row (str): The row key to search for in the table.
            column_family (str, optional): The column family to filter the search (default is None).
            column_name (str, optional): The column name to filter the search (default is None).
            versions (int, optional): The number of versions to retrieve (default is 1).
            version (float, optional): The version to retrieve (default is None).

        Returns:
            pd.DataFrame: A DataFrame containing the retrieved data or error messages.
        """
        # Check if the row key is empty and return an error DataFrame if it is.
        if row.strip() == '':
            return pd.DataFrame({"Error": ["RowKey is empty"]})

        # Check if the specified table exists in the database.
        if table in self.tables:
            # Retrieve data from the table based on the provided row key, column family, column name and number of versions.
            data = self.tables[table].obtainTableInfoRowkeyWithMetadata(row, column_family, column_name, nversions, version)
            
            # If no data is found for the given row key, return an error DataFrame.
            if len(data) == 0:
                return pd.DataFrame({"Error": ["Row not found"]})

            # If only one row of data is found, return it as a single-row DataFrame.
            if len(data) == 1:
                return pd.DataFrame(data, index=[0])

            # Return the retrieved data as a DataFrame.
            return pd.DataFrame(data)
        else:
            # Return an error DataFrame if the specified table does not exist.
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
                    rows = [row.rowKey for row in family.columns[column].rows]
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

            # Verify if table is enable
            if not data.isEnable:
                return f"Error: The table '{table}' is disabled."
            
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

            except (KeyError, AttributeError):
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
            # Verify if table is enable
            if not data.isEnable:
                return f"Error: The table '{table}' is disabled."

            found = False  # Initialize a flag to indicate if the value is found
            # Iterate through each column family in the data
            for family in data.columnFamilies:
                # Iterate through each column in the column family
                for column in family.columns.keys():
                    result = family.columns[column].searchRow(row)
                    if result:
                        family.columns[column].rows.remove(row)
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

            # Verify if table is enable
            if not data.isEnable:
                return f"Error: The table '{table}' is disabled."
            
            # Extract and join all column family names into a single string.
            family_names = [family.name for family in data.columnFamilies]

            # Messages indicating the steps of the truncation process.
            disable_str = '-Disabling table...'
            truncate_str = '-Truncating table...'
            rebuild_str = '-Rebuilding table...'

            # Disable, drop, and recreate the table.
            self.disable(table=table)
            self.drop(table=table)
            self.create(name=table, column_families=family_names)

            # Join all the messages together.
            messages = [init_str, disable_str, truncate_str, rebuild_str]

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
        init_time = time.perf_counter()
        
        # Check if the specified table exists in the database.
        if table in self.tables:
            # Verify if table is enable
            if not self.tables[table].isEnable:
                return f"Error: The table '{table}' is disabled."
            # Call the insertOrUpdateRow method on the specified table.
            if self.tables[table].insertOrUpdateRow(rowKey, column_family, column, value):
                # Save the updated data back to the file.
                with open(f"{self.tableDirectory}/{table}.hfile", 'wb') as outf:
                    pickle.dump(self.tables[table], outf)
                # Calculate the total time taken for the operation.
                time_taken = time.perf_counter() - init_time
                # Return the time taken in milliseconds.
                return self.outputFormatter(time_taken, 0)
            else:
                # Return an error message if the specified column family is not found.
                return f"Error: Column family '{column_family}' could not be found."
        else:
            # Return an error message if the table does not exist.
            return f"Error: The table '{table}' could not be found."
            
    def alter(self, table:str, args:Dict[str, Union[str, List[str]]]):
        # Record the start time for performance measurement.
        init_time = time.perf_counter()
        # Check if the specified table exists in the database.
        if table in self.tables:
            # Verify if table is enable
            if not self.tables[table].isEnable:
                return f"Error: The table '{table}' is disabled."

            if 'delete' in args:
                for column in self.tables[table].columnFamilies:
                    if column.name == args['delete']:
                        # Verify if is the last column family
                        if len(self.tables[table].columnFamilies) == 1:
                            return f"Error: Table '{table}' must have at least one column family."
                        self.tables[table].columnFamilies.remove(column)
                        break
            elif 'cf' in args:
                if 'method' in args:
                    method = args['method']
                    if method == 'delete':
                        for column in self.tables[table].columnFamilies:
                            if column.name == args['cf']:
                                # Verify if is the last column family
                                if len(self.tables[table].columnFamilies) == 1:
                                    return f"Error: Table '{table}' must have at least one column family."
                                self.tables[table].columnFamilies.remove(column)
                                break
                    elif method == 'rename':
                        toModify = None
                        exist = False
                        for column in self.tables[table].columnFamilies:
                            if column.name == args['cf']:
                                toModify = column
                            if column.name == args['new_cf']:
                                exist = True

                        if toModify:
                            if not exist:
                                toModify.name = args['new_cf']
                            else:
                                return f"Error: Column family '{args['new_cf']}' already exists."
                        else:
                            return f"Error: Column family '{args['cf']}' could not be found."
                    elif method == 'add':
                        exist = False
                        for column in self.tables[table].columnFamilies:
                            if column.name == args['cf']:
                                exist = True
                                break
                        if not exist:
                            self.tables[table].addColumnFamily(args['cf'])
                        else:
                            return f"Error: Column family '{args['cf']}' already exists."
                else:
                    self.tables[table].addColumnFamily(args['cf'])

            elif 'index' in args:
                self.tables[table].setIndexed()

            # Save the updated data back to the file
            with open(f"{self.tableDirectory}/{table}.hfile", 'wb') as outf:
                pickle.dump(self.tables[table], outf)

            # Calculate the total time taken for the operation.
            time_taken = time.perf_counter() - init_time

            # Return the time taken in milliseconds.
            return self.outputFormatter(time_taken, 0)
            
        else:
            # Return an error message if the table does not exist.
            return f"Error: The table '{table}' could not be found."
            
    def describe(self, table:str):
        if table in self.tables:
            data = self.tables[table].describeTable()
            data['Name'] = table
            return pd.DataFrame(data, index=[0])
        else:
            return pd.DataFrame({"Error": ["Table not found"]})
        
    def insertMany(self, file):
        """
        Insert multiple rows into multiple tables from a given file and measure the time taken for the operation.

        Parameters:
            file: A file where keys are table names and values are rows to insert.

        Returns:
            str: A formatted string indicating the time taken to perform the insertion.
        """
        # Record the start time for performance measurement.
        init_time = time.perf_counter()
        
        # Iterate over each table and its corresponding rows in the input file.
        for table, rows in file.items():
            if table in self.tables:
                # Insert multiple rows into the specified table.
                self.tables[table].insertMany(rows)
                
                # Save the updated table to a file using pickle.
                with open(f"{self.tableDirectory}/{table}.hfile", 'wb') as outf:
                    pickle.dump(self.tables[table], outf)

        # Calculate the total time taken for the operation.
        time_taken = time.perf_counter() - init_time

        # Return the time taken in a formatted string.
        return self.outputFormatter(time_taken, 0)
          
        
