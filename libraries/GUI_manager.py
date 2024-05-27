import pickle
import customtkinter
from .components import InputCommand, Table
import pandas as pd
from .Classes import Table as TableClass, parse_command
from .TableManager import TableManager
from typing import List, Dict, Any
import time
import os
import json

class GUI_manager:
    def __init__(self, tableDirectory:str):
        self.app = customtkinter.CTk()
        self.app.title('Hbase GUI Manager')
        self.app.geometry("960x540")
        self.app.resizable(False, False)
        self.app.minsize(960,540)
        self.app.maxsize(960,540)

        # Make input command
        self.inputCommand = InputCommand(self.app, self.obtainOperation)
        self.tableManager = TableManager(tableDirectory)
        
    def change_table(self, data,  time:float=0.0):
        if hasattr(self, 'table'):
            self.table.destroy_table()
            self.miniFrame.destroy()
            delattr(self, 'table')
        if hasattr(self, 'message'):
            self.message.destroy()
            delattr(self, 'message')

        # Create a miniFrame with label and time
        self.miniFrame = customtkinter.CTkFrame(self.app, width=940, height=40)
        self.miniFrame.pack()
        # Create a label with time and number of rows Config total time to show the time in seconds or milliseconds and only use 4 decimal places
        if time < 1:
            self.timeLabel = customtkinter.CTkLabel(self.miniFrame, text=f"Time: {time * 1000:.4f} ms | Rows: {len(data)}")
        else:
            self.timeLabel = customtkinter.CTkLabel(self.miniFrame, text=f"Time: {time:.4f} s | Rows: {len(data)}")
        self.timeLabel.pack(side=customtkinter.LEFT, padx=10)
        # limit data not more than 50 rows
        data = data[:50]
        self.table = Table(self.app, data)
        self.table.create_table()

    def messageLabel(self, message):
        if hasattr(self, 'table'):
            self.table.destroy_table()
            self.miniFrame.destroy()
            delattr(self, 'table')
        if hasattr(self, 'message'):
            self.message.destroy()
            delattr(self, 'message')
        self.message = customtkinter.CTkLabel(self.app, text=message)
        self.message.pack()

    def validation(self, variables, expectedValues):
        """
        Validates if the expected values are present in the variables dictionary.

        Args:
            variables (dict): The dictionary containing the variables to be validated.
            expectedValues (list): A list of expected keys to check in the variables dictionary.

        Returns:
            tuple: A tuple containing a boolean indicating success or failure, and the value associated with the expected keys (or an empty string).
        """
        returnStatement = []
        
        for key in expectedValues:
            # Check if the expected key is in the variables dictionary.
            if key not in variables:
                # If the key is missing, display an informative message and return False.
                self.messageLabel(f"Error: The required variable '{key}' is missing. Please provide '{key}'.")
                return False, ''
            
            # If the key is present, ensure the value is a string.
            returnStatement.append(variables[key] if isinstance(variables[key], str) else '')
        return True, returnStatement

    def obtainOperation(self, command):
        operation, variables = parse_command(command)

        if operation == 'list':
            init_time = time.perf_counter()
            # Call the list method of tableManager
            # Return the result of the list method and display it using change_table
            result = self.tableManager.list_()
            self.change_table(result, time.perf_counter() - init_time)

        elif operation == 'scan':
            init_time = time.perf_counter()
            # Perform validation for the 'is_enabled' operation
            validation, returnStatement = self.validation(variables=variables, expectedValues=['table'])
            # Extract table name from the return statement
            table = returnStatement[0]
            # Call the scan method of tableManager
            # Return the result of the scan method and display it using change_table
            result = self.tableManager.scan(table)
            self.change_table(result, time.perf_counter() - init_time)

        elif operation == 'disable':
            # Perform validation for the 'is_enabled' operation
            validation, returnStatement = self.validation(variables=variables, expectedValues=['table'])
            # Extract table name from the return statement
            table = returnStatement[0]
            # Call the disable method of tableManager
            # Return the result of the disable method and display it using messageLabel
            self.messageLabel(self.tableManager.disable(table))
            
        elif operation == 'enable':
            # Perform validation for the 'is_enabled' operation
            validation, returnStatement = self.validation(variables=variables, expectedValues=['table'])
            # Extract table name from the return statement
            table = returnStatement[0]
            # Call the enable method of tableManager
            # Return the result of the enable method and display it using messageLabel
            self.messageLabel(self.tableManager.enable(table))

        elif operation == 'is_enabled':
            # Perform validation for the 'is_enabled' operation
            validation, returnStatement = self.validation(variables=variables, expectedValues=['table'])
            # Extract table name from the return statement
            table = returnStatement[0]
            # Call the is_enabled method of tableManager
            # Return the result of the isEnabled method and display it using messageLabel
            self.messageLabel(self.tableManager.isEnabled(table))

        elif operation == 'create':
            # Perform validation for the 'create' operation
            validation, returnStatement = self.validation(variables=variables, expectedValues=['table', 'column_families'])
            # Extract table name and column families from the return statement
            # returnStatement should be a tuple with the table name and a list of column families
            table, column_families = returnStatement
            # Ensure column_families is typed as a List[str]
            column_families: List[str] = column_families
            # Call the createTable method of tableManager to create the table with provided column families
            # Return the result of the createTable method and display it using messageLabel
            self.messageLabel(self.tableManager.createTable(table, column_families))

        elif operation == 'get':
            # Measure the initial time for performance tracking
            initial_time = time.perf_counter()
            # Check if 'table' and 'row' are present in the variables dictionary.
            validation, returnStatement = self.validation(variables=variables, expectedValues=['table', 'row'])
            if validation:
                # Extract table name, row, and column name (if provided) from returnStatement
                table, row, column_name = returnStatement
                # If column name is provided, split it into column family and name
                if column_name:
                    column_family = column_name.split(':')[0] if ':' in column_name else None
                    column_name = column_name.split(':')[1] if ':' in column_name else column_name
                    # Retrieve data from the tableManager based on provided parameters
                    result = self.tableManager.get(table, row, column_family, column_name)
                    # Calculate time taken for the operation
                    time_taken = time.perf_counter() - initial_time
                    # Update the table with the retrieved data and time taken
                    self.change_table(result, time_taken)
            # If column name is not provided or validation fails, retrieve data based on table and row only
            result = self.tableManager.get(table, row)
            # Calculate time taken for the operation
            time_taken = time.perf_counter() - initial_time
            # Update the table with the retrieved data and time taken
            self.change_table(result, time_taken)

        elif operation == 'count':
            # Check if 'table' is in the variables dictionary.
            validation, returnStatement = self.validation(variables=variables, expectedValues=['table'])
            if validation:
                # Extract the table name from variables and ensure it's a string.
                table: str = returnStatement[0]
                # Call the count method on tableManager with the table name and display the result.
                self.messageLabel(self.tableManager.count(table))

        elif operation == 'drop':
            # Check if 'table' is in the variables dictionary.
            validation, returnStatement = self.validation(variables=variables, expectedValues=['table'])
            if validation:
                # Extract the table name from variables and ensure it's a string.
                table: str = returnStatement[0]
                # Call the drop method on tableManager with the table name and display the result.
                self.messageLabel(self.tableManager.drop(table))

        elif operation == 'drop_all':
            # Check if 'regex' is in the variables dictionary.
            validation, returnStatement = self.validation(variables=variables, expectedValues=['regex'])
            if validation:
                # Extract the regex from variables and ensure it's a string.
                regex: str = returnStatement[0]
                # Call the drop_all method on tableManager with the regex provided and display the result.
                self.messageLabel(self.tableManager.dropAll(regex))

        elif operation == 'delete':
            # Validate that the required variables 'table', 'row', 'column_name', and 'timestamp' are present in the input.
            validation, returnStatement = self.validation(variables=variables, expectedValues=['table', 'row', 'column_name', 'timestamp'])
            if validation:
                try:
                    # Extract and convert timestamp to float.
                    table, row, column_name, timestamp_str = returnStatement
                    timestamp_float = float(timestamp_str)
                except ValueError:
                    self.messageLabel("Error: Invalid timestamp format. Please provide a valid timestamp.")
                    return  # Exit if timestamp conversion fails

                # Call the delete method on the tableManager with the validated parameters and display the result.
                self.messageLabel(self.tableManager.delete(table, row, column_name, timestamp_float))

        elif operation == 'delete_all':
            # Validate that the required variables 'table', 'row', 'column_name', and 'timestamp' are present in the input.
            validation, returnStatement = self.validation(variables=variables, expectedValues=['table', 'row'])
            if validation:
                    # Unpack the returnStatement list into individual variables.
                    table, row = returnStatement
                    # Call the delete method on the tableManager with the validated parameters and display the result.
                    self.messageLabel(self.tableManager.deleteAll(table, row))

        elif operation == 'put':
            # Validate that the required variables 'table', 'row', 'column', and 'value' are present in the input.
            validation, returnStatement = self.validation(variables=variables, expectedValues=['table', 'row', 'column', 'value'])
            if validation:
                # Unpack the returnStatement list into individual variables.
                table, row, column, value = returnStatement
                # Extract the column family from the column variable.
                column_family = column.split(':')[0] if ':' in column else ''
                column = column.split(':')[1] if ':' in column else column
                # Save in a pickle
                with open(f'./Tables/{table}.hfile', 'wb') as file:
                    pickle.dump(self.tableManager.tables[table], file)
                # Call the put method on the tableManager with the validated parameters and display the result.
                self.messageLabel(self.tableManager.put(table, row, column_family, column, value))

        elif operation == 'alter':
            # Validate that the required variables 'table', 'column_family', and 'columns' are present in the input.
            validation, returnStatement = self.validation(variables=variables, expectedValues=['table'])
            if validation:
                # Unpack the returnStatement list into individual variables.
                table = returnStatement[0]
                # Call the alter method on the tableManager with the validated parameters and display the result.
                self.messageLabel(self.tableManager.alter(table, variables))
                
        elif operation == 'truncate':
            # Validate that the required variable 'table' is present in the input.
            validation, returnStatement = self.validation(variables=variables, expectedValues=['table'])
            if validation:
                # Unpack the returnStatement list into individual variables.
                table: str = returnStatement[0]
                # Call the truncate method on the tableManager with the validated parameters and display the result.
                self.messageLabel(self.tableManager.truncate(table))

        elif operation == 'describe':
            # Validate that the required variable 'table' is present in the input.
            validation, returnStatement = self.validation(variables=variables, expectedValues=['table'])
            if validation:
                # Unpack the returnStatement list into individual variables.
                initial_time = time.perf_counter()
                table: str = returnStatement[0]
                result = self.tableManager.describe(table)

                # Call the describe method on the tableManager with the validated parameters and display the result.
                self.change_table(result, time.perf_counter() - initial_time)

        elif operation == 'insert_many':
            # Validate that the required variable 'table' is present in the input.
            validation, returnStatement = self.validation(variables=variables, expectedValues=['file'])
            if validation:
                # Unpack the returnStatement list into individual variables.
                file: str = returnStatement[0]
                try:
                    # Attempt to read the specified file as JSON
                    with open(file, 'rb') as file:
                        # Load the JSON data from the file
                        data = json.load(file)
                    # Insert the loaded data into the table managed by tableManager
                    self.messageLabel(self.tableManager.insertMany(data))
                except Exception as e:
                    self.messageLabel(f"Error: {e}")

        else:
            self.messageLabel(f"Error: The provided command '{operation}' is not recognized.")

    def mainloop(self):
        self.app.mainloop()

