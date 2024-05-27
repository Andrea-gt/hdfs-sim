import customtkinter
from .components import InputCommand, Table
import pandas as pd
from .Classes import Table as TableClass, parse_command
from .TableManager import TableManager
from typing import List, Dict, Any
import time

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
        
    def on_dropdown_select(self, value):
        print(value)

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
        print(f"Command: {command}")
        operation, variables = parse_command(command)

        if operation == 'list':
            initial_time = time.perf_counter()
            result = self.tableManager.list_()
            total_time = time.perf_counter() - initial_time
            self.change_table(result, total_time)

        elif operation == 'scan':
            if 'table' not in variables:
                self.messageLabel("Table not found in command. Please, insert a table name.")
                return
            table:str = variables['table'] if isinstance(variables['table'], str) else ''
            initial_time = time.perf_counter()
            result = self.tableManager.scan(table)
            self.change_table(result, time.perf_counter() - initial_time)

        elif operation == 'disable':
            if 'table' not in variables:
                self.messageLabel("Table not found in command. Please, insert a table name.")
                return
            table:str = variables['table'] if isinstance(variables['table'], str) else ''
            self.messageLabel(self.tableManager.disable(table))
            
        elif operation == 'enable':
            if 'table' not in variables:
                self.messageLabel("Table not found in command. Please, insert a table name.")
                return
            table:str = variables['table'] if isinstance(variables['table'], str) else ''
            self.messageLabel(self.tableManager.enable(table))

        elif operation == 'is_enabled':
            if 'table' not in variables:
                self.messageLabel("Table not found in command. Please, insert a table name.")
                return
            table:str = variables['table'] if isinstance(variables['table'], str) else ''
            self.messageLabel(self.tableManager.isEnable(table))

        elif operation == 'create':
            if 'table' not in variables:
                self.messageLabel("Table not found in command. Please, insert a table name.")
                return
            table:str = variables['table'] if isinstance(variables['table'], str) else ''
            if 'columns' not in variables:
                self.messageLabel("Columns not found in command. Please, insert columns.")
                return
            columns:List[str] = variables['columns'] if isinstance(variables['columns'], list) else [variables['columns']]
            self.messageLabel(self.tableManager.createTable(table, columns))

        elif operation == 'get':
            if 'table' not in variables:
                self.messageLabel("Table not found in command. Please, insert a table name.")
                return
            table:str = variables['table'] if isinstance(variables['table'], str) else ''
            if 'rowkey' not in variables:
                self.messageLabel("Rowkey not found in command. Please, insert a rowkey.")
                return
            rowkey:str = variables['rowkey'] if isinstance(variables['rowkey'], str) else ''
            initial_time = time.perf_counter()
            result = self.tableManager.get(table, rowkey)
            total_time = time.perf_counter() - initial_time
            self.change_table(result, total_time)

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

        elif operation == 'deleteall':
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
                # Call the put method on the tableManager with the validated parameters and display the result.
                self.messageLabel(self.tableManager.put(table, row, column_family, column, value))

        elif operation == 'truncate':
            # Validate that the required variable 'table' is present in the input.
            validation, returnStatement = self.validation(variables=variables, expectedValues=['table'])
            if validation:
                # Unpack the returnStatement list into individual variables.
                table: str = returnStatement
                # Call the truncate method on the tableManager with the validated parameters and display the result.
                self.messageLabel(self.tableManager.put(table))

    def mainloop(self):
        self.app.mainloop()

