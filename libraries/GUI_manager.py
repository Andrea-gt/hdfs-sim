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
            if 'table' not in variables:
                self.messageLabel("Table not found in command. Please, insert a table name.")
                return
            table:str = variables['table'] if isinstance(variables['table'], str) else ''
            self.messageLabel(self.tableManager.count(table))
        
        elif operation == 'drop':
            if 'table' not in variables:
                self.messageLabel("Table not found in command. Please, insert a table name.")
                return
            table:str = variables['table'] if isinstance(variables['table'], str) else ''
            ## CALL DROP TABLE

    def mainloop(self):
        self.app.mainloop()

