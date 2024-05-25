import customtkinter
from .components import InputCommand, Table
import pandas as pd
from .Classes import Table as TableClass, parse_command
from .TableManager import TableManager
from typing import List, Dict, Any

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

    def change_table(self, data):
        if hasattr(self, 'table'):
            self.table.destroy_table()
            delattr(self, 'table')
        if hasattr(self, 'message'):
            self.message.destroy()
            delattr(self, 'message')
        self.table = Table(self.app, data)
        self.table.create_table()

    def messageLabel(self, message):
        if hasattr(self, 'table'):
            self.table.destroy_table()
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
            self.change_table(self.tableManager.list_())
        elif operation == 'scan':
            if 'table' not in variables:
                self.messageLabel("Table not found in command. Please, insert a table name.")
                return
            table:str = variables['table'] if isinstance(variables['table'], str) else ''
            self.change_table(self.tableManager.scan(table))
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
        elif operation == 'isenable':
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
            result = self.tableManager.get(table, rowkey)
            self.change_table(result)
        

    def mainloop(self):
        self.app.mainloop()

