import customtkinter
from .components import InputCommand, Table
import pandas as pd
from .Classes import Table as TableClass
from .TableManager import TableManager

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
        command = command.split(' ')
        operation = command[0].lower()

        if operation == 'list':
            self.change_table(self.tableManager.list_())
        elif operation == 'scan':
            table = command[1]
            self.change_table(self.tableManager.scan(table))
        elif operation == 'disable':
            table = command[1]
            self.messageLabel(self.tableManager.disable(table))
        elif operation == 'enable':
            table = command[1]
            self.messageLabel(self.tableManager.enable(table))
        elif operation == 'isenable':
            table = command[1]
            self.messageLabel(self.tableManager.isEnable(table))


    def mainloop(self):
        self.app.mainloop()






