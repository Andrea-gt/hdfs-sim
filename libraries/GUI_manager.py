import customtkinter
from .components import InputCommand, Table
import pandas as pd
from .Classes import Table as TableClass

class GUI_manager:
    def __init__(self):
        self.app = customtkinter.CTk()
        self.app.title('Hbase GUI Manager')
        self.app.geometry("960x540")
        self.app.resizable(False, False)
        self.app.minsize(960,540)
        self.app.maxsize(960,540)

        # Make input command
        self.inputCommand = InputCommand(self.app, self.obtainOperation)
        
        

    def on_dropdown_select(self, value):
        print(value)

    def change_table(self, data):
        if hasattr(self, 'table'):
            self.table.destroy_table()
            delattr(self, 'table')
        self.table = Table(self.app, data)
        self.table.create_table()

    def obtainOperation(self, operation):
        print(f"Operation: {operation}")

    def mainloop(self):
        self.app.mainloop()






