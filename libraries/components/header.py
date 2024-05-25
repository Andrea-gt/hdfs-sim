from .dropdown import Dropdown
import customtkinter
from typing import List, Dict

# Create a header with a dropdown menu and buttons
class Header:
    def __init__(self, master, tables:List[str], operations:Dict[str, callable], funcCreateTable:callable, change_table:callable):
        self.master = master
        self.tables = tables
        self.operations = operations
        self.selected_table = ""
        # if has tables in the list
        if len(self.tables)>0:
            self.selected_table = self.tables[0]

        self.create_table = funcCreateTable
        self.change_table = change_table

    def create_header(self):
        # Create a frame
        self.frame = customtkinter.CTkFrame(self.master)
        self.frame.pack(fill="x")

        # Create a dropdown
        self.dropdown = Dropdown(self.frame, self.tables, self.change_table)
        self.dropdown.create_dropdown()

        # Create a button to create a table
        button = customtkinter.CTkButton(self.frame, text="Create Table", command=self.create_table)

        # Create buttons
        for operation in self.operations:
            button = customtkinter.CTkButton(self.frame, text=operation, command=self.operations[operation])
            button.pack(side="left")

    def obtain_table(self, selected_table):
        self.selected_table = selected_table
        print(f"Selected table: {selected_table}")

    def get_selected_table(self):
        return self.selected_table
    
    def destroy(self):
        self.frame.destroy()
        self.dropdown.destroy()

    def set_tables(self, tables):
        self.tables = tables
        self.dropdown.destroy()
        self.dropdown = Dropdown(self.frame, self.tables, self.change_table)
        self.dropdown.create_dropdown()
        self.master.update()

        

    




