import customtkinter
import tkinter as tk
from CTkTable import CTkTable
import pandas as pd

# Create a table using customtkinter

class Table:
    def __init__(self, master, dataFrame:pd.DataFrame):
        self.master = master
        self.dataFrame: pd.DataFrame = dataFrame
        self.columns = self.dataFrame.columns
        self.rows = self.dataFrame.values.tolist()
        
    def create_table(self):
        # Create a container for the table
        self.table_container = customtkinter.CTkFrame(self.master, bg_color="#252525")
        self.table_container.pack(fill="both", expand=True)

        # Create a canvas for the table
        self.table_canvas = tk.Canvas(self.table_container, bg="#252525", highlightthickness=0)
        self.table_canvas.pack(side="left", fill="both", expand=True)

        # Create a frame for the table
        self.table_frame = customtkinter.CTkFrame(self.table_canvas, bg_color="#252525")
        self.table_frame.pack(fill="both")

        # Create a table
        self.table = CTkTable(self.table_frame, column=len(self.columns), row=len(self.rows)+1, 
                              values=[self.columns]+self.dataFrame.values.tolist(),
                              header_color='#265c8c',
                              font=('Helvetica', 15),
                              width=238,
                              height=40,
                              border_width=10,
                              border_color='#252525',
                              write=True,
                              corner_radius=30
                              )

    
        # Añadir binding a cada celda de la tabla
        for element in self.table.frame.values():
            if isinstance(element, customtkinter.CTkEntry):
                element.configure(state='readonly', border_width=0)

        
        # Change header font          
        self.table.pack(fill="both", expand=True)
        self.table_canvas.create_window(0, 0, window=self.table_frame, anchor="nw")
        self.table_canvas.update_idletasks()
        # Actualizar el scrollregion después de agregar todos los widgets
        self.table_frame.update_idletasks()
        self.table_canvas.config(scrollregion=self.table_canvas.bbox("all"))

        # Bind the mousewheel event to the canvas
        self.table_canvas.bind_all("<MouseWheel>", self._on_mousewheel)


    def destroy_table(self):
        self.table_container.destroy()
        self.master.update()

    def _on_mousewheel(self, event):
        if len(self.dataFrame) > 11:
            self.table_canvas.yview_scroll(-1*(event.delta//120), "units")
        if len(self.columns) > 3:
            self.table_canvas.xview_scroll(-1*(event.delta//120), "units")
            

    

        
        



