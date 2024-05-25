import customtkinter

# Create a button with a dropdown menu using CTkComboBox
class Dropdown:
    def __init__(self, master, options, command):
        self.master = master
        self.options = options
        self.command = command

    def create_dropdown(self):
        self.dropdown = customtkinter.CTkComboBox(
            self.master, 
            values=self.options, 
            command=self.command,
            border_width=2
            )
        self.dropdown._dropdown_menu.config(borderwidth=0)


        # Set the first option as the default value, if are empty, set the default value as an empty string
        if len(self.options)>0:
            self.dropdown.set("Select a table")
        else:
            self.dropdown.set("Not has tables")

        # Refactor size to fit the dropdown
        self.dropdown.pack(side="left")

        # Make input uneditable
        self.dropdown.configure(state="readonly")

    def destroy(self):
        self.dropdown.destroy()


