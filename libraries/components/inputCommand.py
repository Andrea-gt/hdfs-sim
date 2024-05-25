import customtkinter


class InputCommand:
    def __init__(self, master, sendFunc:callable) -> None:
        self.master = master
        # Create a frame
        self.frame = customtkinter.CTkFrame(master, width=940, height=100)
        self.frame.pack()
        # Create a label
        self.label = customtkinter.CTkLabel(self.frame, text="Command:")
        self.label.pack(side=customtkinter.LEFT, padx=10)
        # Create a entry
        self.entry = customtkinter.CTkEntry(self.frame, width=500)
        # Make entry use function sendFunc when the user press enter
        self.entry.bind("<Return>", lambda event:sendFunc(self.entry.get()))
        self.entry.pack(side=customtkinter.LEFT)
        # Create a button
        self.button = customtkinter.CTkButton(self.frame, text="Send", 
                                              command=lambda:sendFunc(self.entry.get()), 
                                              border_color='#252525')
        # Make space between the button and the entry
        self.button.pack(side=customtkinter.RIGHT, padx=10)

    def getCommand(self) -> str:
        return self.entry.get()
    
    def setCommand(self, command:str) -> None:
        self.entry.set(command)
