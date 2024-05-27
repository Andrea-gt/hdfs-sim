# Import necessary modules
from libraries import GUI_manager, Table
import pickle

# Initialize the GUI manager with the path './Tables'
gui = GUI_manager('./Tables')

# Start the main event loop of the GUI, which waits for user interactions
gui.mainloop()