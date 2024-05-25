from libraries import GUI_manager, Table
import pickle

structDict = {'info': ['name', 'age'], '': ['id']}

table = Table(structDict)
table.insertOne({'info': {'name': 'John', 'age': 25}, '': {'id': 1}})
table.insertOne({'info': {'name': 'Jane', 'age': 22}, '': {'id': 2}})

table.obtainTableInfo()

dictTable = {'1': table.obtainTableInfo()}

#with open('./Tables/1.hfile', 'wb') as file:
#    pickle.dump(table, file)


# GUI_manager()
gui = GUI_manager('./Tables')
gui.mainloop()