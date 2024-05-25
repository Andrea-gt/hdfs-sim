from libraries import GUI_manager, Table

structDict = {'info': ['name', 'age'], '': ['id']}

table = Table(structDict)
table.insertOne({'info': {'name': 'John', 'age': 25}, '': {'id': 1}})
table.insertOne({'info': {'name': 'Jane', 'age': 22}, '': {'id': 2}})

table.obtainTableInfo()



dictTable = {'1': table.obtainTableInfo()}

# GUI_manager()
gui = GUI_manager()
gui.mainloop()