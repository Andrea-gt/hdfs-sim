import json
import random
import string

def generate_random_value(length=6):
    """Genera un valor aleatorio."""
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

def generate_row(row_id):
    """Genera una fila con datos aleatorios."""
    return {
        "Hola": {
            "Column1": generate_random_value(),
            "Column2": generate_random_value(),
            "Column3": generate_random_value()
        },
        "Que": {
            "ColumnA": generate_random_value(),
            "ColumnB": generate_random_value(),
            "ColumnC": generate_random_value()
        }
    }

def generate_table(num_rows):
    """Genera una tabla con un número específico de filas."""
    table = {}
    for i in range(1, num_rows + 1):
        row_id = f"row{i}"
        table[row_id] = generate_row(row_id)
    return {"dinamo": table}

# Generar una tabla con 3 filas
data = generate_table(10000)

# Convertir el diccionario a una cadena JSON
json_data = json.dumps(data, indent=2)

# Guardar el JSON en un archivo
with open('data.json', 'w') as f:
    f.write(json_data)

print(json_data)
