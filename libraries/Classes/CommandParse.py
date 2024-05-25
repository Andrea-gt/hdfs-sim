import shlex

def parse_command(command):
    # Separar la operación del resto del comando
    parts = shlex.split(command)
    if len(parts) == 0:
        return None, {}

    operation = parts[0]
    variables = {}

    # Procesar las variables/componentes
    for part in parts[1:]:
        if part.startswith('-'):
            key_value = part[1:].split('=', 1)  # Split by '=' only once
            if len(key_value) == 2:
                key, value = key_value
                # Detectar y manejar listas
                if value.startswith('[') and value.endswith(']'):
                    # Remover los corchetes exteriores
                    value = value[1:-1]
                    # Separar por comas, sin necesidad de comillas
                    value = value.split(',')
                variables[key] = value
            else:
                variables[key_value[0]] = None

    return operation, variables

