import json
from database import get_connection, get_rule_json, get_last_value, insert_result, mark_as_processed

def main():
    conn = get_connection()
    cursor = conn.cursor()

    # Récupération du JSON de règle
    json_text = get_rule_json(cursor,1)
    if not json_text:
        raise Exception("Aucune règle")

    json_data = json.loads(json_text)
    blocks = json_data["blocks"]

    variables = {}       # Dictionnaire des variables lues dynamiquement
    operation_code = ""  # Chaîne de code dynamique à exécuter
    result_var = None    # ID de la variable où stocker le résultat

    # Analyse des blocs
    for block in blocks:
        if block["class"] == "ReadVar":
            var_id = block["parameters"]["Id"]
            val = get_last_value(cursor, var_id)
            var_name = f"var_{var_id}"
            variables[var_name] = val

        elif block["class"] in ["+", "-", "*", "/"]:
            op = block["class"]
/*************  ✨ Windsurf Command 🌟  *************/
            input_ids = block["parameters"]["Id"]
            inputs = []
            for var_id in input_ids:
                var_name = f"var_{var_id}"
                if var_name not in variables:
                    raise Exception(f"Variable {var_name} non définie")
                inputs.append(var_name)
            inputs = [f"var_{var_id}" for var_id in input_ids]
/*******  da833e5b-2753-4df8-a852-b5ad6aaca9c4  *******/

            # Construit l'expression mathématique dynamique
            expression = f" {op} ".join(inputs)
            operation_code = f"result = {expression}"

        elif block["class"] == "WriteVar":
            result_var = block["parameters"]["Id"]

    # Exécution de l'opération dynamiquement
    local_vars = {}
    local_vars.update(variables)
    exec(operation_code, {}, local_vars)
    result = local_vars["result"]

    print(f"Résultat: {result}")

    # Insère le résultat dans la base
    insert_result(cursor, result_var, result)

    # Marque les ReadVar comme traitées
    for block in blocks:
        if block["class"] == "ReadVar":
            var_id = block["parameters"]["Id"]
            mark_as_processed(cursor, var_id)

    conn.commit()
    print("Résultat stocké et variables marquées comme traitées avec succès.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
