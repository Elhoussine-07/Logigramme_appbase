import json
from database import get_connection, get_rule_json, get_last_value, insert_result

def main():
    conn = get_connection()
    cursor = conn.cursor()

 
    json_text = get_rule_json(cursor, 'Addition Test')
    if not json_text:
        raise Exception("Aucune règle trouvée avec le nom 'Addition Test'")

    json_data = json.loads(json_text)
    blocks = json_data["blocks"]

    read_vars = []
    write_var_id = None
    operation = None

    # ✅ Extraire ReadVar, +, WriteVar
    for block in blocks:
        if block["class"] == "ReadVar":
            read_vars.append(block["parameters"]["Id"])
        elif block["class"] in ["+", "-", "*", "/"]:
            operation = block["class"]
        elif block["class"] == "WriteVar":
            write_var_id = block["parameters"]["Id"]

    # ✅ Récupérer les valeurs des ReadVars
    values = []
    for var_id in read_vars:
        val = get_last_value(cursor, var_id)
        if val is None:
            raise Exception(f"Aucune valeur trouvée pour variable {var_id}")
        values.append(val)

    # ✅ Calcul
    if operation == "+":
        result = values[0] + values[1]
    elif operation == "-":
        result = values[0] - values[1]
    elif operation == "*":
        result = values[0] * values[1]
    elif operation == "/":
        result = values[0] / values[1]
    else:
        raise Exception("Opération non supportée")

    print(f"Résultat: {result}")

    # ✅ Stocker résultat
    insert_result(cursor, write_var_id, result)
    conn.commit()

    print("✅ Résultat stocké avec succès.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
