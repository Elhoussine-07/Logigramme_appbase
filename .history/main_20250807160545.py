import json
from database import get_connection, get_rule_json, insert_result,mark_as_processed,get_common_values

def main():
    conn = get_connection()
    cursor = conn.cursor()

    json_text = get_rule_json(cursor, 1)
    if not json_text:
        raise Exception("Aucune règle")

    json_data = json.loads(json_text)
    blocks = json_data["blocks"]

    variable_ids = []
    operation = None
    result_varId = None

    for block in blocks:
        if block["class"] == "ReadVar":
            var_id = block["parameters"]["Id"]
            variable_ids.append(var_id)

        elif block["class"] in ["+", "-", "*", "/"]:
            operation = block["class"]

        elif block["class"] == "WriteVar":
            result_varId = block["parameters"]["Id"]

    # Récupère les valeurs par date commune
    dated_values = get_common_values(cursor, variable_ids)

    for date, values in dated_values:
        expr = f" {operation} ".join(str(v) for v in values)
        local_vars = {}
        exec(f"result = {expr}", {}, local_vars)
        result = local_vars["result"]

        print(f"Date: {date} | Résultat: {result}")
        
        # Insère le résultat avec la date
        cursor.execute("""
            INSERT INTO his_valeur (
                id_variable, date_acquisition, id_qualification, date_insertion, val_brute, val_valide
            )
            VALUES (?, ?, 0, GETDATE(), ?, ?)
        """, (result_varId, date, result, result))

        # Marquer les valeurs comme traitées
        for var_id in variable_ids:
            cursor.execute("""
                UPDATE his_valeur
                SET id_qualification = 1
                WHERE id_variable = ? AND date_acquisition = ? AND id_qualification = 0
            """, (var_id, date))

    conn.commit()
    print("Traitement terminé.")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
    