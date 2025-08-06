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
    Values=[]
    # Analyse des blocs
    for block in blocks:
        if block["class"] == "ReadVar":
            var_id = block["parameters"]["Id"]
            val = get_last_value(cursor, var_id)
            Values.append(val)
            varIds.append(var_id)
    print(Values)   

        

if __name__ == "__main__":
    main()
