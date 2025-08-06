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
    print(blocks)
    
    

    

if __name__ == "__main__":
    main()
