import pyodbc

def get_connection():
    
    return pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=DESKTOP-FOIB0OT;'
        'DATABASE=DATARULE;'
       'Trusted_Connection=yes;'
    )

def get_rule_json(cursor,id_rule):
    cursor.execute("SELECT text_JSON FROM ref_regle WHERE id_regle=?",id_rule)
    row = cursor.fetchone()
    return row[0] if row else None


def get_last_value(cursor, var_id):
    
    cursor.execute(
        "SELECT TOP 1 val_valide FROM his_valeur WHERE id_variable = ?  ORDER BY date_acquisition DESC",
        var_id
    )
    result = cursor.fetchone()
    return result[0] if result else None


#def mark_as_processed(cursor, var_id):
    #cursor.execute("""
       # UPDATE his_valeur
       # SET id_qualification = 1
       # WHERE id_variable = ?
       #   AND id_qualification = 0
       # AND date_acquisition = (
       #     SELECT MIN(date_acquisition)
        #    FROM his_valeur
       #     WHERE id_variable = ? AND id_qualification = 0
       # )
  #  """, (var_id, var_id))




def insert_result(cursor, var_id, valeur):
    cursor.execute("""
        INSERT INTO his_valeur (
            id_variable, date_acquisition, date, id_qualification, val_brute, val_valide
        )
        VALUES (?, GETDATE(), 0, ?,?)
    """, var_id, valeur, valeur)
