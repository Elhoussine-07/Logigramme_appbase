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


#


def mark_as_processed(cursor, var_id):
    cursor.execute("""
        UPDATE his_valeur
        SET id_qualification = 1
        WHERE id_variable = ?
        AND id_qualification = 0
        AND date_acquisition = (
            SELECT MIN(date_acquisition)    
            FROM his_valeur
            WHERE id_variable = ? AND id_qualification = 0
        )
     """, (var_id, var_id))




def insert_result(cursor, var_id, valeur):
    cursor.execute("""
        INSERT INTO his_valeur (
            id_variable, date_acquisition, id_qualification, date_insertion,  val_brute, val_valide
        )
        VALUES (?, GETDATE() ,0,GETDATE(), ?,?)
    """, var_id, valeur, valeur)



def get_common_values(cursor, variable_ids):
    # Dictionnaire des dates par variable
    dates_by_var = {}
    for var_id in variable_ids:
        cursor.execute("""
            SELECT date_acquisition
            FROM his_valeur
            WHERE id_variable = ? AND id_qualification = 0
        """, (var_id,))
        dates_by_var[var_id] = set(row[0] for row in cursor.fetchall())

    # Intersection de toutes les dates
    common_dates = set.intersection(*dates_by_var.values())
    common_dates = sorted(common_dates)  # triées pour traitement séquentiel

    # Récupération des valeurs par date
    results = []
    for date in common_dates:
        values = []
        for var_id in variable_ids:
            cursor.execute("""
                SELECT val_valide
                FROM his_valeur
                WHERE id_variable = ? AND id_qualification = 0 AND date_acquisition = ?
            """, (var_id, date))
            row = cursor.fetchone()
            if row:
                values.append(row[0])
            else:
                break
        if len(values) == len(variable_ids):
            results.append((date, values))
    
    return results  # Liste de tuples (date, [valeurs])
