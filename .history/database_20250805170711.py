# database.py

import pyodbc

def get_connection():
    return pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=DESKTOP-FOIB0OT;'
        'DATABASE=DataRuleDB;'
       'Trusted_Connection=yes;'
    )

def get_rule_json(cursor, rule_name):
    cursor.execute("SELECT text_JSON FROM ref_regle WHERE lib_nom = ?", rule_name)
    row = cursor.fetchone()
    return row[0] if row else None

# ✅ Lire la dernière valeur d'une variable depuis his_valeur
def get_last_value(cursor, var_id):
    cursor.execute("""
        SELECT TOP 1 val_valide
        FROM his_valeur
        WHERE id_variable = ?
        ORDER BY date_acquisition DESC
    """, var_id)
    result = cursor.fetchone()
    return result[0] if result else None

# ✅ Insérer un résultat dans his_valeur
def insert_result(cursor, var_id, valeur):
    cursor.execute("""
        INSERT INTO his_valeur (
            id_variable, date_acquisition, val_brute, id_analyse,
            sig_libelle, conformite_auto, date_insertion, val_valide,
            code_transition, id_qualification
        )
        VALUES (?, GETDATE(), ?, 1, 'Result', 1, SYSDATETIME(), ?, 0, 1)
    """, var_id, valeur, valeur)
