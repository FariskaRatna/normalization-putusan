import json
import psycopg2
import os
from psycopg2.extras import execute_values, RealDictCursor
from import_id import get_case_id

DB_CONFIG = {
    "dbname": "terrorism_metadata",
    "user": "is02",
    "password": "open123",
    "host": "localhost",
    "port": "5433"
}

def import_monetary_amounts(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    what_obj = data.get("what", {})
    case_number = what_obj.get("case_number", None)

    monetary_fields = {
        "monetary_penalties": "normalized_monetary_penalties",
        "seized_money_amount": "normalized_seized_money_amount"
    }

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        id_cases = get_case_id(cursor, case_number)
        if not id_cases:
            print(f"Kasus {case_number} tidak ditemukan.")
            return

        baru = 0
        lama = 0

        for field_key, amount_type in monetary_fields.items():
            raw_value = what_obj.get(field_key)
            amount = raw_value

            if amount is None:
                continue

            check_query = """
                SELECT id FROM public.case_monetary_amounts 
                WHERE case_id = %s AND amount_type = %s
            """
            cursor.execute(check_query, (id_cases, amount_type))
            result = cursor.fetchone()

            if result:
                lama += 1
            else:
                # 2. Insert data baru
                insert_query = """
                    INSERT INTO public.case_monetary_amounts 
                    (case_id, amount_type, amount, currency, notes, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                
                notes = f"Hasil ekstraksi dari field {field_key}"
                cursor.execute(insert_query, (id_cases, amount_type, amount, "IDR", notes))
                baru += 1
                print(f"Nominal BARU ({amount_type}): {amount} ditambahkan ke kasus {case_number}")

        conn.commit()
        print(f"✅ Selesai: {baru} Data Baru, {lama} Sudah Ada.")

    except Exception as e:
        print(f"❌ Error Database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()