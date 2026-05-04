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

def import_mitigating_factors(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    mitigating_factors = data.get("why", {}).get("mitigating_factors", [])
    case_number = data.get("what", {}).get("case_number", None)


    if isinstance(mitigating_factors, str):
        mitigating_factors = [mitigating_factors]

    if not mitigating_factors:
        print("Tidak ada mitigating factors di file")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        mitigating_baru = 0
        mitigating_lama = 0

        id_cases = get_case_id(cursor, case_number)
        
        for description in mitigating_factors:
            clean_description = description.strip()
            if not clean_description:
                continue

            cursor.execute("SELECT id FROM public.mitigating_factors WHERE description = %s", (clean_description,))
            result = cursor.fetchone()

            if result:
                mitigating_id = result[0]
                mitigating_lama += 1
            else: 
                insert_query = """
                    INSERT INTO public.mitigating_factors (id_cases, description, created_at, updated_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id;
                """

                cursor.execute(insert_query, (id_cases, clean_description,))

                mitigating_id = cursor.fetchone()[0]
                mitigating_baru += 1
                print(f"Mitigating factor BARU ditambahkan: {clean_description} (ID: {mitigating_id})")
                
        conn.commit()
        print(f"Selesai: {mitigating_baru} Baru, {mitigating_lama} Sudah Ada")

    except Exception as e:
        print(f"Error database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()