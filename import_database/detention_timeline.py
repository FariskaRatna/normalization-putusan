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

def import_detention_timeline(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    detention_timeline = data.get("when", {}).get("detention_timeline", [])
    case_number = data.get("what", {}).get("case_number", None)

    if not detention_timeline:
        print("Tidak ada detention timeline di file")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        detention_baru = 0
        detention_lama = 0

        id_cases = get_case_id(cursor, case_number)
        
        for item in detention_timeline:
            if not isinstance(item, dict):
                continue
                
            description = item.get("description", "").strip()
            date_str = item.get("date", "").strip()
            start_date = item.get("start_date")
            end_date = item.get("end_date")

            if not description and not date_str:
                continue

            check_query = """
                SELECT id FROM public.detention_timeline 
                WHERE id_cases = %s AND description = %s AND detention_date = %s
            """
            cursor.execute(check_query, (id_cases, description, date_str))
            result = cursor.fetchone()

            if result:
                detention_id = result[0]
                detention_lama += 1
            else: 
                insert_query = """
                    INSERT INTO public.detention_timeline 
                    (id_cases, description, detention_date, start_date, end_date, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id;
                """
                
                cursor.execute(insert_query, (id_cases, description, date_str, start_date, end_date))

                detention_id = cursor.fetchone()[0]
                detention_baru += 1
                print(f"Detention timeline BARU ditambahkan: {description} ({date_str}) (ID: {detention_id})")
                
        conn.commit()
        print(f"Selesai: {detention_baru} Baru, {detention_lama} Sudah Ada")

    except Exception as e:
        print(f"Error database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()