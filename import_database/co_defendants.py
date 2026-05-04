import json
import psycopg2
import os
from psycopg2.extras import execute_values, RealDictCursor

DB_CONFIG = {
    "dbname": "terrorism_metadata",
    "user": "is02",
    "password": "open123",
    "host": "localhost",
    "port": "5433"
}

def import_co_defendants(json_path_file):
    try:
        with open(json_path_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Gagal memuat file: {e}")
        return
    
    co_defendants = data.get("who", {}).get("normalized_co_defendants", [])
    
    if not co_defendants or co_defendants == ["Tidak Diketahui"]:
        print("Tidak ada co-defendants valid di file ini.")
        return
    
    incoming_names = set()
    for name in co_defendants:
        if name and name != "Tidak Diketahui":
            incoming_names.add(name)

    if not incoming_names:
        return

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        check_query = "SELECT fullname FROM public.persons WHERE fullname = ANY(%s);"
        cursor.execute(check_query, (list(incoming_names),))
        
        existing_names = {row[0] for row in cursor.fetchall()}

        records_to_insert = []
        for name in incoming_names:
            if name not in existing_names:
                records_to_insert.append((
                    name, "Tidak Diketahui", "Tidak Diketahui", "Tidak Diketahui", 
                    "Tidak Diketahui", "Tidak Diketahui", "Tidak Diketahui", 
                    None, "Tidak Diketahui"
                ))

        if records_to_insert:
            insert_query = """
                INSERT INTO public.persons 
                (fullname, gender, nationality, religion, occupation, 
                 address, place_of_birth, date_of_birth, education_status, 
                 created_at, updated_at) 
                VALUES %s;
            """
            execute_values(
                cursor, 
                insert_query, 
                records_to_insert,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            )
            print(f"Berhasil menambahkan {len(records_to_insert)} co-defendants baru.")
        else:
            print("Semua co-defendants di file ini sudah ada di database (SKIP).")
        
        conn.commit()
        
    except Exception as e:
        print(f"Error Database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()