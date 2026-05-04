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

def import_officials_to_persons(json_path_file):
    try:
        with open(json_path_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Gagal memuat file: {e}")
        return
    
    who_block = data.get("who", {})
    
    official_keys = [
        "normalized_defense_counsels",
        "normalized_judges",
        "normalized_clerk",
        "normalized_prosecutors"
    ]

    unique_names = set() 
    
    for key in official_keys:
        names_list = who_block.get(key, [])
        for name in names_list:
            if name and name.lower() != "tidak diketahui":
                unique_names.add(name)

    if not unique_names:
        print("Tidak ada pejabat pengadilan valid untuk diimport di file ini.")
        return

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        check_query = "SELECT fullname FROM public.persons WHERE fullname = ANY(%s);"
        cursor.execute(check_query, (list(unique_names),))
        
        existing_names = {row[0] for row in cursor.fetchall()}

        new_persons_to_insert = []
        for name in unique_names:
            if name not in existing_names:
                new_persons_to_insert.append((
                    name, "Tidak Diketahui", "Tidak Diketahui", "Tidak Diketahui", 
                    "Tidak Diketahui", "Tidak Diketahui", "Tidak Diketahui", 
                    None, "Tidak Diketahui"
                ))

        if new_persons_to_insert:
            insert_persons_query = """
                INSERT INTO public.persons 
                (fullname, gender, nationality, religion, occupation, 
                 address, place_of_birth, date_of_birth, education_status, 
                 created_at, updated_at) 
                VALUES %s;
            """
            execute_values(
                cursor, insert_persons_query, new_persons_to_insert,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            )
            print(f"Berhasil menambahkan {len(new_persons_to_insert)} pejabat pengadilan baru ke tabel persons.")
        else:
            print("Semua pejabat pengadilan di file ini sudah ada di database (SKIP).")

        conn.commit()
        
    except Exception as e:
        print(f"Error Database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()