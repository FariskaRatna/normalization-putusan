import json
import psycopg2
import os
from psycopg2.extras import execute_values, RealDictCursor
from import_id import get_city_id

DB_CONFIG = {
    "dbname": "terrorism_metadata",
    "user": "is02",
    "password": "open123",
    "host": "localhost",
    "port": "5433"
}

def import_court(json_file_path):
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    where_block = data.get("where", {})

    court_name = where_block.get("district_court")
    court_code = where_block.get("normalized_court_code")
    city_name = where_block.get("normalized_court_city")

    if not court_name or "Tidak Diketahui" in court_name or "Data Kosong" in court_name:
        print(f"SKIP: Data pengadilan tidak ada")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # 2. Cari City ID (Foreign Key)
        city_id = get_city_id(cursor, city_name)

        # 3. Lakukan UPSERT ke tabel courts
        insert_query = """
            INSERT INTO courts (court_name, court_code, court_city_id, created_at, updated_at)
            VALUES (%s, %s, %s, NOW(), NOW())
            ON CONFLICT (court_name) 
            DO UPDATE SET 
                court_code = EXCLUDED.court_code,
                court_city_id = EXCLUDED.court_city_id,
                updated_at = NOW()
            RETURNING id;
        """
        cursor.execute(insert_query, (court_name, court_code, city_id))
        court_id = cursor.fetchone()['id']
        conn.commit()
        
        print(f"SUKSES: [{court_name}] tersimpan/terupdate dengan ID: {court_id}")

    except psycopg2.Error as e:
        print(f"DATABASE ERROR: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()