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

def import_article_charges(json_file_path):
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Gagal membaca file: {e}")
        return
    
    charges = data.get("what", {}).get("normalized_articles", [])
    if not charges:
        print("Tidak ada pasal yang diimport di file ini")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for charge in charges:
            cursor.execute("SELECT id FROM public.legal_articles WHERE article_text = %s", (charge,))
            result = cursor.fetchone()

            if not result:
                cursor.execute(
                    "INSERT INTO public.legal_articles (article_text) VALUES (%s) RETURNING id",
                    (charge,)
                )

                new_id = cursor.fetchone()[0]
                print(f"Pasal baru ditambahkan: {charge} (ID: {new_id})")
            else:
                print(f"Pasal sudah ada: {charge}")
            
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()