import json
import psycopg2
import os
from psycopg2.extras import execute_values, RealDictCursor
from import_id import get_case_id, get_legal_id

DB_CONFIG = {
    "dbname": "terrorism_metadata",
    "user": "is02",
    "password": "open123",
    "host": "localhost",
    "port": "5433"
}

def import_case_charge_articles(json_file_path):
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    case_number = data.get("what", {}).get("case_number", None)
    articles = data.get("what", {}).get("normalized_articles", [])
    
    if isinstance(articles, str):
        articles = [articles]

    if not articles:
        print("Tidak ada data normalized_articles di file ini.")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        article_baru = 0
        article_lama = 0

        # Dapatkan ID kasus
        id_cases = get_case_id(cursor, case_number)
        if not id_cases:
            print(f"Kasus {case_number} tidak ditemukan di tabel cases.")
            return

        for article_text in articles:
            clean_article = article_text.strip()
            if not clean_article:
                continue
                
            id_article = get_legal_id(cursor, clean_article)
            
            if not id_article:
                print(f"Pasal '{clean_article}' tidak ditemukan di database master. Melewati...")
                continue

            cursor.execute(
                "SELECT id FROM public.case_charged_articles WHERE case_id = %s AND article_id = %s",
                (id_cases, id_article)
            )
            result = cursor.fetchone()

            if result:
                article_lama += 1
            else:
                insert_query = """
                    INSERT INTO public.case_charged_articles (case_id, article_id, created_at, updated_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id;
                """
                cursor.execute(insert_query, (id_cases, id_article))
                new_id = cursor.fetchone()[0]
                print(f"Relasi BARU ditambahkan: {clean_article} (ID Relasi: {new_id})")
                
                article_baru += 1

        conn.commit()
        print(f"Selesai: {article_baru} Relasi Baru, {article_lama} Relasi Sudah Ada")

    except Exception as e:
        print(f"Error database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()