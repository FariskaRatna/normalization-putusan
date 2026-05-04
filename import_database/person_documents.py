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

def import_person_documents(json_file_path):
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Gagal membaca file: {e}")
        return

    defendants = data.get("who", {}).get("defendants", [])
    
    doc_mapping = {
        "nik": "NIK",
        "passport_no": "Passport",
        "kk_no": "KK"
    }

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        for person in defendants:
            name = person.get("normalized_name")
            if not name or name == "Tidak Diketahui":
                continue
                
            cursor.execute("SELECT id FROM public.persons WHERE fullname = %s", (name,))
            res = cursor.fetchone()
            if not res:
                continue
            
            person_id = res[0]
            
            doc_records = []
            for json_key, doc_label in doc_mapping.items():
                doc_number = person.get(json_key)
                
                if doc_number and str(doc_number).lower() != "tidak diketahui":
                    doc_records.append((
                        person_id, 
                        doc_label, 
                        doc_number, 
                        None # issuing_region
                    ))
            
            if doc_records:
                insert_query = """
                    INSERT INTO public.person_documents 
                    (person_id, document_type, document_number, issuing_region, created_at, updated_at)
                    VALUES %s
                    ON CONFLICT DO NOTHING;
                """
                execute_values(
                    cursor, insert_query, doc_records,
                    template="(%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                )
        
        conn.commit()
        print("Berhasil memproses dokumen terdakwa.")
        
    except Exception as e:
        print(f"Error Database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()