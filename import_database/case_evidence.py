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

def import_case_evidence(json_file_path):
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Gagal membaca file: {e}")
        return

    what_block = data.get("what", {})
    
    case_number = str(what_block.get("case_number", "")).strip()
    
    # Terapkan logika sensor 'xxx' yang sama agar cocok dengan database
    if "xxx" in case_number.lower():
        nama_file = os.path.basename(json_file_path)
        case_number = f"{case_number} [Sensor: {nama_file}]"

    if not case_number or case_number in ["Tidak Diketahui", "Data Kosong"]:
        print(f"⏩ SKIP: Nomor kasus tidak ada di {json_file_path}")
        return

    evidence_items = what_block.get("evidence_items", [])

    if not evidence_items:
        print(f"⏩ SKIP: Tidak ada barang bukti di file {json_file_path}.")
        return

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("SELECT id FROM public.cases WHERE case_number = %s", (case_number,))
        result = cursor.fetchone()
        
        if not result:
            print(f"⚠️ SKIP: Kasus '{case_number}' belum ada di database. Harap import kasusnya dulu.")
            return
            
        case_id = result[0]
        inserted_count = 0

        for item in evidence_items:
            item_name = item.get("description")
            quantity = item.get("quantity", 1)
            unit = item.get("unit", "pcs")
            disposition = item.get("disposition", "Tidak Diketahui")

            if not item_name or item_name == "Tidak Diketahui": 
                continue

            insert_query = """
                INSERT INTO public.case_evidence_items 
                (case_id, description, quantity, unit, disposition, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
            """
            cursor.execute(insert_query, (case_id, item_name, quantity, unit, disposition))
            inserted_count += 1
        
        conn.commit()
        print(f"✅ SUKSES: {inserted_count} barang bukti ditambahkan untuk kasus ID: {case_id}")

    except Exception as e:
        print(f"❌ Error Database: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()