import json
import psycopg2
import os
from psycopg2.extras import execute_values, RealDictCursor
from import_id import get_case_id, get_person_id

DB_CONFIG = {
    "dbname": "terrorism_metadata",
    "user": "is02",
    "password": "open123",
    "host": "localhost",
    "port": "5433"
}

def import_case_defendant_details(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    who_obj = data.get("who", {})
    when_obj = data.get("when", {})
    case_number = data.get("what", {}).get("case_number", None)

    ideology = who_obj.get("defendant_ideology_affiliation", None)
    local_network = who_obj.get("normalized_local_network", None)
    joined_at_raw = when_obj.get("normalized_joined_at", None)

    defendants = who_obj.get("defendants", [])

    if not defendants:
        print("Tidak ada data 'defendants' di file ini.")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Dapatkan ID Kasus
        id_cases = get_case_id(cursor, case_number)
        if not id_cases:
            print(f"Kasus {case_number} tidak ditemukan.")
            return

        detail_baru = 0
        detail_lama = 0

        # 4. Looping setiap terdakwa di dalam array
        for defendant in defendants:
            # Utamakan penggunaan normalized_name jika ada
            raw_name = defendant.get("normalized_name") or defendant.get("name")
            
            if not raw_name or raw_name == "Tidak Diketahui":
                continue
                
            # Dapatkan ID Orang (Pastikan master person sudah diisi duluan)
            person_id = get_person_id(cursor, raw_name.strip())
            if not person_id:
                print(f"Orang bernama '{raw_name}' belum ada di master person. Melewati...")
                continue

            age = defendant.get("normalized_age")

            # 5. Pengecekan Duplikat
            check_query = """
                SELECT id FROM public.case_defendant_details 
                WHERE case_id = %s AND defendant_person_id = %s
            """
            cursor.execute(check_query, (id_cases, person_id))
            
            if cursor.fetchone():
                detail_lama += 1
            else:
                # 6. Insert Data
                insert_query = """
                    INSERT INTO public.case_defendant_details 
                    (case_id, defendant_person_id, defendant_age, ideology_affiliation, 
                     local_network, local_network_joined_at, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                cursor.execute(insert_query, (
                    id_cases, 
                    person_id, 
                    age, 
                    ideology, 
                    local_network, 
                    joined_at_raw
                ))
                detail_baru += 1
                print(f"Detail terdakwa BARU: {raw_name} (Usia: {age}) ditambahkan ke kasus.")

        conn.commit()
        print(f"✅ Selesai: {detail_baru} Detail Baru, {detail_lama} Sudah Ada.")

    except Exception as e:
        print(f"❌ Error Database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()