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

def import_case_people(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    who_obj = data.get("who", {})
    case_number = data.get("what", {}).get("case_number", None)
    
    defendant = who_obj.get("defendant_name", "")
    co_defendants = who_obj.get("normalized_co_defendants", [])
    judges = who_obj.get("normalized_judges", [])
    prosecutors = who_obj.get("normalized_prosecutors", [])
    defense_counsels = who_obj.get("normalized_defense_counsels", [])
    clerks = who_obj.get("normalized_clerk", [])
    witnesses = who_obj.get("witnesses", [])
    
    people_to_process = []
    
    # Terdakwa Utama
    if defendant and defendant != "Tidak Diketahui":
        people_to_process.append({"name": defendant, "role": "defendant", "is_presiding": False, "seq": 1})
        
    # Co-Defendants
    for i, name in enumerate(co_defendants, start=2):
        if name != "Tidak Diketahui":
            people_to_process.append({"name": name, "role": "co_defendant", "is_presiding": False, "seq": i})

    # Hakim (is_presiding = True untuk urutan 1)
    for i, name in enumerate(judges, start=1):
        if name != "Tidak Diketahui":
            people_to_process.append({"name": name, "role": "judge", "is_presiding": (i == 1), "seq": i})

    # Jaksa
    for i, name in enumerate(prosecutors, start=1):
        if name != "Tidak Diketahui": # Penting agar "Tidak Diketahui" tidak masuk ke database master person
            people_to_process.append({"name": name, "role": "prosecutor", "is_presiding": False, "seq": i})

    # Pengacara / Penasihat Hukum
    for i, name in enumerate(defense_counsels, start=1):
        if name != "Tidak Diketahui":
            people_to_process.append({"name": name, "role": "defense_counsel", "is_presiding": False, "seq": i})

    # Panitera
    for i, name in enumerate(clerks, start=1):
        if name != "Tidak Diketahui":
            people_to_process.append({"name": name, "role": "clerk", "is_presiding": False, "seq": i})

    # Saksi
    for i, name in enumerate(witnesses, start=1):
        if name != "Tidak Diketahui":
            people_to_process.append({"name": name, "role": "witness", "is_presiding": False, "seq": i})

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        id_cases = get_case_id(cursor, case_number)
        if not id_cases:
            print(f"Kasus {case_number} tidak ditemukan.")
            return

        relasi_baru = 0
        relasi_lama = 0

        # 3. Looping untuk insert ke database
        for item in people_to_process:
            clean_name = item["name"].strip()
            if not clean_name:
                continue

            person_id = get_person_id(cursor, clean_name)
            if not person_id:
                print(f"Orang bernama '{clean_name}' belum ada di tabel master person. Melewati...")
                continue

            # Cek duplikasi relasi (kasus yang sama, orang yang sama, peran yang sama)
            check_query = """
                SELECT id FROM public.case_people 
                WHERE case_id = %s AND person_id = %s AND role = %s
            """
            cursor.execute(check_query, (id_cases, person_id, item["role"]))
            if cursor.fetchone():
                relasi_lama += 1
            else:
                insert_query = """
                    INSERT INTO public.case_people 
                    (case_id, person_id, role, is_presiding_judge, sequence_no, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id;
                """
                cursor.execute(insert_query, (
                    id_cases, 
                    person_id, 
                    item["role"], 
                    item["is_presiding"], 
                    item["seq"]
                ))
                relasi_baru += 1
                print(f"Relasi BARU: {clean_name} ({item['role']}) ditambahkan ke kasus.")

        conn.commit()
        print(f"✅ Selesai: {relasi_baru} Relasi Baru, {relasi_lama} Sudah Ada.")

    except Exception as e:
        print(f"❌ Error Database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()