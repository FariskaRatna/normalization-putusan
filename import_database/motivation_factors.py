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

def import_motivation_factors(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    motivations = data.get("why", {}).get("motivation_factors", [])
    classifieds = data.get("why", {}).get("classified_motivation_factors", [])
    case_number = data.get("what", {}).get("case_number", "")

    # Ubah ke list jika ternyata bentuknya cuma string tunggal
    if isinstance(motivations, str): motivations = [motivations]
    if isinstance(classifieds, str): classifieds = [classifieds]

    if not motivations or not classifieds:
        print(f"⏩ SKIP: Data motivation / classified kosong di file {json_path}")
        return

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        motivation_baru = 0
        motivation_lama = 0

        if "xxx" in case_number.lower():
            import os
            nama_file = os.path.basename(json_path)
            case_number = f"{case_number} [Sensor: {nama_file}]"

        cursor.execute("SELECT id FROM public.cases WHERE case_number = %s", (case_number,))
        res_case = cursor.fetchone()
        if not res_case:
            print(f"⚠️ SKIP: Kasus {case_number} belum ada di tabel cases.")
            return
        id_cases = res_case[0]

        for desc, classified_name in zip(motivations, classifieds):
            clean_desc = desc.strip()
            clean_class = classified_name.strip()

            if not clean_desc or not clean_class:
                continue

            cursor.execute("""
                SELECT id FROM public.classified_factors_source 
                WHERE classified_name = %s AND classified_type = 'motivation'
            """, (clean_class,))
            res_class = cursor.fetchone()
            
            if not res_class:
                print(f"⚠️ GAGAL: Klasifikasi '{clean_class}' belum terdaftar di tabel master.")
                continue
            id_classified = res_class[0]

            cursor.execute("""
                SELECT id FROM public.motivation_factors 
                WHERE id_cases = %s AND id_classified = %s AND description = %s
            """, (id_cases, id_classified, clean_desc))
            
            if cursor.fetchone():
                motivation_lama += 1
            else:
                insert_query = """
                    INSERT INTO public.motivation_factors 
                    (id_cases, id_classified, description, created_at, updated_at)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id;
                """
                cursor.execute(insert_query, (id_cases, id_classified, clean_desc))
                motivation_id = cursor.fetchone()[0]
                motivation_baru += 1
                print(f"✅ BARU: [{clean_class}] -> {clean_desc[:30]}... (ID: {motivation_id})")

        conn.commit()
        print(f"🎉 Selesai {json_path}: {motivation_baru} Baru, {motivation_lama} Sudah Ada")

    except Exception as e:
        print(f"❌ Error database: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()