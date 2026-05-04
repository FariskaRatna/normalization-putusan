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

def import_case_activities(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    target_block = data.get("what", {}) 
    case_number = data.get("what", {}).get("case_number", "")

    activity_keys = ["pelatihan_activities", "perencanaan_activities", "tindakan_activities"]

    has_activities = any(target_block.get(k) for k in activity_keys)
    if not has_activities:
        print(f"⏩ SKIP: Tidak ada data aktivitas di {json_path}")
        return

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        if "xxx" in case_number.lower():
            nama_file = os.path.basename(json_path)
            case_number = f"{case_number} [Sensor: {nama_file}]"

        cursor.execute("SELECT id FROM public.cases WHERE case_number = %s", (case_number,))
        res_case = cursor.fetchone()
        if not res_case:
            print(f"⚠️ SKIP: Kasus {case_number} belum ada di tabel cases.")
            return
        case_id = res_case[0]

        def get_province_id(prov_name):
            if not prov_name: return None
            cursor.execute("SELECT id FROM public.provinces WHERE province_name ILIKE %s LIMIT 1", (f"%{prov_name}%",))
            res = cursor.fetchone()
            return res[0] if res else None

        def get_city_id(city_name):
            if not city_name: return None
            cursor.execute("SELECT id FROM public.cities WHERE city_name ILIKE %s LIMIT 1", (f"%{city_name}%",))
            res = cursor.fetchone()
            return res[0] if res else None

        total_inserted = 0

        for key in activity_keys:
            activities_list = target_block.get(key, [])
            
            activity_type = key.replace("_activities", "") 
            
            for act in activities_list:
                # Ekstrak data teks
                time_str = act.get("time")
                location_str = act.get("location")
                desc_str = act.get("description")
                loc_source = act.get("location_source")
                
                est_year = act.get("estimated_year")
                est_month = act.get("estimated_month")
                
                prov_id = get_province_id(act.get("estimated_province"))
                city_id = get_city_id(act.get("estimated_city"))

                insert_query = """
                    INSERT INTO public.case_activities 
                    (case_id, activity_type, activity_time, location, description, 
                     location_source, estimated_year, estimated_month, 
                     estimated_province_id, estimated_city_id, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                cursor.execute(insert_query, (
                    case_id, activity_type, time_str, location_str, desc_str,
                    loc_source, est_year, est_month, prov_id, city_id
                ))
                total_inserted += 1

        conn.commit()
        print(f"SUKSES: {total_inserted} aktivitas berhasil dimasukkan untuk kasus ID: {case_id}")

    except Exception as e:
        print(f"Error Database: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()