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

def import_cities(json_path_file):
    try:
        with open(json_path_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Gagal memuat file: {e}")
        return

    city_prov_map = {}

    who_block = data.get("who", {})
    kota_jaringan = who_block.get("kota_jaringan")
    if kota_jaringan and kota_jaringan.lower() != "tidak diketahui":
        city_prov_map[kota_jaringan] = None 

    where_block = data.get("where", {})
    court_location = where_block.get("normalized_court_city")
    court_province = where_block.get("normalized_court_province")
    if court_location and court_location.lower() != "tidak diketahui":
        city_prov_map[court_location] = (
            court_province if court_province and court_province.lower() != "tidak diketahui"
            else None
        )

    what_block = data.get("what", {})
    activity_keys = ["perencanaan_activities", "tindakan_activities", "pelatihan_activities"]

    for act_key in activity_keys:
        activities = what_block.get(act_key, [])
        for act in activities:
            city = act.get("estimated_city")
            prov = act.get("estimated_province")
            
            if city and city.lower() != "tidak diketahui":
                current_prov = city_prov_map.get(city)
                if not current_prov and prov and prov.lower() != "tidak diketahui":
                    city_prov_map[city] = prov
                elif city not in city_prov_map:
                    city_prov_map[city] = prov if (prov and prov.lower() != "tidak diketahui") else None

    if not city_prov_map:
        print("Tidak ada data kota valid untuk diimport di file ini.")
        return

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        unique_prov_names = list({prov for prov in city_prov_map.values() if prov})
        prov_name_to_id = {}
        
        if unique_prov_names:
            cursor.execute("SELECT id, province_name FROM public.provinces WHERE province_name = ANY(%s);", (unique_prov_names,))
            prov_name_to_id = {row[1]: row[0] for row in cursor.fetchall()}

        unique_cities = list(city_prov_map.keys())
        cursor.execute("SELECT city_name FROM public.cities WHERE city_name = ANY(%s);", (unique_cities,))
        existing_cities = {row[0] for row in cursor.fetchall()}

        new_cities_to_insert = []
        for city_name, prov_name in city_prov_map.items():
            if city_name not in existing_cities:
                prov_id = prov_name_to_id.get(prov_name) if prov_name else None
                
                new_cities_to_insert.append((
                    city_name,
                    prov_id
                ))

        if new_cities_to_insert:
            insert_query = """
                INSERT INTO public.cities 
                (city_name, province_id, created_at, updated_at) 
                VALUES %s;
            """
            execute_values(
                cursor, insert_query, new_cities_to_insert,
                template="(%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            )
            print(f"Berhasil menambahkan {len(new_cities_to_insert)} kota baru ke database.")
        else:
            print("Semua kota di file ini sudah ada di database (SKIP).")

        conn.commit()

    except Exception as e:
        print(f"Error Database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()