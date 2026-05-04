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

def import_locations_from_activities(json_path_file):
    try:
        with open(json_path_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Gagal memuat file: {e}")
        return

    location_to_city_map = {}
    
    what_block = data.get("what", {})
    activity_keys = ["perencanaan_activities", "tindakan_activities", "pelatihan_activities"]

    for key in activity_keys:
        activities = what_block.get(key, [])
        for act in activities:
            raw_location = act.get("location")
            city_name = act.get("estimated_city")
            
            if raw_location and raw_location.lower() != "tidak diketahui":
                location_to_city_map[raw_location] = city_name

    if not location_to_city_map:
        return

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        unique_cities = list({c for c in location_to_city_map.values() if c and c.lower() != "tidak diketahui"})
        city_name_to_id = {}
        if unique_cities:
            cursor.execute("SELECT id, city_name FROM public.cities WHERE city_name = ANY(%s);", (unique_cities,))
            city_name_to_id = {row[1]: row[0] for row in cursor.fetchall()}

        all_raw_texts = list(location_to_city_map.keys())
        cursor.execute("SELECT location_text FROM public.locations WHERE location_text = ANY(%s);", (all_raw_texts,))
        existing_locations = {row[0] for row in cursor.fetchall()}

        records_to_insert = []
        for raw_text, city_name in location_to_city_map.items():
            if raw_text not in existing_locations:
                city_id = city_name_to_id.get(city_name)
                
                records_to_insert.append((
                    raw_text,
                    city_id,
                    None, 
                    None  
                ))

        if records_to_insert:
            insert_query = """
                INSERT INTO public.locations 
                (location_text, city_id, long, lat, created_at, updated_at) 
                VALUES %s;
            """
            execute_values(
                cursor, insert_query, records_to_insert,
                template="(%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            )
            print(f"Berhasil menambahkan {len(records_to_insert)} alamat lokasi baru.")
        
        conn.commit()

    except Exception as e:
        print(f"Error Database: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()