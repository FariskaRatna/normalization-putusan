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

def extract_province(data):
    provinces = set()

    what = data.get("what", {})
    for key in ['perencanaan_activities', 'pelatihan_activities', 'tindakan_activities']:
        activities = what.get(key, [])
        for act in activities:
            prov = act.get("estimated_province")
            if prov and prov.lower() not in ["unknown", "null", "none", "tidak diketahui"]:
                provinces.add(prov.strip())

    who = data.get("who", {})
    network_prov = who.get("provinsi_jaringan", {})
    if network_prov and network_prov.lower() not in ["unknown", "null", "none", "tidak diketahui"]:
        provinces.add(network_prov.strip())
    
    where = data.get("where", {})
    court_province = where.get("normalized_court_province", {})
    if court_province and court_province.lower() not in ["unknown", "null", "none", "tidak diketahui"]:
        provinces.add(court_province.strip())

    return list(provinces)

def import_provinces(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    unique_provinces = extract_province(data)

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        for prov_name in unique_provinces:
            cursor.execute("SELECT id FROM public.provinces WHERE province_name = %s", (prov_name,))
            result = cursor.fetchone()

            if not result:
                cursor.execute(
                    "INSERT INTO  public.provinces (province_name, created_at, updated_at) VALUES (%s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id",
                    (prov_name,)
                )
                new_id = cursor.fetchone()[0]
                print(f"Provinsi baru ditambahkan: {prov_name} (ID: {new_id})")
            else:
                print(f"Provinsi sudah ada: {prov_name}")
        
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()