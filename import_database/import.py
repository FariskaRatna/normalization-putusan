import json
import psycopg2

DB_CONFIG = {
    "dbname": "terrorism_metadata",
    "user": "is02",
    "password": "open123",
    "host": "localhost",
    "port": "5432"
}

def import_entities(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    related_entities = data.get("who", {}).get("related_entities", [])

    if isinstance(related_entities, str):
        related_entities = [related_entities]

    if not related_entities:
        print("Tidak ada data related_entities di JSON ini.")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        entitas_baru = 0
        entitas_lama = 0
        
        for entity_name in related_entities:
            entity_name_clean = entity_name.strip()
            if not entity_name_clean:
                continue

            cursor.execute("SELECT id FROM public.entities WHERE entity_name = %s", (entity_name_clean,))
            result = cursor.fetchone()
            
            if result:
                entity_id = result[0]
                entitas_lama += 1
            else:
                insert_query = """
                    INSERT INTO public.entities (entity_name, entity_type, created_at, updated_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id;
                """
                
                cursor.execute(insert_query, (entity_name_clean, "Tidak Diketahui"))
                
                entity_id = cursor.fetchone()[0]
                entitas_baru += 1
                print(f"  ✨ Entitas BARU ditambahkan: '{entity_name_clean}' (ID: {entity_id})")
        
        conn.commit()
        print(f"✅ Selesai! {entitas_baru} Entitas Baru, {entitas_lama} Entitas Sudah Ada.")

    except Exception as e:
        print(f"❌ Terjadi Error: {e}")
        if conn:
            conn.rollback() 
    finally:
        if conn:
            cursor.close()
            conn.close()

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
                    "INSERT INTO  public.provinces (province_name) VALUES (%s) RETURNING id",
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



def insert_item_catalog(json_file_path):
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Gagal membaca file: {e}")
        return

    evidence_items = data.get("what", {}).get("evidence_items", [])
    if not evidence_items:
        print("Tidak ada barang bukti untuk diimport di file ini.")
        return

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for item in evidence_items:
            item_name = item.get("normalized_name")
            item_category = item.get("item_category", "other")
            default_unit = item.get("unit", "pcs")

            if not item_name or item_name == "Tidak Diketahui": 
                continue

            check_query = "SELECT id FROM public.item_catalog WHERE item_name = %s"
            cursor.execute(check_query, (item_name,))
            result = cursor.fetchone()

            if result:
                catalog_id = result[0]
                print(f"SKIP: '{item_name}' sudah ada di katalog (ID: {catalog_id}).")
            else:
                insert_query = """
                    INSERT INTO public.item_catalog 
                    (item_category, item_name, default_unit, created_at, updated_at)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id;
                """
                cursor.execute(insert_query, (item_category, item_name, default_unit))
                catalog_id = cursor.fetchone()[0]
                print(f"BARU: '{item_name}' ditambahkan ke katalog (ID: {catalog_id}).")
        
        conn.commit()

    except Exception as e:
        print(f"Error Database: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

insert_item_catalog("19_PID~SUS_2022_PN JKT~TIM.json")