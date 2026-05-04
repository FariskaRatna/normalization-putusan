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

def import_case_entities(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    who_block = data.get("who", {})
    what_block = data.get("what", {})
    case_number = what_block.get("case_number", "")

    related_entities = who_block.get("related_entities", [])
    investigators = who_block.get("investigators", [])
    ideology = who_block.get("defendant_ideology_affiliation", [])

    if isinstance(related_entities, str): related_entities = [related_entities]
    if isinstance(investigators, str): investigators = [investigators]
    if isinstance(ideology, str): ideology = [ideology]

    all_entities = []
    for item in related_entities:
        all_entities.append((item, "Organization", "affiliation"))
        
    for item in investigators:
        all_entities.append((item, "Investigator", "handled_by"))
        
    for item in ideology:
        all_entities.append((item, "Ideology/Affiliation", "ideology_source"))

    if not all_entities:
        print(f"⏩ SKIP: Tidak ada entitas di {json_path}.")
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

        relasi_baru = 0

        for entity_name, entity_type, relation_type in all_entities:
            entity_name_clean = str(entity_name).strip()
            if not entity_name_clean or entity_name_clean.lower() in ["none", "tidak diketahui", "null", ""]:
                continue

            cursor.execute("""
                SELECT id FROM public.entities 
                WHERE entity_name = %s AND entity_type = %s
            """, (entity_name_clean, entity_type))
            res_entity = cursor.fetchone()
            
            if res_entity:
                entity_id = res_entity[0]
            else:
                cursor.execute("""
                    INSERT INTO public.entities (entity_name, entity_type, created_at, updated_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id;
                """, (entity_name_clean, entity_type))
                entity_id = cursor.fetchone()[0]

            cursor.execute("""
                SELECT id FROM public.case_entities 
                WHERE case_id = %s AND entity_id = %s AND relation_type = %s
            """, (case_id, entity_id, relation_type))
            
            if not cursor.fetchone():
                insert_rel_query = """
                    INSERT INTO public.case_entities 
                    (case_id, entity_id, relation_type, created_at, updated_at)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
                """
                cursor.execute(insert_rel_query, (case_id, entity_id, relation_type))
                relasi_baru += 1
                print(f"✅ RELASI BARU: Kasus -> {relation_type} -> '{entity_name_clean}'")

        conn.commit()
        print(f"🎉 Selesai {json_path}: {relasi_baru} relasi ditambahkan ke kasus.")

    except Exception as e:
        print(f"❌ Error database: {e}")
        if conn: conn.rollback() 
    finally:
        if conn:
            cursor.close()
            conn.close()