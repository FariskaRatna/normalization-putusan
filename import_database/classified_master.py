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

def import_classified_master(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    why_block = data.get("why", {})

    factor_mappings = {
        "classified_aggravating_factors": "aggravating",
        "classified_motivation_factors": "motivation",
        "classified_radicalization_sources": "radicalization"
    }
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        total_baru = 0
        total_lama = 0

        for json_key, classified_type in factor_mappings.items():
            
            factors_list = why_block.get(json_key, [])
            
            if isinstance(factors_list, str):
                factors_list = [factors_list]

            if not factors_list:
                continue 

            for classified_name in factors_list:
                clean_name = classified_name.strip()
                if not clean_name:
                    continue

                check_query = """
                    SELECT id FROM public.classified_factors_source 
                    WHERE classified_name = %s AND classified_type = %s
                """
                cursor.execute(check_query, (clean_name, classified_type))
                result = cursor.fetchone()

                if result:
                    total_lama += 1
                else:
                    insert_query = """
                        INSERT INTO public.classified_factors_source 
                        (classified_name, classified_type, created_at, updated_at)
                        VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        RETURNING id;
                    """
                    cursor.execute(insert_query, (clean_name, classified_type))
                    new_id = cursor.fetchone()[0]
                    total_baru += 1
                    
                    print(f"✅ BARU [{classified_type.upper()}]: {clean_name} (ID: {new_id})")

        conn.commit()
        
        if total_baru > 0 or total_lama > 0:
            print(f"🎉 Selesai import {json_path}: {total_baru} Baru, {total_lama} Sudah Ada")
        else:
            print(f"⏩ SKIP: Tidak ada data classified apa pun di file {json_path}")

    except Exception as e:
        print(f"❌ Error database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()