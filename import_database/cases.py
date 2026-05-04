import json
import psycopg2
import os
from psycopg2.extras import execute_values, RealDictCursor
from import_id import get_court_id

DB_CONFIG = {
    "dbname": "terrorism_metadata",
    "user": "is02",
    "password": "open123",
    "host": "localhost",
    "port": "5433"
}

def import_cases(json_file_path):
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    what_block = data.get("what", {})
    when_block = data.get("when", {})
    how_much_block = data.get("how_much", {})
    why_block = data.get("why", {})

    case_number = what_block.get("case_number")

    if "xxx" in case_number.lower():
        nama_file = os.path.basename(json_file_path)
        case_number = f"{case_number} [Sensor: {nama_file}]"

    if not case_number or case_number in ["Tidak Diketahui", "Data Kosong"]:
        print(f"SKIP: Data Kasus tidak ada di {json_file_path}")
        return
    
    court_name = what_block.get("court_name")
    process_level = what_block.get("court_level")
    indictment_model = what_block.get("indictment_model")
    verdict_outcome = what_block.get("verdict_per_charge")
    prison_term_years = how_much_block.get("prison_term_years")
    prison_term_months = how_much_block.get("prison_term_months")
    detention_credit = what_block.get("detention_credit")
    court_date = when_block.get("district_court_date")
    appeal_timeline = when_block.get("appeal_timeline")
    has_attack_plan = what_block.get("has_attack_plan")
    attack_plan_summary = what_block.get("attack_plan_summary")
    arrest_date = when_block.get("arrest_date")
    communication = what_block.get('defendant_chat_platform')
    injury_severity = why_block.get('injury_severity')
    channel = why_block.get('radicalization_channel')


    if not case_number or "Tidak Diketahui" in case_number or "Data Kosong" in case_number:
        print(f"SKIP: Data Kasus tidak ada")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        court_id = get_court_id(cursor, court_name)
        if not court_id:
            print(f"GAGAL INSERT: Pengadilan '{court_name}' belum ada di DB untuk kasus {case_number}")
            return
        
        insert_query = """
            INSERT INTO cases(case_number, court_id, process_level, indictment_model, verdict_outcome,
            prison_term_years, prison_term_months, detention_credit, court_date, arrest_date, appeal_timeline,
            has_attack_plan, attack_plan_summary, communication, injury_severity, channel, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (case_number)
            DO UPDATE SET
                court_id = EXCLUDED.court_id,
                process_level = EXCLUDED.process_level, 
                indictment_model = EXCLUDED.indictment_model, 
                verdict_outcome = EXCLUDED.verdict_outcome,
                prison_term_years = EXCLUDED.prison_term_years, 
                prison_term_months = EXCLUDED.prison_term_months, 
                detention_credit = EXCLUDED.detention_credit, 
                court_date = EXCLUDED.court_date, 
                arrest_date = EXCLUDED.arrest_date, 
                appeal_timeline = EXCLUDED.appeal_timeline,
                has_attack_plan = EXCLUDED.has_attack_plan, 
                attack_plan_summary = EXCLUDED.attack_plan_summary,
                communication = EXCLUDED.communication,
                injury_severity = EXCLUDED.injury_severity,
                channel = EXCLUDED.channel,
                updated_at = NOW()
            RETURNING id;
        """

        # Variabelnya sekarang pas 16 buah untuk 16 buah %s
        cursor.execute(insert_query, (case_number, court_id, process_level, indictment_model, verdict_outcome,
            prison_term_years, prison_term_months, detention_credit, court_date, arrest_date, appeal_timeline,
            has_attack_plan, attack_plan_summary, communication, injury_severity, channel))
        
        case_id = cursor.fetchone()['id']
        conn.commit()

        print(f"SUKSES: {case_number} tersimpan/terupdate dengan ID: {case_id}")

    except psycopg2.Error as e:
        print(f"DATABASE ERROR: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()