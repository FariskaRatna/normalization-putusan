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
                print(f"Entitas BARU ditambahkan: '{entity_name_clean}' (ID: {entity_id})")
        
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

def import_article_charges(json_file_path):
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Gagal membaca file: {e}")
        return
    
    charges = data.get("what", {}).get("normalized_articles", [])
    if not charges:
        print("Tidak ada pasal yang diimport di file ini")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for charge in charges:
            cursor.execute("SELECT id FROM public.legal_articles WHERE article_text = %s", (charge,))
            result = cursor.fetchone()

            if not result:
                cursor.execute(
                    "INSERT INTO public.legal_articles (article_text) VALUES (%s) RETURNING id",
                    (charge,)
                )

                new_id = cursor.fetchone()[0]
                print(f"Pasal baru ditambahkan: {charge} (ID: {new_id})")
            else:
                print(f"Pasal sudah ada: {charge}")
            
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def import_case_evidence(json_file_path):
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Gagal membaca file: {e}")
        return

    what_block = data.get("what", {})
    
    case_number = str(what_block.get("case_number", "")).strip()
    
    # Terapkan logika sensor 'xxx' yang sama agar cocok dengan database
    if "xxx" in case_number.lower():
        nama_file = os.path.basename(json_file_path)
        case_number = f"{case_number} [Sensor: {nama_file}]"

    if not case_number or case_number in ["Tidak Diketahui", "Data Kosong"]:
        print(f"⏩ SKIP: Nomor kasus tidak ada di {json_file_path}")
        return

    evidence_items = what_block.get("evidence_items", [])

    if not evidence_items:
        print(f"⏩ SKIP: Tidak ada barang bukti di file {json_file_path}.")
        return

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("SELECT id FROM public.cases WHERE case_number = %s", (case_number,))
        result = cursor.fetchone()
        
        if not result:
            print(f"⚠️ SKIP: Kasus '{case_number}' belum ada di database. Harap import kasusnya dulu.")
            return
            
        case_id = result[0]
        inserted_count = 0

        for item in evidence_items:
            item_name = item.get("description")
            quantity = item.get("quantity", 1)
            unit = item.get("unit", "pcs")
            disposition = item.get("disposition", "Tidak Diketahui")

            if not item_name or item_name == "Tidak Diketahui": 
                continue

            insert_query = """
                INSERT INTO public.case_evidence_items 
                (case_id, description, quantity, unit, disposition, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
            """
            cursor.execute(insert_query, (case_id, item_name, quantity, unit, disposition))
            inserted_count += 1
        
        conn.commit()
        print(f"✅ SUKSES: {inserted_count} barang bukti ditambahkan untuk kasus ID: {case_id}")

    except Exception as e:
        print(f"❌ Error Database: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def import_persons(json_file_path):
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Gagal membaca file: {e}")
        return
    
    defendants = data.get("who", {}).get("defendants", [])
    if not defendants:
        print("Tidak ada terdakwa untuk diimport di file ini")
        return
    
    incoming_names = set()
    records_dict = {} 
    
    for person in defendants:
        name = person.get("normalized_name")
        if not name or name == "Tidak Diketahui":
            continue
            
        incoming_names.add(name)
        records_dict[name] = (
            name, person.get("gender"), person.get("nationality"), 
            person.get("religion"), person.get("occupation"),
            person.get("address"), person.get("pob"), 
            person.get("dob"), person.get("education_status")
        )

    if not incoming_names:
        print("Tidak ada data valid untuk diproses.")
        return

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        check_query = "SELECT fullname FROM public.persons WHERE fullname = ANY(%s);"
        cursor.execute(check_query, (list(incoming_names),))
        
        existing_names = {row[0] for row in cursor.fetchall()}

        records_to_insert = [
            record for name, record in records_dict.items() 
            if name not in existing_names
        ]

        if records_to_insert:
            insert_query = """
                INSERT INTO public.persons
                (fullname, gender, nationality, religion, occupation,
                address, place_of_birth, date_of_birth, education_status,
                created_at, updated_at) 
                VALUES %s;
            """
            execute_values(
                cursor, 
                insert_query, 
                records_to_insert,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            )
            print(f"Berhasil menambahkan {len(records_to_insert)} data terdakwa baru.")
        else:
            print("Semua terdakwa di file ini sudah ada di database (SKIP).")

        conn.commit()
    
    except Exception as e:
        print(f"Error Database: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def import_co_defendants(json_path_file):
    try:
        with open(json_path_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Gagal memuat file: {e}")
        return
    
    co_defendants = data.get("who", {}).get("normalized_co_defendants", [])
    
    if not co_defendants or co_defendants == ["Tidak Diketahui"]:
        print("Tidak ada co-defendants valid di file ini.")
        return
    
    incoming_names = set()
    for name in co_defendants:
        if name and name != "Tidak Diketahui":
            incoming_names.add(name)

    if not incoming_names:
        return

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        check_query = "SELECT fullname FROM public.persons WHERE fullname = ANY(%s);"
        cursor.execute(check_query, (list(incoming_names),))
        
        existing_names = {row[0] for row in cursor.fetchall()}

        records_to_insert = []
        for name in incoming_names:
            if name not in existing_names:
                records_to_insert.append((
                    name, "Tidak Diketahui", "Tidak Diketahui", "Tidak Diketahui", 
                    "Tidak Diketahui", "Tidak Diketahui", "Tidak Diketahui", 
                    None, "Tidak Diketahui"
                ))

        if records_to_insert:
            insert_query = """
                INSERT INTO public.persons 
                (fullname, gender, nationality, religion, occupation, 
                 address, place_of_birth, date_of_birth, education_status, 
                 created_at, updated_at) 
                VALUES %s;
            """
            execute_values(
                cursor, 
                insert_query, 
                records_to_insert,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            )
            print(f"Berhasil menambahkan {len(records_to_insert)} co-defendants baru.")
        else:
            print("Semua co-defendants di file ini sudah ada di database (SKIP).")
        
        conn.commit()
        
    except Exception as e:
        print(f"Error Database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def import_officials_to_persons(json_path_file):
    try:
        with open(json_path_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Gagal memuat file: {e}")
        return
    
    who_block = data.get("who", {})
    
    official_keys = [
        "normalized_defense_counsels",
        "normalized_judges",
        "normalized_clerk",
        "normalized_prosecutors"
    ]

    unique_names = set() 
    
    for key in official_keys:
        names_list = who_block.get(key, [])
        for name in names_list:
            if name and name.lower() != "tidak diketahui":
                unique_names.add(name)

    if not unique_names:
        print("Tidak ada pejabat pengadilan valid untuk diimport di file ini.")
        return

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        check_query = "SELECT fullname FROM public.persons WHERE fullname = ANY(%s);"
        cursor.execute(check_query, (list(unique_names),))
        
        existing_names = {row[0] for row in cursor.fetchall()}

        new_persons_to_insert = []
        for name in unique_names:
            if name not in existing_names:
                new_persons_to_insert.append((
                    name, "Tidak Diketahui", "Tidak Diketahui", "Tidak Diketahui", 
                    "Tidak Diketahui", "Tidak Diketahui", "Tidak Diketahui", 
                    None, "Tidak Diketahui"
                ))

        if new_persons_to_insert:
            insert_persons_query = """
                INSERT INTO public.persons 
                (fullname, gender, nationality, religion, occupation, 
                 address, place_of_birth, date_of_birth, education_status, 
                 created_at, updated_at) 
                VALUES %s;
            """
            execute_values(
                cursor, insert_persons_query, new_persons_to_insert,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            )
            print(f"Berhasil menambahkan {len(new_persons_to_insert)} pejabat pengadilan baru ke tabel persons.")
        else:
            print("Semua pejabat pengadilan di file ini sudah ada di database (SKIP).")

        conn.commit()
        
    except Exception as e:
        print(f"Error Database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def import_person_documents(json_file_path):
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Gagal membaca file: {e}")
        return

    defendants = data.get("who", {}).get("defendants", [])
    
    doc_mapping = {
        "nik": "NIK",
        "passport_no": "Passport",
        "kk_no": "KK"
    }

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        for person in defendants:
            name = person.get("normalized_name")
            if not name or name == "Tidak Diketahui":
                continue
                
            cursor.execute("SELECT id FROM public.persons WHERE fullname = %s", (name,))
            res = cursor.fetchone()
            if not res:
                continue
            
            person_id = res[0]
            
            doc_records = []
            for json_key, doc_label in doc_mapping.items():
                doc_number = person.get(json_key)
                
                if doc_number and str(doc_number).lower() != "tidak diketahui":
                    doc_records.append((
                        person_id, 
                        doc_label, 
                        doc_number, 
                        None # issuing_region
                    ))
            
            if doc_records:
                insert_query = """
                    INSERT INTO public.person_documents 
                    (person_id, document_type, document_number, issuing_region, created_at, updated_at)
                    VALUES %s
                    ON CONFLICT DO NOTHING;
                """
                execute_values(
                    cursor, insert_query, doc_records,
                    template="(%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                )
        
        conn.commit()
        print("Berhasil memproses dokumen terdakwa.")
        
    except Exception as e:
        print(f"Error Database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

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


def get_city_id(cursor, city_name):
    if not city_name or city_name == "Tidak Diketahui":
        return None
    
    query = "SELECT id FROM cities WHERE city_name = %s LIMIT 1"
    cursor.execute(query, (city_name,))
    result = cursor.fetchone()

    return result['id'] if result else None

def import_court(json_file_path):
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    where_block = data.get("where", {})

    court_name = where_block.get("district_court")
    court_code = where_block.get("normalized_court_code")
    city_name = where_block.get("normalized_court_city")

    if not court_name or "Tidak Diketahui" in court_name or "Data Kosong" in court_name:
        print(f"SKIP: Data pengadilan tidak ada")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # 2. Cari City ID (Foreign Key)
        city_id = get_city_id(cursor, city_name)

        # 3. Lakukan UPSERT ke tabel courts
        insert_query = """
            INSERT INTO courts (court_name, court_code, court_city_id, created_at, updated_at)
            VALUES (%s, %s, %s, NOW(), NOW())
            ON CONFLICT (court_name) 
            DO UPDATE SET 
                court_code = EXCLUDED.court_code,
                court_city_id = EXCLUDED.court_city_id,
                updated_at = NOW()
            RETURNING id;
        """
        cursor.execute(insert_query, (court_name, court_code, city_id))
        court_id = cursor.fetchone()['id']
        conn.commit()
        
        print(f"SUKSES: [{court_name}] tersimpan/terupdate dengan ID: {court_id}")

    except psycopg2.Error as e:
        print(f"DATABASE ERROR: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()


def get_court_id(cursor, court_name):
    if not court_name or court_name == "Tidak Diketahui":
        return None
    
    query = "SELECT id FROM courts WHERE court_name = %s LIMIT 1"
    cursor.execute(query, (court_name,))
    result = cursor.fetchone()

    return result['id'] if result else None

def import_cases(json_file_path):
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    what_block = data.get("what", {})
    when_block = data.get("when", {})
    how_much_block = data.get("how_much", {})
    why_block = data.get("why", {})

    case_number = what_block.get("case_number")

    if "xxx" in case_number.lower():
        nama_file = os.path.basename(json_file_path) # Ambil nama file-nya saja
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
            has_attack_plan, attack_plan_summary, communication, injury_severity, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
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
                communication = EXCLUDE.communication
                injury_severity = EXCLUDE.injury_severity
                channel = EXCLUDE.radicalization_channel
                updated_at = NOW()
            RETURNING id;
        """

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

def get_case_id(cursor, case):
    if not case or case == "Tidak Diketahui":
        return None
    
    query = "SELECT id FROM cases WHERE case_number = %s LIMIT 1"
    cursor.execute(query, (case,))
    result = cursor.fetchone()

    return result[0] if result else None

def get_classified_id(cursor, classified):
    if not classified:
        return []
    
    if isinstance(classified, str):
        classified = [classified]

    ids = []

    for cls in classified:
        cursor.execute("SELECT id FROM classified_factors_source WHERE LOWER(TRIM(classified_name)) = LOWER(trim(%s)) LIMIT 1", (cls,))
        result = cursor.fetchone()
        if result:
            ids.append(result[0])
        else:
            print(f"TIDAK DITEMUKAN: {cls}")

        return ids

def import_aggravating_factors(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    aggravatings = data.get("why", {}).get("aggravating_factors", [])
    classifieds = data.get("why", {}).get("classified_aggravating_factors", [])
    case_number = data.get("what", {}).get("case_number", "")

    # Ubah ke list jika ternyata bentuknya cuma string tunggal
    if isinstance(aggravatings, str): aggravatings = [aggravatings]
    if isinstance(classifieds, str): classifieds = [classifieds]

    if not aggravatings or not classifieds:
        print(f"⏩ SKIP: Data aggravating / classified kosong di file {json_path}")
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

        for desc, classified_name in zip(aggravatings, classifieds):
            clean_desc = desc.strip()
            clean_class = classified_name.strip()

            if not clean_desc or not clean_class:
                continue

            cursor.execute("""
                SELECT id FROM public.classified_factors_source 
                WHERE classified_name = %s AND classified_type = 'aggravating'
            """, (clean_class,))
            res_class = cursor.fetchone()
            
            if not res_class:
                print(f"⚠️ GAGAL: Klasifikasi '{clean_class}' belum terdaftar di tabel master.")
                continue
            id_classified = res_class[0]

            cursor.execute("""
                SELECT id FROM public.aggravating_factors 
                WHERE id_cases = %s AND id_classified = %s AND description = %s
            """, (id_cases, id_classified, clean_desc))
            
            if cursor.fetchone():
                motivation_lama += 1
            else:
                insert_query = """
                    INSERT INTO public.aggravating_factors 
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

def import_radicalization_source(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    radicalizations = data.get("why", {}).get("radicalization_sources", [])
    classifieds = data.get("why", {}).get("classified_radicalization_sources", [])
    case_number = data.get("what", {}).get("case_number", "")

    # Ubah ke list jika ternyata bentuknya cuma string tunggal
    if isinstance(radicalizations, str): radicalizations = [radicalizations]
    if isinstance(classifieds, str): classifieds = [classifieds]

    if not radicalizations or not classifieds:
        print(f"⏩ SKIP: Data radicalization / classified kosong di file {json_path}")
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

        for desc, classified_name in zip(radicalizations, classifieds):
            clean_desc = desc.strip()
            clean_class = classified_name.strip()

            if not clean_desc or not clean_class:
                continue

            cursor.execute("""
                SELECT id FROM public.classified_factors_source 
                WHERE classified_name = %s AND classified_type = 'radicalization'
            """, (clean_class,))
            res_class = cursor.fetchone()
            
            if not res_class:
                print(f"⚠️ GAGAL: Klasifikasi '{clean_class}' belum terdaftar di tabel master.")
                continue
            id_classified = res_class[0]

            cursor.execute("""
                SELECT id FROM public.radicalization_sources 
                WHERE id_cases = %s AND id_classified = %s AND description = %s
            """, (id_cases, id_classified, clean_desc))
            
            if cursor.fetchone():
                motivation_lama += 1
            else:
                insert_query = """
                    INSERT INTO public.radicalization_sources 
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

def import_mitigating_factors(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    mitigating_factors = data.get("why", {}).get("mitigating_factors", [])
    case_number = data.get("what", {}).get("case_number", None)


    if isinstance(mitigating_factors, str):
        mitigating_factors = [mitigating_factors]

    if not mitigating_factors:
        print("Tidak ada mitigating factors di file")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        mitigating_baru = 0
        mitigating_lama = 0

        id_cases = get_case_id(cursor, case_number)
        
        for description in mitigating_factors:
            clean_description = description.strip()
            if not clean_description:
                continue

            cursor.execute("SELECT id FROM public.mitigating_factors WHERE description = %s", (clean_description,))
            result = cursor.fetchone()

            if result:
                mitigating_id = result[0]
                mitigating_lama += 1
            else: 
                insert_query = """
                    INSERT INTO public.mitigating_factors (id_cases, description, created_at, updated_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id;
                """

                cursor.execute(insert_query, (id_cases, clean_description,))

                mitigating_id = cursor.fetchone()[0]
                mitigating_baru += 1
                print(f"Mitigating factor BARU ditambahkan: {clean_description} (ID: {mitigating_id})")
                
        conn.commit()
        print(f"Selesai: {mitigating_baru} Baru, {mitigating_lama} Sudah Ada")

    except Exception as e:
        print(f"Error database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def import_detention_timeline(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    detention_timeline = data.get("when", {}).get("detention_timeline", [])
    case_number = data.get("what", {}).get("case_number", None)

    if not detention_timeline:
        print("Tidak ada detention timeline di file")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        detention_baru = 0
        detention_lama = 0

        id_cases = get_case_id(cursor, case_number)
        
        for item in detention_timeline:
            if not isinstance(item, dict):
                continue
                
            description = item.get("description", "").strip()
            date_str = item.get("date", "").strip()
            start_date = item.get("start_date")
            end_date = item.get("end_date")

            if not description and not date_str:
                continue

            check_query = """
                SELECT id FROM public.detention_timeline 
                WHERE id_cases = %s AND description = %s AND detention_date = %s
            """
            cursor.execute(check_query, (id_cases, description, date_str))
            result = cursor.fetchone()

            if result:
                detention_id = result[0]
                detention_lama += 1
            else: 
                insert_query = """
                    INSERT INTO public.detention_timeline 
                    (id_cases, description, detention_date, start_date, end_date, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id;
                """
                
                cursor.execute(insert_query, (id_cases, description, date_str, start_date, end_date))

                detention_id = cursor.fetchone()[0]
                detention_baru += 1
                print(f"Detention timeline BARU ditambahkan: {description} ({date_str}) (ID: {detention_id})")
                
        conn.commit()
        print(f"Selesai: {detention_baru} Baru, {detention_lama} Sudah Ada")

    except Exception as e:
        print(f"Error database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def get_legal_id(cursor, article_text):
    if not article_text or article_text == "Tidak Diketahui":
        return None
    
    query = "SELECT id FROM legal_articles WHERE article_text = %s LIMIT 1"
    cursor.execute(query, (article_text,))
    result = cursor.fetchone()

    return result[0] if result else None

def import_case_charge_articles(json_file_path):
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    case_number = data.get("what", {}).get("case_number", None)
    articles = data.get("what", {}).get("normalized_articles", [])
    
    if isinstance(articles, str):
        articles = [articles]

    if not articles:
        print("Tidak ada data normalized_articles di file ini.")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        article_baru = 0
        article_lama = 0

        # Dapatkan ID kasus
        id_cases = get_case_id(cursor, case_number)
        if not id_cases:
            print(f"Kasus {case_number} tidak ditemukan di tabel cases.")
            return

        for article_text in articles:
            clean_article = article_text.strip()
            if not clean_article:
                continue
                
            id_article = get_legal_id(cursor, clean_article)
            
            if not id_article:
                print(f"Pasal '{clean_article}' tidak ditemukan di database master. Melewati...")
                continue

            cursor.execute(
                "SELECT id FROM public.case_charged_articles WHERE case_id = %s AND article_id = %s",
                (id_cases, id_article)
            )
            result = cursor.fetchone()

            if result:
                article_lama += 1
            else:
                insert_query = """
                    INSERT INTO public.case_charged_articles (case_id, article_id, created_at, updated_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id;
                """
                cursor.execute(insert_query, (id_cases, id_article))
                new_id = cursor.fetchone()[0]
                print(f"Relasi BARU ditambahkan: {clean_article} (ID Relasi: {new_id})")
                
                article_baru += 1

        conn.commit()
        print(f"Selesai: {article_baru} Relasi Baru, {article_lama} Relasi Sudah Ada")

    except Exception as e:
        print(f"Error database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def import_monetary_amounts(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    what_obj = data.get("what", {})
    case_number = what_obj.get("case_number", None)

    monetary_fields = {
        "monetary_penalties": "normalized_monetary_penalties",
        "seized_money_amount": "normalized_seized_money_amount"
    }

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        id_cases = get_case_id(cursor, case_number)
        if not id_cases:
            print(f"Kasus {case_number} tidak ditemukan.")
            return

        baru = 0
        lama = 0

        for field_key, amount_type in monetary_fields.items():
            raw_value = what_obj.get(field_key)
            amount = raw_value

            if amount is None:
                continue

            # 1. Cek apakah sudah ada nominal dengan tipe yang sama untuk kasus ini
            check_query = """
                SELECT id FROM public.case_monetary_amounts 
                WHERE case_id = %s AND amount_type = %s
            """
            cursor.execute(check_query, (id_cases, amount_type))
            result = cursor.fetchone()

            if result:
                lama += 1
            else:
                # 2. Insert data baru
                insert_query = """
                    INSERT INTO public.case_monetary_amounts 
                    (case_id, amount_type, amount, currency, notes, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                
                notes = f"Hasil ekstraksi dari field {field_key}"
                cursor.execute(insert_query, (id_cases, amount_type, amount, "IDR", notes))
                baru += 1
                print(f"Nominal BARU ({amount_type}): {amount} ditambahkan ke kasus {case_number}")

        conn.commit()
        print(f"✅ Selesai: {baru} Data Baru, {lama} Sudah Ada.")

    except Exception as e:
        print(f"❌ Error Database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def get_person_id(cursor, person_name):
    """Mencari ID orang dari tabel master berdasarkan nama."""
    if not person_name:
        return None
    cursor.execute("SELECT id FROM public.persons WHERE fullname = %s LIMIT 1", (person_name.strip(),))
    result = cursor.fetchone()
    return result[0] if result else None

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

def import_case_defendant_details(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    who_obj = data.get("who", {})
    when_obj = data.get("when", {})
    case_number = data.get("what", {}).get("case_number", None)

    ideology = who_obj.get("defendant_ideology_affiliation", None)
    local_network = who_obj.get("normalized_local_network", None)
    joined_at_raw = when_obj.get("normalized_joined_at", None)

    defendants = who_obj.get("defendants", [])

    if not defendants:
        print("Tidak ada data 'defendants' di file ini.")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Dapatkan ID Kasus
        id_cases = get_case_id(cursor, case_number)
        if not id_cases:
            print(f"Kasus {case_number} tidak ditemukan.")
            return

        detail_baru = 0
        detail_lama = 0

        # 4. Looping setiap terdakwa di dalam array
        for defendant in defendants:
            # Utamakan penggunaan normalized_name jika ada
            raw_name = defendant.get("normalized_name") or defendant.get("name")
            
            if not raw_name or raw_name == "Tidak Diketahui":
                continue
                
            # Dapatkan ID Orang (Pastikan master person sudah diisi duluan)
            person_id = get_person_id(cursor, raw_name.strip())
            if not person_id:
                print(f"Orang bernama '{raw_name}' belum ada di master person. Melewati...")
                continue

            age = defendant.get("normalized_age")

            # 5. Pengecekan Duplikat
            check_query = """
                SELECT id FROM public.case_defendant_details 
                WHERE case_id = %s AND defendant_person_id = %s
            """
            cursor.execute(check_query, (id_cases, person_id))
            
            if cursor.fetchone():
                detail_lama += 1
            else:
                # 6. Insert Data
                insert_query = """
                    INSERT INTO public.case_defendant_details 
                    (case_id, defendant_person_id, defendant_age, ideology_affiliation, 
                     local_network, local_network_joined_at, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                cursor.execute(insert_query, (
                    id_cases, 
                    person_id, 
                    age, 
                    ideology, 
                    local_network, 
                    joined_at_raw
                ))
                detail_baru += 1
                print(f"Detail terdakwa BARU: {raw_name} (Usia: {age}) ditambahkan ke kasus.")

        conn.commit()
        print(f"✅ Selesai: {detail_baru} Detail Baru, {detail_lama} Sudah Ada.")

    except Exception as e:
        print(f"❌ Error Database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

import_motivation_factors("19_PID~SUS_2022_PN JKT~TIM.json")