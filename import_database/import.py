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

def insert_article_charges(json_file_path):
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


def insert_persons(json_file_path):
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

def insert_co_defendants(json_path_file):
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

def insert_person_documents(json_file_path):
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

def import_channel_radicalization(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    channel = data.get("why", {}).get("radicalization_channel", [])
    if isinstance(channel, str):
        channel = [channel]

    if not channel:
        print("Tidak ada channel radicalization di file ini")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        channel_baru = 0
        channel_lama = 0

        for channel_name in channel:
            clean_channel_name = channel_name.strip()
            if not clean_channel_name:
                continue

            cursor.execute("SELECT id FROM public.radicalization_channel WHERE channel_name = %s", (clean_channel_name,))
            result = cursor.fetchone()

            if result:
                channel_id = result[0]
                channel_lama += 1
            else:
                insert_query = """
                    INSERT INTO public.radicalization_channel (channel_name, created_at, updated_at)
                    VALUES (%s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id;
                """

                cursor.execute(insert_query, (clean_channel_name,))

                channel_id = cursor.fetchone()[0]
                channel_baru += 1
                print(f"Classified Motivation BARU ditambahkan: {clean_channel_name} (ID: {channel_id})")

        conn.commit()
        print(f"Selesai, {channel_baru} Baru, {channel_lama} Sudah Ada")

    except Exception as e:
        print(f"Error database: {e}")
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

def get_channel_id(cursor, channel_name):
    if not channel_name or channel_name == "Tidak Diketahui":
        return None
    
    query = "SELECT id FROM radicalization_channel WHERE channel_name = %s LIMIT 1"
    cursor.execute(query, (channel_name,))
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
    channel_name = why_block.get("radicalization_channel")
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
        
        channel_id = get_channel_id(cursor, channel_name)
        if not channel_id:
            print(f"GAGAL INSERT: Pengadilan '{channel_name}' belum ada di DB untuk kasus {case_number}")
            return
        

        insert_query = """
            INSERT INTO cases(case_number, court_id, id_channel, process_level, indictment_model, verdict_outcome,
            prison_term_years, prison_term_months, detention_credit, court_date, arrest_date, appeal_timeline,
            has_attack_plan, attack_plan_summary, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (case_number)
            DO UPDATE SET
                court_id = EXCLUDED.court_id, 
                id_channel = EXCLUDED.id_channel,
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
                updated_at = NOW()
            RETURNING id;
        """

        cursor.execute(insert_query, (case_number, court_id, channel_id, process_level, indictment_model, verdict_outcome,
            prison_term_years, prison_term_months, detention_credit, court_date, arrest_date, appeal_timeline,
            has_attack_plan, attack_plan_summary))
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

def import_classified_aggravating(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    classified_aggravating = data.get("why", {}).get("classified_aggravating_factors", [])
    if isinstance(classified_aggravating, str):
        classified_aggravating = [classified_aggravating]

    if not classified_aggravating:
        print("Tidak ada classified_aggravating di file")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        aggravating_baru = 0
        aggravating_lama = 0

        for classified_name in classified_aggravating:
            clean_aggravating_name = classified_name.strip()
            if not clean_aggravating_name:
                continue

            cursor.execute("SELECT id FROM public.classified_aggravating WHERE classified_name = %s", (clean_aggravating_name,))
            result = cursor.fetchone()

            if result:
                aggravating_id = result[0]
                aggravating_lama += 1
            else:
                insert_query = """
                    INSERT INTO public.classified_aggravating (classified_name, created_at, updated_at)
                    VALUES (%s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id;
                """

                cursor.execute(insert_query, (clean_aggravating_name,))

                aggravating_id = cursor.fetchone()[0]
                aggravating_baru += 1
                print(f"Classified Aggravating BARU ditambahkan: {clean_aggravating_name} (ID: {aggravating_id})")

        conn.commit()
        print(f"Selesai, {aggravating_baru} Baru, {aggravating_lama} Sudah Ada")

    except Exception as e:
        print(f"Error database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def import_classified_motivation(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    classified_motivation = data.get("why", {}).get("classified_motivation_factors", [])
    if isinstance(classified_motivation, str):
        classified_motivation = [classified_motivation]

    if not classified_motivation:
        print("Tidak ada classified_motivation di file")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        motivation_baru = 0
        motivation_lama = 0

        for classified_name in classified_motivation:
            clean_motivation_name = classified_name.strip()
            if not clean_motivation_name:
                continue

            cursor.execute("SELECT id FROM public.classified_motivation WHERE classified_name = %s", (clean_motivation_name,))
            result = cursor.fetchone()

            if result:
                motivation_id = result[0]
                motivation_lama += 1
            else:
                insert_query = """
                    INSERT INTO public.classified_motivation (classified_name, created_at, updated_at)
                    VALUES (%s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id;
                """

                cursor.execute(insert_query, (clean_motivation_name,))

                motivation_id = cursor.fetchone()[0]
                motivation_baru += 1
                print(f"Classified Motivation BARU ditambahkan: {clean_motivation_name} (ID: {motivation_id})")

        conn.commit()
        print(f"Selesai, {motivation_baru} Baru, {motivation_lama} Sudah Ada")

    except Exception as e:
        print(f"Error database: {e}")
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

    return result['id'] if result else None

def get_aggravating_id(cursor, aggravating):
    if not aggravating:
        return []

    if isinstance(aggravating, str):
        aggravating = [aggravating]

    query = """
        SELECT id FROM classified_aggravating 
        WHERE classified_name = ANY(%s)
    """
    cursor.execute(query, (aggravating,))
    results = cursor.fetchall()

    return [r[0] for r in results]

def get_motivation_id(cursor, motivation):
    if not motivation or motivation == "Tidak Diketahui":
        return None
    
    query = "SELECT id FROM classified_motivation WHERE classified_name = %s LIMIT 1"
    cursor.execute(query, (motivation,))
    result = cursor.fetchone()

    return result['id'] if result else None

def import_aggravating_factors(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    aggravating_factors = data.get("why", {}).get("aggravating_factors", [])
    classified_name = data.get("why", {}).get("classified_motivation", None)
    case_number = data.get("what", {}).get("case_number", None)

    if isinstance(aggravating_factors, str):
        aggravating_factors = [aggravating_factors]

    if not aggravating_factors:
        print("Tidak ada aggravating factors di file")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        aggravating_baru = 0
        aggravating_lama = 0

        id_aggravating = get_aggravating_id(cursor, classified_name)
        id_cases = get_case_id(cursor, case_number)
        
        for description in aggravating_factors:
            clean_description = description.strip()
            if not clean_description:
                continue

            cursor.execute("SELECT id FROM public.aggravating_factors WHERE description = %s", (clean_description,))
            result = cursor.fetchone()

            if result:
                aggravating_id = result[0]
                aggravating_lama += 1
            else:
                insert_query = """
                    INSERT INTO public.aggravating_factors (id_cases, id_aggravating, description, created_at, updated_at)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id;
                """
                cursor.execute(insert_query, (id_cases, id_aggravating, clean_description,))
                
                aggravating_id = cursor.fetchone()[0]
                aggravating_baru += 1
                
                print(f"aggravating Factor BARU ditambahkan: {clean_description} (ID: {aggravating_id})")

        conn.commit()
        print(f"Selesai: {aggravating_baru} Baru, {aggravating_lama} Sudah Ada")

    except Exception as e:
        print(f"Error database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def import_motivation_factors(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    motivation_factors = data.get("why", {}).get("motivation_factors", [])
    classified_name = data.get("why", {}).get("classified_motivation", None)

    if isinstance(motivation_factors, str):
        motivation_factors = [motivation_factors]

    if not motivation_factors:
        print("Tidak ada motivation factors di file")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        motivation_baru = 0
        motivation_lama = 0

        id_motivation = get_motivation_id(cursor, classified_name)
        
        for description in motivation_factors:
            clean_description = description.strip()
            if not clean_description:
                continue

            cursor.execute("SELECT id FROM public.motivation_factors WHERE description = %s", (clean_description,))
            result = cursor.fetchone()

            if result:
                motivation_id = result[0]
                motivation_lama += 1
            else:
                insert_query = """
                    INSERT INTO public.motivation_factors (id_motivation, description, created_at, updated_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id;
                """
                cursor.execute(insert_query, (id_motivation, clean_description,))
                
                motivation_id = cursor.fetchone()[0]
                motivation_baru += 1
                
                print(f"Motivation Factor BARU ditambahkan: {clean_description} (ID: {motivation_id})")

        conn.commit()
        print(f"Selesai: {motivation_baru} Baru, {motivation_lama} Sudah Ada")

    except Exception as e:
        print(f"Error database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()


import_aggravating_factors("19_PID~SUS_2022_PN JKT~TIM.json")