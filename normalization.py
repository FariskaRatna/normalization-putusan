import os
import json
import pandas as pd
import re
from normalize_case_info import normalize_case_data

# =========================
# CONFIG
# =========================
INPUT_FOLDER = "imputed_json"
OUTPUT_FOLDER = "clean_json"
KAMUS_LOKASI = "kamus_lokasi.json"
KAMUS_SUPER = "kamus_super_wilayah.json"

FILENAME_CODE_MAP = [
    # Jakarta — spesifik dulu
    (r'JKT[~\s]*UT[R]?',   'DKI Jakarta',        'Jakarta Utara'),
    (r'JKT[~\s]*BRT',      'DKI Jakarta',        'Jakarta Barat'),
    (r'JKT[~\s]*TIM',      'DKI Jakarta',        'Jakarta Timur'),
    (r'JKT[~\s]*SEL',      'DKI Jakarta',        'Jakarta Selatan'),
    (r'JKT[~\s]*PST',      'DKI Jakarta',        'Jakarta Pusat'),
    (r'JAK[~\s]*SEL',      'DKI Jakarta',        'Jakarta Selatan'),
    (r'JKTSEL',            'DKI Jakarta',        'Jakarta Selatan'),
    (r'\bJKT\b',           'DKI Jakarta',        'Jakarta'),
    (r'\bDKI\b',           'DKI Jakarta',        'Jakarta'),
    # Bali
    (r'\bDPS\b',           'Bali',               'Denpasar'),
    # Maluku
    (r'\bAMB\b',           'Maluku',             'Ambon'),
    # Maluku Utara
    (r'\bTBK\b',           'Maluku Utara',       'Tobelo'),
    # Jawa Timur
    (r'\bPLH\b',           'Jawa Timur',         'Sidoarjo'),
    (r'\bMLN\b',           'Jawa Timur',         'Malang'),
    (r'\bTUB\b',           'Jawa Timur',         'Tulungagung'),
    # Jawa Tengah
    (r'\bSMG\b',           'Jawa Tengah',        'Semarang'),
    # Jawa Barat
    (r'\bTAS\b',           'Jawa Barat',         'Tasikmalaya'),
    (r'\bBAN\b',           'Jawa Barat',         'Bandung'),
    # DI Yogyakarta
    (r'\bBTL\b',           'DI Yogyakarta',      'Bantul'),
    # Banten
    (r'\bBTN\b',           'Banten',             'Banten'),
    (r'\bTBNN\b',          'Banten',             'Tigaraksa'),
    # Sulawesi Selatan
    (r'\bMKS\b',           'Sulawesi Selatan',   'Makassar'),
    # Sulawesi Tengah
    (r'\bPLI\b',           'Sulawesi Tengah',    'Palu'),
    (r'\bDGL\b',           'Sulawesi Tengah',    'Donggala'),
    # Sulawesi Tenggara / lainnya
    (r'\bSON\b',           'Nusa Tenggara Timur','Soe'),
    (r'\bSGT\b',           'Kalimantan Timur',   'Sangatta'),
    (r'\bSMR\b',           'Kalimantan Timur',   'Samarinda'),
    # Kalimantan Barat
    (r'\bPTS\b',           'Kalimantan Barat',   'Pontianak'),
    (r'\bPLW\b',           'Kalimantan Tengah',  'Palangka Raya'),
    (r'\bKLB\b',           'Nusa Tenggara Timur','Kalabahi'),
    # Sumatera Utara
    (r'\bMDN\b',           'Sumatera Utara',     'Medan'),
    (r'\bLTK\b',           'Sumatera Utara',     'Langkat'),
    (r'PT[~\s]*MDN',       'Sumatera Utara',     'Medan'),
    (r'PTMDN',             'Sumatera Utara',     'Medan'),
    # Aceh
    (r'\bSBH\b',           'Aceh',               'Sabang'),
    # Papua
    (r'\bNAB\b',           'Papua',              'Nabire'),
    # Riau / Kalimantan
    (r'\bRHL\b',           'Riau',               'Rokan Hulu'),
    # Jawa Tengah
    (r'\bPR\b',            'Jawa Tengah',        'Purworejo'),
    # Jambi
    (r'PT[~\s]*JMB|\bJMB\b', 'Jambi',           'Jambi'),
    # PT DKI (banding)
    (r'PT[~\s]*DKI|PTDKI',  'DKI Jakarta',       'Jakarta'),
    # SDN — PN Sukadana (Kayong Utara, Kalimantan Barat)
    (r'\bSDN\b',           'Kalimantan Barat',   'Sukadana'),
    # PM = Pengadilan Militer — PM I-04 = Palembang (Sumatera Selatan)
    (r'PM[~\s]*I[~\s]*04', 'Sumatera Selatan',   'Palembang'),
    # putusan_xxx_pn_jkt.utr / pn_.jkt_utr
    (r'JKT[._\s]*UTR',     'DKI Jakarta',        'Jakarta Utara'),
    (r'JKT[._\s]*BRT',     'DKI Jakarta',        'Jakarta Barat'),
    (r'JKT[._\s]*TIM',     'DKI Jakarta',        'Jakarta Timur'),
    (r'JKT[._\s]*SEL',     'DKI Jakarta',        'Jakarta Selatan'),
]

BULAN_MAP = {
    "januari": 1, "jan": 1,
    "februari": 2, "feb": 2, "pebruari": 2,
    "maret": 3, "mar": 3,
    "april": 4, "apr": 4,
    "mei": 5,
    "juni": 6, "jun": 6,
    "juli": 7, "jul": 7,
    "agustus": 8, "agu": 8, "agt": 8,
    "september": 9, "sep": 9, "sept": 9,
    "oktober": 10, "okt": 10,
    "november": 11, "nov": 11, "nopember": 11,
    "desember": 12, "des": 12
}

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

try:
    with open(KAMUS_LOKASI, 'r', encoding='utf-8') as f:
        kamus_data = json.load(f)
        
    TYPO_FIX = kamus_data.get("TYPO_FIX", {})
    CITY_TO_PROVINCE = {k.lower(): v for k, v in kamus_data.get("CITY_TO_PROVINCE", {}).items()}
    FOREIGN_LOCATIONS = {loc.lower() for loc in kamus_data.get("FOREIGN_LOCATIONS", [])}
    MANUAL_MAP = {k.lower(): tuple(v) for k, v in kamus_data.get("MANUAL_MAP", {}).items()}

    with open(KAMUS_SUPER, 'r', encoding='utf-8') as f:
        raw_master = json.load(f)
        MASTER_WILAYAH = {k.lower(): v for k, v in raw_master.items()}

    PROVINCE_NAMES = {v["prov"].lower() for v in MASTER_WILAYAH.values()}
    PROVINCE_NAMES.update({'kaltim', 'sumatra utara', 'sumatra barat', 'sumatera barat'})

    print("Kamus lokasi berhasil dimuat!")

except Exception as e:
    print("Gagal memuat kamus lokasi: ", e)
    exit()

PROVINCE_TO_CITIES = {}
for wilayah, data_wil in MASTER_WILAYAH.items():
    prov = data_wil["prov"]
    kota = data_wil["kota"]
    if prov and kota:
        if prov not in PROVINCE_TO_CITIES:
            PROVINCE_TO_CITIES[prov] = set()
        PROVINCE_TO_CITIES[prov].add(wilayah.title()) 

for prov in PROVINCE_TO_CITIES:
    PROVINCE_TO_CITIES[prov] = sorted(list(PROVINCE_TO_CITIES[prov]), key=len, reverse=True)

# =========================
# FUNGSI BARU: EKSTRAKSI WAKTU AKTIVITAS
# =========================
def extract_year(text):
    match = re.search(r"(19|20)\d{2}", str(text))
    return int(match.group()) if match else None

bulan_map = {
    "januari": 1, "februari": 2, "maret": 3, "april": 4,
    "mei": 5, "juni": 6, "juli": 7, "agustus": 8,
    "september": 9, "oktober": 10, "november": 11, "desember": 12
}

def extract_month(text):
    text = str(text).lower()
    for nama_bulan, angka in bulan_map.items():
        if nama_bulan in text:
            return angka
    return None

def clean_halaman(text):
    text = re.sub(r'\(Wilayah:.*?\)', '', str(text), flags=re.IGNORECASE)
    return re.sub(r'\s*\(Halaman\s*[\d]+\)', '', text, flags=re.IGNORECASE).strip()

def halaman_remove_local_network(text):
    if not text or str(text).strip() in ["", "Tidak Diketahui", "None"]:
        return "Tidak Diketahui"
        
    text = str(text)
    text = re.sub(r'\s*\(Halaman.*', '', text, flags=re.IGNORECASE)
    
    clean_text = text.strip()
    return clean_text if clean_text else "Tidak Diketahui"

def fix_typos(text):
    t = str(text)
    for pat, repl in TYPO_FIX.items():
        t = re.sub(pat, repl, t, flags=re.IGNORECASE)
    return t

def extract_location(raw_text):
    if not raw_text or str(raw_text).strip() == '':
        return None, None
    
    wilayah_match = re.search(r'\(Wilayah:\s*(.*?)\)', str(raw_text), flags=re.IGNORECASE)
    wilayah_text = wilayah_match.group(1) if wilayah_match else ""

    cleaned_main = clean_halaman(raw_text)
    cleaned_main = fix_typos(cleaned_main)

    def search_in_text(text_to_search):
        if not text_to_search:
            return None, None
        
        text_lower = text_to_search.lower()
        if text_lower in FOREIGN_LOCATIONS:
            return 'Luar Negeri', text_to_search
        if text_lower in MANUAL_MAP:
            return MANUAL_MAP[text_lower]

        # 1. Identifikasi semua Provinsi yang disebut secara eksplisit di teks
        # Ini untuk verifikasi (Confirmation)
        mentioned_provinces = []
        for prov_name in PROVINCE_NAMES:
            if re.search(r'\b' + re.escape(prov_name) + r'\b', text_lower):
                mentioned_provinces.append(prov_name)

        # 2. Cari semua kandidat lokasi dari Master Wilayah
        words = re.findall(r'\b\w+\b', text_lower)
        candidates = []
        
        for n in [4, 3, 2, 1]:
            for i in range(len(words) - n + 1):
                phrase = " ".join(words[i:i+n])
                if phrase in MASTER_WILAYAH:
                    res_prov = MASTER_WILAYAH[phrase]["prov"]
                    res_kota = MASTER_WILAYAH[phrase]["kota"]
                    
                    # HITUNG SKOR KANDIDAT
                    score = n * 10 # Semakin panjang frasa, semakin bagus
                    
                    # BONUS: Jika Provinsinya cocok dengan yang disebut di teks
                    if res_prov.lower() in mentioned_provinces:
                        score += 100 
                        
                    # BONUS: Jika dia adalah nama Kota resmi (bukan cuma kelurahan)
                    if phrase in CITY_TO_PROVINCE:
                        score += 20
                    
                    # BONUS: Prioritas kata yang lebih ke kanan (akhir kalimat)
                    score += i 

                    candidates.append({
                        'prov': res_prov,
                        'kota': res_kota if phrase not in PROVINCE_NAMES else None,
                        'score': score
                    })

        if not candidates:
            return None, None

        # Pilih kandidat dengan skor tertinggi
        candidates.sort(key=lambda x: x['score'], reverse=True)
        best = candidates[0]
        
        return best['prov'], best['kota']
    
    prov, kota = search_in_text(cleaned_main)
    if prov is None and wilayah_text:
        cleaned_wilayah = fix_typos(re.sub(r'\s*\(Halaman\s*[\d]+\)', '', wilayah_text, flags=re.IGNORECASE).strip())
        prov, kota = search_in_text(cleaned_wilayah)
    
    return prov, kota

def extract_time_info(text):
    if not text or str(text).strip() in ["", "unknown", "tidak diketahui"]:
        return {"year": None, "month": None}
    
    text_lower = str(text).lower()

    year_match = re.search(r"\b(19|20)\d{2}\b", text_lower)
    year = int(year_match.group()) if year_match else None

    month = None
    for nama_bulan, angka in BULAN_MAP.items():
        if re.search(r"\b" + nama_bulan + r"\b", text_lower):
            month = angka
            break

    return {"year": year, "month": month}

def process_temporal_list(raw_list):
    if not raw_list:
        return []
    
    if isinstance(raw_list, str):
        raw_list = [raw_list]
        
    hasil = []
    for item in raw_list:
        parts = str(item).split("\n---\n")
        for part in parts:
            part_clean = part.strip()
            if len(part_clean) > 3:
                waktu = extract_time_info(part_clean)
                hasil.append({
                    "aktivitas": part_clean,
                    "tahun": waktu["year"],
                    "bulan": waktu["month"]
                })
    return hasil

def enrich_activities(data):
    what_obj = data.get("what", {})
    activity_keys = ['pelatihan_activities', 'perencanaan_activities', 'tindakan_activities']
    
    for act_key in activity_keys:
        if act_key in what_obj and isinstance(what_obj[act_key], list):
            for item in what_obj[act_key]:
                if isinstance(item, dict):
                    if 'time' in item:
                        item['estimated_year'] = extract_year(item['time'])
                        item['estimated_month'] = extract_month(item['time'])
                    
                    if 'location' in item:
                        prov, kota = extract_location(item['location'])
                        item['estimated_province'] = prov
                        item['estimated_city'] = kota


    final_prov_network = data.get("who", {}).get("provinsi_jaringan")
    final_kota_network = data.get("who", {}).get("kota_jaringan")

    for act_key in activity_keys:
        if act_key in what_obj and isinstance(what_obj[act_key], list):
            for item in what_obj[act_key]:
                if isinstance(item, dict):
                    prov = item.get('estimated_province')
                    kota = item.get('estimated_city')
                    loc_source = str(item.get('location_source', ''))

                    if kota is None or str(kota).strip() == '':
                        
                        if 'description' in item and prov:
                            kota_dari_desc = extract_kota_from_activity_for_province(item['description'], prov)
                            if kota_dari_desc:
                                item['estimated_city'] = kota_dari_desc
                                continue 

                        if 'Tier 4' in loc_source or 'local_network' in loc_source:
                            
                            if not prov and final_prov_network and final_prov_network != 'Unknown':
                                item['estimated_province'] = final_prov_network
                                
                            if final_kota_network and final_kota_network not in ('Unknown', 'tidak disebutkan'):
                                item['estimated_city'] = final_kota_network

    return data

def extract_kota_from_activity_for_province(activity_text, known_province):
    """Cari kota/kab spesifik dari where_activity_locations untuk provinsi yang sudah diketahui."""
    if pd.isna(activity_text) or known_province is None:
        return None

    cities_in_province = PROVINCE_TO_CITIES.get(known_province, [])
    if not cities_in_province:
        return None

    entries = [e.strip() for e in str(activity_text).split('---')]
    for entry in entries:
        for city in cities_in_province:
            pattern = r'\b' + re.escape(city) + r'\b'
            if re.search(pattern, entry, flags=re.IGNORECASE):
                return city
    return None

def extract_province_from_filename(filename):
    if pd.isna(filename):
        return None, None
    fn = str(filename).upper().replace('~', ' ').replace('.', ' ')
    for pat, prov, kota in FILENAME_CODE_MAP:
        if re.search(pat, fn, flags=re.IGNORECASE):
            return prov, kota
    return None, None


def normalize_charged_articles(data):
    what_block = data.get("what", {})
    if not isinstance(what_block, dict):
        what_block = {}
        
    raw_articles = what_block.get("charged_articles", [])
    if not raw_articles:
        raw_articles = data.get("charged_articles", [])

    if not raw_articles or not isinstance(raw_articles, list):
        if "what" not in data:
            data["what"] = {}

        data["what"]["normalized_articles"] = ["Tidak Diketahui"]
        return data

    combined_text = " ".join([str(r).lower() for r in raw_articles])
    global_law = "UU Terorisme" # Default fallback
    
    if any(k in combined_text for k in ["terorisme", "perpu", "15 tahun 2003", "5 tahun 2018"]):
        global_law = "UU Terorisme"
    elif "pendanaan" in combined_text or "9 tahun 2013" in combined_text:
        global_law = "UU Pendanaan Terorisme"
    elif "kuhp" in combined_text:
        global_law = "KUHP"
    elif "darurat" in combined_text or "12 tahun 1951" in combined_text:
        global_law = "UU Darurat No. 12 Tahun 1951"

    cleaned_results = set()
    has_specific_pasal = False

    for raw_text in raw_articles:
        text = str(raw_text).lower().replace("menja di", "menjadi").strip()
        
        # 3. Ekstraksi Pasal
        pattern = r'(pasal\s+\d+[a-z]*(\s+huruf\s+[a-z])?(\s+ayat\s*\(\d+\))?(\s*jo\.?\s*pasal\s+\d+[a-z]*(\s+huruf\s+[a-z])?)?)'
        pasal_match = re.search(pattern, text)
        
        if pasal_match:
            has_specific_pasal = True
            pasal_raw = pasal_match.group(1)
            
            pasal_clean = re.sub(r'jo\.+', 'jo', pasal_raw) 
            pasal_clean = pasal_clean.replace("jo", "jo.")
            final_pasal = re.sub(r'\s+', ' ', pasal_clean).strip().title().replace("Jo.", "jo.")
            
            local_law = ""
            if any(k in text for k in ["terorisme", "perpu", "15 tahun 2003", "5 tahun 2018"]):
                local_law = "UU Terorisme"
            elif "pendanaan" in text or "9 tahun 2013" in text:
                local_law = "UU Pendanaan Terorisme"
            elif "kuhp" in text:
                local_law = "KUHP"
            elif "darurat" in text or "12 tahun 1951" in text:
                local_law = "UU Darurat No. 12 Tahun 1951"
            else:
                local_law = global_law
            
            cleaned_results.add(f"{final_pasal} {local_law}".strip())
            
        else:
            pass

    if not has_specific_pasal and global_law:
        cleaned_results.add(global_law)

    # 5. Simpan Hasil
    if "what" not in data:
        data["what"] = {}
        
    data["what"]["normalized_articles"] = sorted(list(cleaned_results))
    
    return data

def is_valid_court_name(raw_text):
    if not raw_text or str(raw_text).lower() == "tidak diketahui":
        return "Tidak Diketahui"
    
    text_lower = raw_text.lower()
    
    valid_prefixes = ["pengadilan negeri", "pengadilan tinggi", "mahkamah agung", "pn ", "pt "]
    if not any(p in text_lower for p in valid_prefixes):
        return False 
        
    noise_words = [
        'sejak', 'yang', 'memeriksa', 'mengadili', 'perkara', 
        'atau', 'tempat', 'lain', 'di', 'dari', 'dan', 'klas', 'sebagai', 'untuk'
    ]
    if any(n in text_lower for n in noise_words):
        return False
        
    return True

def clean_noisy_court(raw_text):
    if not raw_text or str(raw_text).lower() == "tidak diketahui":
        return "Tidak Diketahui"
    
    text = re.sub(r'\bMe\s+dan\b', 'Medan', str(raw_text), flags=re.IGNORECASE)
    match = re.search(r'(?i)(Pengadilan\s+Negeri|PN)\s+([a-zA-Z]+(?:\s+[a-zA-Z]+){0,2})', text)
    
    if match:
        potential_city = match.group(2).strip()

        noise_words = [
            'sejak', 'yang', 'memeriksa', 'mengadili', 'perkara', 
            'atau', 'tempat', 'lain', 'di', 'dari', 'dan', 'klas', 'sebagai', 'untuk'
        ]

        clean_words = []
        for word in potential_city.split():
            if word.lower() in noise_words:
                break 
            clean_words.append(word)
            
        clean_city_name = " ".join(clean_words).title()
        
        if clean_city_name:
            return f"Pengadilan Negeri {clean_city_name}"
            
    return str(raw_text).strip()

def normalize_court_info(data):
    where_block = data.get("where") or {}
    what_block = data.get("what") or {}
    
    case_number = what_block.get("case_number") or data.get("case_id") or ""
    case_number = str(case_number).strip()
    raw_name = where_block.get("district_court") or what_block.get("court_name") or where_block.get("court_location") or ""

    def safe_val(v):
        if not v or str(v).lower() in ["none", "null", "tidak diketahui", "", "[lokasi tidak terdeteksi]"]:
            return "Tidak Diketahui"
        return str(v).strip()
    
    case_upper = case_number.upper()

    if re.search(r'\d+\s*(K|PK)/', case_upper) or "/MA" in case_upper or "MAHKAMAH AGUNG" in str(raw_name).upper():
        clean_name = "Mahkamah Agung RI"
        kota, prov, code = "Jakarta Pusat", "DKI Jakarta", "MA"

        where_block.update({
            "district_court": clean_name, "court_location": kota,
            "normalized_court_province": prov, "normalized_court_city": kota,
            "normalized_court_code": code
        })
        what_block["court_name"] = clean_name
        
        data["where"] = where_block
        data["what"] = what_block # Pastikan disimpan kembali!
        return data

    level = "PT" if re.search(r'\bPT\b', case_upper) else "PN"

    if is_valid_court_name(raw_name) and "none" not in str(raw_name).lower():
        clean_name = str(raw_name).strip()
        prov_raw, kota_raw = extract_location(clean_name)
        prov, kota = safe_val(prov_raw), safe_val(kota_raw)
        code = extract_court_code(case_number, fallback_location=kota, clean_court_name=clean_name)
    else:
        text_to_clean = re.sub(r'sejak|yang memeriksa|mengadili|perkara|putusan|memperhatikan|atau|tempat lain|none|null', '', str(raw_name), flags=re.IGNORECASE)
        if text_to_clean:
            prov_raw, kota_raw = extract_location(text_to_clean)
        else:
            prov_raw, kota_raw = None, None
        
        prov, kota = safe_val(prov_raw), safe_val(kota_raw)

        if kota == "Tidak Diketahui" and case_number:
            match = re.search(r'/\d{4}/([^/]+)$', case_number.strip())
            if match:
                raw_code = match.group(1).strip()
                clean_code = re.sub(r'^(PT|PN)[\.\s]*', '', raw_code, flags=re.IGNORECASE)
                clean_code = re.sub(r'[^a-zA-Z0-9]', ' ', clean_code)
                clean_code = re.sub(r'\s+', ' ', clean_code).strip().upper()
                darurat_map = {
                    "DKI": ("DKI Jakarta", "Jakarta Pusat"), # PT DKI lokasinya di Jakarta Pusat
                    "JKT UTR": ("DKI Jakarta", "Jakarta Utara"),
                    "JKT TIM": ("DKI Jakarta", "Jakarta Timur"),
                    "JKT SEL": ("DKI Jakarta", "Jakarta Selatan"),
                    "JKT PST": ("DKI Jakarta", "Jakarta Pusat"),
                    "JKT BRT": ("DKI Jakarta", "Jakarta Barat"),
                    "JMB": ("Jambi", "Jambi"),
                    "KLB": ("Nusa Tenggara Timur", "Kalabahi"),
                    "SGT": ("Kalimantan Timur", "Sangatta"),
                    "MDN": ("Sumatera Utara", "Medan"),
                    "TUB": ("Bengkulu", "Tubei"),
                    "PTS": ("Kalimantan Barat", "Putussibau"),
                    "TAS": ("Bengkulu", "Tais"),
                    "LTK": ("Flores Barat", "Larantuka"),
                    "PR": ("Kalimantan Tengah", "Kalimantan Tengah"),
                    "SBH": ("Sumatera Utara", "Sibuhuan"),
                    "PLW": ("Riau", "Pelalawan")
                }
                
                if clean_code.upper() in darurat_map:
                    prov_cad, kota_cad = darurat_map[clean_code.upper()]
                else:
                    prov_cad, kota_cad = extract_location(clean_code)
                    
                prov_cad, kota_cad = safe_val(prov_cad), safe_val(kota_cad)
                if kota_cad != "Tidak Diketahui":
                    prov, kota = prov_cad, kota_cad

        if kota == "Tidak Diketahui":
            activity_locs = where_block.get("activity_locations", [])
            if isinstance(activity_locs, list):
                combined_locs = " ".join(activity_locs)
                prov_act, kota_act = extract_location(combined_locs)
                prov_act, kota_act = safe_val(prov_act), safe_val(kota_act)
                
                if kota_act != "Tidak Diketahui":
                    prov, kota = prov_act, kota_act

        if kota == "Tidak Diketahui":
            match_name = re.search(r'(?i)(?:Pengadilan\s+(?:Negeri|Tinggi)|PN|PT)\s+([a-zA-Z]+)', str(raw_name))
            if match_name:
                kota = match_name.group(1).strip().title()
            elif case_number:
                match_code = re.search(r'/\d{4}/([^/]+)$', case_number.strip())
                if match_code:
                    kota = re.sub(r'^(PT|PN)[\.\s]*', '', match_code.group(1).strip(), flags=re.IGNORECASE).title()
                    kota = re.sub(r'[^a-zA-Z0-9]', '', kota)
        
        if level == "PT" and "Jakarta" in kota:
            clean_name = "Pengadilan Tinggi DKI Jakarta"
        else:
            prefix = "Pengadilan Tinggi" if level == "PT" else "Pengadilan Negeri"
            clean_name = f"{prefix} {kota}" if kota != "Tidak Diketahui" else f"{prefix} [Lokasi Tidak Terdeteksi]"
            
        code = extract_court_code(case_number, fallback_location=kota, clean_court_name=clean_name)

    # ==========================================
    # UPDATE JSON
    # ==========================================
    where_block.update({
        "district_court": clean_name,
        "court_location": kota if kota != "Tidak Diketahui" else None,
        "normalized_court_province": prov if prov != "Tidak Diketahui" else None,
        "normalized_court_city": kota if kota != "Tidak Diketahui" else None,
        "normalized_court_code": code
    })

    what_block["court_name"] = clean_name
    what_block["case_number"] = case_number
    
    data["where"] = where_block
    data["what"] = what_block
    return data


def safe_parse_date(x):
    if not x:
        return pd.NaT

    # coba parse normal dulu (ISO format)
    dt = pd.to_datetime(x, errors="coerce")

    # kalau gagal, coba dayfirst (DD-MM-YYYY)
    if pd.isna(dt):
        dt = pd.to_datetime(x, errors="coerce", dayfirst=True)

    return dt


# =========================
# NORMALISASI IDEOLOGI
# =========================
def normalize_ideology(x):

    if not x or str(x).strip().lower() in ["", "unknown", "tidak diketahui"]:
        return "Tidak Diketahui"

    text = str(x).lower()

    # cleaning kuat
    text = re.sub(r"[^a-z\s/]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    # shortcut mapping
    DIRECT_MAP = {
        "jad": "Jamaah Ansharut Daulah (JAD)",
        "ji": "Jemaah Islamiyah (JI)",
        "isis": "Islamic State (ISIS)",
        "mit": "Mujahidin Indonesia Timur (MIT)",
        "fpi": "Front Pembela Islam (FPI)",
        "nii": "Negara Islam Indonesia (NII)"
    }

    if text in DIRECT_MAP:
        return DIRECT_MAP[text]

    result = set()

    # ISIS
    if re.search(r"\bisis\b|islamic state|daulah islamiy|daulah islam|daullah|daulah islamiah is", text):
        result.add("Islamic State (ISIS)")

    # JI
    if re.search(r"jamaah islamiyah|jemaah islamiyah|jama.ah islamiyah|\bji\b", text):
        result.add("Jemaah Islamiyah (JI)")

    # JAD (FIXED)
    if re.search(r"\bjad\b", text) or "anshor daulah" in text or "anshorut daulah" in text:
        result.add("Jamaah Ansharut Daulah (JAD)")

    # MIT
    if re.search(r"\bmit\b|mujahidin indonesia timur", text):
        result.add("Mujahidin Indonesia Timur (MIT)")

    # FPI
    if re.search(r"\bfpi\b|front pembela islam", text):
        result.add("Front Pembela Islam (FPI)")

    # NII
    if re.search(r"negara islam indonesia|\bnii\b|nii kw|muhammad yusuf tohiri|\bNII Faksi MYT\b", text):
        result.add("Negara Islam Indonesia (NII)")

    # AL QAEDA
    if re.search(r"qaeda|qaedah|qoidah|tanzim al qaeda", text):
        result.add("Al Qaeda")
    
    if "majelis mujahidin indonesia" in text:
        result.add("Majelis Mujahidin Indonesia")

    if "mujahidin pilipina" in text:
        result.add("Mujahidin Pilipina")

    if "khilafah islamiyah" in text:
        result.add("Khilafah Islamiyah")

    return " | ".join(sorted(result)) if result else "Tidak Diketahui"

def split_field(text, sep="\n---\n"):
    """Fungsi pembantu untuk memecah string yang digabung dengan separator khusus."""
    if not text or str(text).strip().lower() in ["", "unknown", "none", "null"]:
        return []
    return [x.strip() for x in str(text).split(sep) if x.strip()]

def classify_source(x):
    """Mengklasifikasikan satu teks sumber radikalisasi ke dalam kategori."""
    if not x or str(x).strip().lower() in ["", "unknown", "none", "null"]:
        return "Data Tidak Lengkap"
        
    x = str(x).lower()
    
    # Video/Digital ISIS
    if any(k in x for k in ["video","isis","daulah","peperangan","eksekusi","rilisan","youtube",
                              "nasyid","film","propaganda","konten","streaming"]):
        return "Video/Konten Digital ISIS"
    # Media Sosial/Chat
    if any(k in x for k in ["facebook","whatsapp","telegram","group","grup","channel",
                              "media sosial","medsos","akun","instagram","website","internet",
                              "online","twitter","wa ","fb ","chat","broadcast","aplikasi",
                              "e-mail","email","tiktok"]):
        return "Media Sosial/Chat"
    # Kajian/Pengajian Informal
    if any(k in x for k in ["kajian","pengajian","ustadz","ceramah","majelis","halaqah",
                              "tarbiyah","taklim","tabligh","dakwah","tauhid","jihad","hijrah",
                              "khutbah","khotbah","ta'lim","talim","syirik","aqidah","fiqih",
                              "muamalah","iman","subuh","magrib","isya","pengurus","pengajar",
                              "pembicara","penceramah","training","pelatihan agama"]):
        return "Kajian/Pengajian Informal"
    # Pondok/Yayasan
    if any(k in x for k in ["pesantren","ponpes","yayasan","pondok","madrasah","ma'had"]):
        return "Pondok/Yayasan"
    # Materi Fisik
    if any(k in x for k in ["buku","materi","cd","audio","majalah","artikel","modul",
                              "kitab","leaflet","pamflet","selebaran","dokumen","file","pdf"]):
        return "Materi Fisik"
    # Pengaruh/Jaringan
    if any(k in x for k in ["pengaruh","komunikasi","keluarga","baiat","amir","jamaah",
                              "kelompok","mit","ji","jad","teman","sahabat","saudara",
                              "rekrut","merekrut","diajak","dipengaruhi","kontak","bertemu",
                              "perkenalan","jaringan","anggota","senior","junior","mentor"]):
        return "Pengaruh/Jaringan"
        
    return "Lainnya"

def normalize_radicalization_sources(raw_source):
    """
    Fungsi utama untuk normalization.py. 
    Menerima data mentah (string atau list), memecahnya, dan mengembalikan list unik kategori.
    """
    if not raw_source:
        return []
        
    # 1. Pastikan bentuknya List (Menerapkan logika df_d1_base["why_radicalization_sources"].str.split)
    if isinstance(raw_source, str):
        sources = split_field(raw_source, sep="\n---\n")
        if not sources: # Jika tidak pakai separator itu, anggap 1 kalimat utuh
            sources = [raw_source]
    elif isinstance(raw_source, list):
        sources = raw_source
    else:
        return []

    categories = set()
    for source in sources:
        source_str = str(source).strip()
        
        if len(source_str) > 3: 
            kategori = classify_source(source_str)
            if kategori not in ["Data Tidak Lengkap", "Lainnya"]:
                categories.add(kategori)

    return list(categories) if categories else ["Lainnya"]


def classify_radicalization_channel(source_list):
    if not source_list or source_list == ["Tidak Diketahui"]:
        return "Tidak Diketahui"
    
    source_set = set(s.lower() for s in source_list)

    digital_cats = {"media sosial/chat", "video/konten digital isis"}
    offline_cats = {
        "kajian/pengajian informal",
        "pondok/yayasan",
        "materi fisik",
        "kegiatan khusus",
        "organisasi/kelompok",
        "relasi/pengaruh",
    }

    has_digital = bool(source_set & digital_cats)
    has_offline = bool(source_set & offline_cats)

    if has_digital and has_offline:
        return "Hybrid"
    elif has_digital:
        return "Online"
    elif has_offline:
        return "Offline"
    else:
        return "Tidak Diketahui"


def classify_motivation(x):
    if not x or str(x).strip().lower() in ["", "nan", "unknown", "tidak diketahui"]:
        return ["Tidak Diketahui"]

    x_lower = str(x).lower()
    labels = set()

    if any(k in x_lower for k in [
        "syariat", "khilafah", "daulah", "negara islam", "hukum islam",
        "hukum allah", "iqomatudin", "kaffah", "kafah", "syariah",
        "penegakan", "menegakkan", "tegak", "tegaknya", "mendirikan",
        "merubah", "mengubah", "mengganti", "falsafah", "ideologi",
        "pancasila", "uud", "demokrasi", "thogut", "anshor thogut",
        "kafir", "kufur", "murtad",
    ]):
        labels.add("Ideologi Syariat/Khilafah")

    if any(k in x_lower for k in [
        "jihad", "amaliyah", "amaliah", "bom", "senjata", "perang",
        "mujahid", "syahid", "mati syahid", "berperang", "memerangi",
        "melawan", "membunuh", "meledak", "teror", "istishadiah",
        "i'dad", "idad", "persiapan fisik", "latihan",
    ]):
        labels.add("Jihad/Kekerasan")

    if any(k in x_lower for k in [
        "hijrah", "berhijrah", "berangkat ke suriah", "ke suriah",
    ]):
        labels.add("Hijrah")

    if any(k in x_lower for k in [
        "pemerintah", "nkri", "pancasila", "thogut", "demokrasi",
        "anti pemerintah", "polisi", "tni", "polri", "aparat",
        "tidak berhukum", "kafir karena", "tidak sesuai",
    ]):
        labels.add("Anti Pemerintah")

    if any(k in x_lower for k in [
        "baiat", "bai'at", "amir", "kelompok", "jamaah", "organisasi",
        "isis", "ji ", "jad", "daulah", "patuh", "taat", "setia",
        "muahadah", "bergabung", "ikut serta", "ikut andil",
        "pemimpin", "pimpinan", "amirul mukminin",
    ]):
        labels.add("Loyalitas Kelompok")

    if any(k in x_lower for k in [
        "balas", "dendam", "sakit hati", "marah", "kecewa",
        "kesal", "prihatin", "simpatik", "empati", "kemarahan",
        "ikut-ikutan", "ikut ikutan",
    ]):
        labels.add("Emosi Personal")

    if any(k in x_lower for k in [
        "ridho", "pahala", "membela islam", "membela umat",
        "agama", "tauhid", "aqidah", "akidah", "membela",
        "kesolehan", "sholeh", "ibadah", "allah", "quran",
        "sunah", "sunnah", "meningkatkan", "memperdalam",
        "memahami", "mempelajari",
    ]):
        labels.add("Religius/Normatif")

    return list(labels) if labels else ["Tidak Diketahui"]

def classify_aggravating(x):
    if not x or str(x).strip().lower() in ["", "nan", "unknown", "tidak diketahui"]:
        return ["Tidak Diketahui"]

    x_lower = str(x).lower()
    labels = set()

    if any(k in x_lower for k in [
        "meresahkan", "resah", "cemas", "teror", "takut", "ketakutan",
        "keresahan", "was-was", "tidak nyaman", "tidak aman", "ketentraman",
        "suasana teror", "rasa takut",
    ]):
        labels.add("Meresahkan Masyarakat")

    if any(k in x_lower for k in [
        "tidak mendukung", "bertentangan", "menghambat", "tidak menunjang",
        "tidak selaras", "melawan hukum",
    ]):
        labels.add("Tidak/Menghambat Program Pemerintah")

    if any(k in x_lower for k in [
        "korban jiwa", "meninggal", "kematian", "nyawa", "tewas",
        "hilangnya nyawa", "korban", "sadis",
    ]):
        labels.add("Menimbulkan Korban Jiwa")

    if any(k in x_lower for k in [
        "nkri", "internasional", "citra", "persatuan", "stabilitas",
        "keamanan negara", "kelangsungan", "keutuhan", "nasionalisme",
        "perdamaian", "ancaman", "membahayakan", "mengganggu keamanan",
        "ideologi", "pancasila",
    ]):
        labels.add("Mengancam Keamanan Negara")

    if any(k in x_lower for k in [
        "kerugian", "kerusakan", "hancur", "perekonomian", "ekonomi",
        "fasilitas", "objek vital", "obyek vital", "infrastruktur",
    ]):
        labels.add("Kerugian Materiil/Fasilitas")

    return list(labels) if labels else ["Tidak Diketahui"]

def process_and_classify_list(raw_text, classify_func):
    if not raw_text or str(raw_text).strip() in ["", "nan", "Unknown", "Tidak Diketahui"]:
        return ["Tidak Diketahui"]
    
    parts = str(raw_text).split("\n---\n")
    categories = set()

    for part in parts:
        part = part.strip()
        if len(part) > 3:
            result = classify_func(part)
            if isinstance(result, list):
                categories.update(result)
            else:
                categories.add(result)

    if len(categories) > 1 and "Tidak Diketahui" in categories:
        categories.discard("Tidak Diketahui")

    return sorted(categories) if categories else ["Tidak Diketahui"]

def normalize_evidence_items(data):
    what_obj = data.get("what", {})
    
    if not isinstance(what_obj, dict):
        return data
        
    items = what_obj.get("evidence_items", [])
    
    if not isinstance(items, list) or not items:
        return data
    
    for item in items:
        raw_desc = str(item.get("description", "")).lower().strip()

        category = "other"
        clean_name = raw_desc

        if any(k in raw_desc for k in ["pcp", "moser", "predator", "shotgun", "senapan angin"]):
            category = "weapon"
            clean_name = "Senjata Angin PCP"
        elif any(k in raw_desc for k in ["revolver", "pistol", "rakitan", "fn", "m16", "laras panjang"]):
            category = "weapon"
            clean_name = "Senjata Api / Amunisi"
        elif any(k in raw_desc for k in ["parang", "golok", "samurai", "pisau", "celurit", "sangkur"]):
            category = "weapon"
            clean_name = "Senjata Tajam"

        elif any(k in raw_desc for k in ["bom", "handak", "detonator", "belerang", "mesiu", "black powder"]):
            category = "explosive"
            clean_name = "Bahan Peledak / Komponen Bom"

        elif any(k in raw_desc for k in ["hp", "handphone", "ponsel", "nokia", "samsung", "xiaomi", "sim card"]):
            category = "phone"
            clean_name = "Telepon Seluler / SIM Card"
        elif any(k in raw_desc for k in ["laptop", "flashdisk", "harddisk", "memory card", "cpu"]):
            category = "other" # Atau buat kategori 'electronic' jika ada di SQL
            clean_name = "Perangkat Digital / Penyimpanan"

        elif any(k in raw_desc for k in ["buku", "kitab", "dokumen", "selebaran", "buletin", "paspor", "ktp"]):
            category = "document"
            clean_name = "Buku / Dokumen Fisik"

        elif any(k in raw_desc for k in ["uang", "rupiah", "cash", "atm", "rekening", "dolar"]):
            category = "cash"
            clean_name = "Uang Tunai / Instrumen Keuangan"

        elif any(k in raw_desc for k in ["motor", "mobil", "stnk", "bpkb"]):
            category = "vehicle"
            clean_name = "Kendaraan Bermotor"

        item["normalized_name"] = clean_name.title()
        item["item_category"] = category

        try:
            item["quantity"] = int(item.get("quantity", 1))
        except:
            item["quantity"] = 1

    return data

def normalize_people_names(data):
    if "who" not in data or not isinstance(data["who"], dict):
        data["who"] = {}
    
    who_block = data["who"]
    defendants = who_block.get("defendants", [])
    
    if not isinstance(defendants, list) or not defendants:
        who_block["defendants"] = [{
            "name": "Tidak Diketahui",
            "normalized_name": "Tidak Diketahui",
            "gender": "Tidak Diketahui",
            "religion": "Tidak Diketahui",
            "occupation": "Tidak Diketahui",
            "education_status": "Tidak Diketahui",
            "age": "Tidak Diketahui",
            "dob": None
        }]
    else:
        for person in defendants:
            fields_to_check = [
                'name', 'alias', 'gender', 'pob', 'dob', 'age', 
                'religion', 'nationality', 'occupation', 
                'education_status', 'address', 'nik', 'passport_no', 'kk_no'
            ]
            
            for field in fields_to_check:
                val = str(person.get(field, "")).strip()
                if not val or val.lower() in ["", "-", "null", "none"]:
                    person[field] = None if field == 'dob' else "Tidak Diketahui"

            raw_dob = person.get("dob")
            if raw_dob is not None and raw_dob != "Tidak Diketahui":
                dob_str = str(raw_dob).lower().strip()
                
                for indo_month, month_num in BULAN_MAP.items():
                    dob_str = re.sub(rf'\b{indo_month}\b', f"-{month_num}-", dob_str)
                
                dob_str = re.sub(r'[\s/.,\\]+', '-', dob_str)
                dob_str = re.sub(r'-+', '-', dob_str).strip('-')
                
                parsed_dob = safe_parse_date(dob_str)
                person["dob"] = parsed_dob.strftime("%Y-%m-%d") if pd.notna(parsed_dob) else None

            raw_name = person.get("name", "")
            if raw_name:
                clean_name = re.sub(r'\s+', ' ', str(raw_name)).strip()
                person["normalized_name"] = clean_name.title()

            gender = str(person.get("gender", "")).lower()
            if "laki" in gender or gender in ["pria", "l"]:
                person["gender"] = "Laki-laki"
            elif "perempuan" in gender or gender in ["wanita", "p"]:
                person["gender"] = "Perempuan"
            else:
                person["gender"] = "Tidak Diketahui"

            religion = str(person.get("religion", "")).lower()
            religions_map = {
                "islam": "Islam",
                "kristen": "Kristen",
                "hindu": "Hindu",
                "katolik": "Katolik",
                "budha": "Budha"
            }
            person["religion"] = "Tidak Diketahui"
            for k, v in religions_map.items():
                if k in religion:
                    person["religion"] = v
                    break

            edu = str(person.get("education_status", "")).strip()
            if edu in ["", "-", "null", "none"]:
                person["education_status"] = "Tidak Diketahui"

            age = str(person.get("age", "")).strip()
            if not age or age in ["", "null", "none" "Tidak Diketahui"]:
                person["normalized_age"] = None
            else:
                match = re.search(r'\d+', age)
                person["normalized_age"] = int(match.group()) if match else None

    co_def = data.get("co_defendants", []) or who_block.get("co_defendants", [])
    
    if not co_def or not isinstance(co_def, list):
        who_block["normalized_co_defendants"] = ["Tidak Diketahui"]
    else:
        clean_co_def = []
        for raw_name in co_def:
            clean = re.split(r'\s+(?:alias|als|bin|binti)\s+', str(raw_name).lower())[0]
            clean = re.sub(r'\b(ustad|pak haji|haji|ibu|bapak)\b', '', clean).strip()
            
            if clean:
                clean_co_def.append(clean.title())
            
        if not clean_co_def:
            who_block["normalized_co_defendants"] = ["Tidak Diketahui"]
        else:
            who_block["normalized_co_defendants"] = sorted(list(set(clean_co_def)))

    return data

def extract_clean_officials(raw_data):
    if not raw_data or raw_data == "" or raw_data == [""]:
        return ["Tidak Diketahui"]
        
    if isinstance(raw_data, list):
        text = ", ".join([str(x) for x in raw_data if x])
    else:
        text = str(raw_data)
        
    # 1. SPLIT DULU berdasarkn koma (karena koma memisahkan orang atau gelar)
    raw_chunks = text.split(',')
    
    clean_names = []
    
    regex_gelar_standar = r'\b(dr|prof|ir|drs|dra|hj|h|s\.?\s*h|m\.?\s*h|c\.?\s*n|bc\.?\s*ip|m\.?\s*s|m\.?\s*m|s\.?\s*e|s\.?\s*pd)\b\.?'
    regex_gelar_kurung = r'\bcll\s*\([a-z]\)\.?'
    
    for chunk in raw_chunks:
        chunk = re.sub(r'\bdan\b', '', chunk, flags=re.IGNORECASE)
        
        chunk = re.sub(regex_gelar_standar, '', chunk, flags=re.IGNORECASE)
        chunk = re.sub(regex_gelar_kurung, '', chunk, flags=re.IGNORECASE)
        
        chunk = re.sub(r'[^a-zA-Z\s]', '', chunk)
        chunk = re.sub(r'\s+', ' ', chunk).strip()
        
        if len(chunk) > 2 and chunk.lower() != "tidak diketahui":
            clean_names.append(chunk.title())
            
    if not clean_names:
        return ["Tidak Diketahui"]
        
    return sorted(list(set(clean_names)))

def _generate_fallback(location_name, clean_court_name=""):
    if location_name and location_name.lower() != "tidak diketahui":
        
        if clean_court_name and "tinggi" in clean_court_name.lower():
            prefix = "PT"
        else:
            prefix = "PN"
            
        if not location_name.upper().startswith(prefix):
            return f"{prefix} {location_name}"
        return location_name
    
    return "Kode tidak valid"

def extract_court_code(case_number, fallback_location="", clean_court_name=""):
    if not case_number or case_number.lower() == "tidak diketahui":
        return _generate_fallback(fallback_location, clean_court_name)
    
    match = re.search(r'/\d{4}/([^/]+)$', case_number.strip())

    if match:
        code = match.group(1).strip()
        code = code.rstrip('.')

        clean_code = re.sub(r'[^a-zA-Z0-9]', ' ', code)
        clean_code = re.sub(r'^(PN|PT)(?=[a-zA-Z])', r'\1 ', clean_code, flags=re.IGNORECASE)
        
        clean_code = re.sub(r'\s+', ' ', clean_code).strip()
        
        clean_code = clean_code.upper()

        if 3 <= len(code) <= 30:
            return code
        
    return _generate_fallback(fallback_location, clean_court_name)

BULAN_ID_EN = {
    "januari": "January", "februari": "February", "maret": "March",
    "april": "April", "mei": "May", "juni": "June",
    "juli": "July", "agustus": "August", "september": "September",
    "oktober": "October", "november": "November", "desember": "December"
}

def split_and_parse_timeline(date_string):
    if not date_string:
        return None, None
        
    parts = re.split(r'(?i)\s+sampai\s+dengan\s+|\s+s/d\s+|\s+hingga\s+', date_string)
    
    parsed_dates = []
    for part in parts:
        clean_part = re.sub(r'(?i)tanggal\s+', '', part).strip()
        
        for id_month, en_month in BULAN_ID_EN.items():
            if id_month in clean_part.lower():
                clean_part = re.sub(f'(?i){id_month}', en_month, clean_part)
                break
                
        parsed_dates.append(safe_parse_date(clean_part))

    start_dt = parsed_dates[0] if len(parsed_dates) > 0 else pd.NaT
    end_dt = parsed_dates[1] if len(parsed_dates) > 1 else pd.NaT
    
    start_date = start_dt.strftime('%Y-%m-%d') if pd.notna(start_dt) else None
    end_date = end_dt.strftime('%Y-%m-%d') if pd.notna(end_dt) else None
    
    return start_date, end_date

def extract_time_detention(data):
    when_obj = data.get("when", {})
    detention = when_obj.get("detention_timeline", [])
    
    if not detention or not isinstance(detention, list):
        if "when" not in data:
            data["when"] = {}

    for item in detention:
        if isinstance(item, dict) and 'date' in item:
            start, end = split_and_parse_timeline(item['date'])
            
            item["start_date"] = start
            item["end_date"] = end
            
            item.pop("estimated_year", None)
            item.pop("estimated_month", None)

    return data
import re

def _extract_numeric(val):
    """
    Helper function untuk mengekstrak angka murni dari string.
    Contoh: 'Rp 50.000.000,-' -> 50000000.0
    """
    if not val or str(val).strip().lower() in ["", "unknown", "n/a", "-"]:
        return None
        
    clean_val = re.sub(r"[^\d]", "", str(val))
    
    if not clean_val:
        return None
        
    try:
        return float(clean_val)
    except ValueError:
        return None

def clean_monetary_value(data):
    """
    Fungsi utama untuk normalization.py
    Mengambil, membersihkan, dan membuat field normalized baru untuk nilai uang.
    """
    if "how_much" not in data:
        data["how_much"] = {}
        
    how_much_obj = data["how_much"]
    
    raw_penalties = how_much_obj.get("monetary_penalties")
    raw_seized = how_much_obj.get("seized_money_amount")
    
    how_much_obj["normalized_monetary_penalties"] = _extract_numeric(raw_penalties)
    how_much_obj["normalized_seized_money_amount"] = _extract_numeric(raw_seized)
    
    return data

def normalized_joined_date(text):
    if pd.isna(text) or str(text).strip().lower() in ["nan", "none", "unknown", ""]:
        return None
    
    text_lower = str(text).lower()
    year_match = re.search(r'\b(19\d{2}|20\d{2})\b', text_lower)

    if not year_match:
        return None
    
    year = year_match.group(1)
    month = "01"
    for nama_bulan, angka_bulan in BULAN_MAP.items():
        if nama_bulan in text_lower:
            month = angka_bulan

    formatted_date = f"{year}-{month}-1"

    return formatted_date

# =========================
# PROCESS PER FILE
# =========================
def process_file(data):

    if "case_id" in data and isinstance(data["case_id"], str):
        data["case_id"] = data["case_id"].replace(" ", "")

    if "what" in data and isinstance(data["what"], dict):
        if "case_number" in data["what"] and isinstance(data["what"]["case_number"], str):
            data["what"]["case_number"] = data["what"]["case_number"].replace(" ", "")

    # =========================
    # HANDLE WHEN (FIX HARD)
    # =========================
    if "when" not in data or not isinstance(data["when"], dict):
        data["when"] = {}

    # ambil reference langsung
    when = data["when"]

    # parse
    court_date = safe_parse_date(when.get("district_court_date"))
    arrest_date = safe_parse_date(when.get("arrest_date"))

    # =========================
    # OVERWRITE LANGSUNG KE data["when"]
    # =========================
    data["when"]["district_court_date"] = (
        court_date.strftime("%Y-%m-%d") if pd.notna(court_date) else None
    )

    data["when"]["arrest_date"] = (
        arrest_date.strftime("%Y-%m-%d") if pd.notna(arrest_date) else None
    )

    # =========================
    # FEATURE
    # =========================
    data["when"]["tahun_putusan"] = int(court_date.year) if pd.notna(court_date) else None
    data["when"]["bulan_putusan"] = int(court_date.month) if pd.notna(court_date) else None

    data["when"]["tahun_penangkapan"] = int(arrest_date.year) if pd.notna(arrest_date) else None

    lag_bulan = None
    if pd.notna(court_date) and pd.notna(arrest_date):
        lag_bulan = round((court_date - arrest_date).days / 30.44, 1)

    data["when"]["lag_bulan"] = lag_bulan

    # =========================
    # HANDLE WHO (IDEOLOGI)
    # =========================
    if "who" not in data or not isinstance(data["who"], dict):
        data["who"] = {}

    ideology_raw = data["who"].get("defendant_ideology_affiliation")
    data["who"]["defendant_ideology_affiliation"] = normalize_ideology(ideology_raw)

    raw_network = data["who"].get("defendant_local_network")


    what_obj = data.get("what", {})
    activity_keys = ['pelatihan_activities', 'perencanaan_activities', 'tindakan_activities']
    
    filename = data.get("file_name", data.get("what", {}).get("case_number", ""))

    prov, kota = extract_location(raw_network)
    source = 'local_network'

    # TAHAP 2: Jika dari local_network gagal, cari langsung di ketiga aktivitas
    if prov is None:
        for act_key in activity_keys:
            # PERBAIKAN: Gunakan what_obj, bukan data
            if act_key in what_obj and isinstance(what_obj[act_key], list):
                for item in what_obj[act_key]:
                    if isinstance(item, dict) and 'location' in item:
                        prov, kota = extract_location(item['location'])
                        if prov:
                            source = act_key 
                            break
                if prov: break
            if prov: break

    # TAHAP 3: Tebak dari nama file
    if prov is None:
        prov, kota = extract_province_from_filename(filename)
        if prov:
            source = 'file_name'

    # TAHAP 4: Mentok
    if prov is None:
        prov = 'Unknown'
        kota = 'Unknown'
        source = 'tidak_diketahui'

    # TAHAP 5: Tambal Sulam Kota (Upgrade kota jika kosong)
    if prov not in ('Unknown', 'Luar Negeri') and (kota is None or str(kota).strip() == ''):
        kota_found = False

        for act_key in activity_keys:
            # PERBAIKAN: Gunakan what_obj, bukan data
            if act_key in what_obj and isinstance(what_obj[act_key], list):
                for item in what_obj[act_key]:
                    if isinstance(item, dict) and 'location' in item:
                        # Obrak-abrik teks location ini mencari nama kota Banten
                        k = extract_kota_from_activity_for_province(item['location'], prov)
                        if k:
                            kota = k
                            nama_sumber_bersih = act_key.replace('_activities', '')
                            source = f"{source}+kota_dari_{nama_sumber_bersih}"
                            kota_found = True
                            break
                if kota_found: break
            if kota_found: break
            
        if not kota_found:
            kota = 'tidak disebutkan'

    # Simpan hasil akhir ke dalam JSON
    data["who"]["provinsi_jaringan"] = prov
    data["who"]["kota_jaringan"] = kota

    if "why" not in data or not isinstance(data["why"], dict):
        data["why"] = {}

    raw_rad_sources = data["why"].get("radicalization_sources", [])
    data["why"]["classified_radicalization_sources"] = process_and_classify_list(raw_rad_sources, normalize_radicalization_sources) 

    classified_sources = data["why"]["classified_radicalization_sources"]
    channel_result = classify_radicalization_channel(classified_sources)
    if channel_result not in ["Data Tidak Lengkap", "Lainnya", "Tidak Diketahui"]:
        data["why"]["radicalization_channel"] = channel_result
    else:
        data["why"]["radicalization_channel"] = "Tidak Diketahui"

    raw_motivation = data["why"].get("motivation_factors", [])
    data["why"]["classified_motivation_factors"] = process_and_classify_list(raw_motivation, classify_motivation)

    raw_aggravating = data["why"].get("aggravating_factors", [])
    data["why"]["classified_aggravating_factors"] = process_and_classify_list(raw_aggravating, classify_aggravating)

    if "what" not in data or not isinstance(data["what"], dict):
        data["what"] = {}

    defense_counsels = data["who"].get("defense_counsels")
    judges = data["who"].get("judges")
    clerk = data["who"].get("clerk")
    prosecutors = data["who"].get("prosecutors")

    data["who"]["normalized_defense_counsels"] = extract_clean_officials(defense_counsels)
    data["who"]["normalized_judges"] = extract_clean_officials(judges)
    data["who"]["normalized_clerk"] = extract_clean_officials(clerk)
    data["who"]["normalized_prosecutors"] = extract_clean_officials(prosecutors)

    data = normalize_evidence_items(data)
    data = normalize_charged_articles(data)
    data = normalize_people_names(data)
    data = normalize_court_info(data)
    data = normalize_case_data(data)
    data = clean_monetary_value(data)

    when_obj = data.get("when", {})
    raw_joined = when_obj.get("defendant_local_network_joined_at")
    when_obj["normalized_joined_at"] = normalized_joined_date(raw_joined)

    who_obj = data.get("who", {})
    raw_local_network = who_obj.get("defendant_local_network", "")
    who_obj["normalized_local_network"] = halaman_remove_local_network(raw_local_network)
    
    return data


# =========================
# MAIN LOOP
# =========================
def main():
    total = 0

    for root, _, files in os.walk(INPUT_FOLDER):
        for file in files:
            if file.endswith(".json"):
                input_path = os.path.join(root, file)

                # mirror folder structure
                relative_path = os.path.relpath(root, INPUT_FOLDER)
                output_dir = os.path.join(OUTPUT_FOLDER, relative_path)
                os.makedirs(output_dir, exist_ok=True)

                output_path = os.path.join(output_dir, file)

                try:
                    with open(input_path, "r", encoding="utf-8") as f:
                        data = json.load(f)

                    # 1. Jalankan proses original kamu
                    data = process_file(data)
                    
                    # 2. Jalankan proses penambahan bulan & tahun aktivitas
                    data = enrich_activities(data)

                    data = extract_time_detention(data)

                    with open(output_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)

                    total += 1

                except Exception as e:
                    print(f"❌ Error di {file}: {e}")

    print(f"✅ Selesai. Total file diproses: {total}")


if __name__ == "__main__":
    main()