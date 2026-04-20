import os
import json
import pandas as pd
import re

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

def normalize_court_info(data):
    where_block = data.get("where", {})
    
    raw_court_name = where_block.get("district_court", "")
    
    if raw_court_name and raw_court_name != "Tidak Diketahui":
        prov, kota = extract_location(raw_court_name)
        
        # Kita timpa atau isi field hasil normalisasi
        where_block["normalized_court_province"] = prov
        where_block["normalized_court_city"] = kota
        
        where_block["court_location"] = kota 
    else:
        if "normalized_court_province" not in where_block:
            where_block["normalized_court_province"] = "Tidak Diketahui"
        if "normalized_court_city" not in where_block:
            where_block["normalized_court_city"] = "Tidak Diketahui"

    data["where"] = where_block
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

def classify_source(x):
    if not x or str(x).strip().lower() in ["", "unknown", "tidak diketahui"]:
        return "Data Tidak Lengkap"
    
    x = str(x).lower()
    if any(k in x for k in ["video","isis","daulah","peperangan","eksekusi","rilisan","youtube"]):
        return "Video/Konten Digital ISIS"
    if any(k in x for k in ["facebook","whatsapp","telegram","group","grup","channel",
                              "media sosial","medsos","akun","instagram","website","internet","online"]):
        return "Media Sosial/Chat"
    if any(k in x for k in ["kajian","pengajian","ustadz","ceramah","majelis","halaqah",
                              "tarbiyah","taklim","tabligh","dakwah","tauhid","jihad","hijrah"]):
        return "Kajian/Pengajian Informal"
    if any(k in x for k in ["pesantren","ponpes","yayasan","pondok"]):
        return "Pondok/Yayasan"
    if any(k in x for k in ["buku","materi","cd","audio","majalah","artikel"]):
        return "Materi Fisik"
    if any(k in x for k in ["pengaruh","komunikasi","keluarga","baiat","amir","jamaah","kelompok","mit","ji"]):
        return "Pengaruh/Jaringan"
    
    return "Lainnya"

def classify_radicalization_channel(raw_sources):
    if not raw_sources:
        return "Data Tidak Lengkap"
    
    if isinstance(raw_sources, list):
        gabungan_teks = " ".join([str(item) for item in raw_sources]).lower()
    else:
        gabungan_teks = str(raw_sources).lower()
        
    if gabungan_teks.strip() in ["", "unknown", "tidak diketahui"]:
        return "Data Tidak Lengkap"
    
    DIGITAL_KW = [
        "facebook","whatsapp","telegram","grup","group","channel",
        "media sosial","medsos","youtube","video","internet","online","akun", "digital"
    ]

    OFFLINE_KW = [
        "kajian","pengajian","ustadz","ceramah","majelis","pesantren",
        "pondok","tatap muka","langsung","halaqah", "masjid"
    ]

    has_d = any(k in gabungan_teks for k in DIGITAL_KW)
    has_o = any(k in gabungan_teks for k in OFFLINE_KW)

    if has_d and has_o:
        return "Hybrid"
    elif has_d:
        return "Online"
    elif has_o:
        return "Offline"
    
    return "Lainnya"


def classify_motivation(x):
    if not x or str(x).strip().lower() in ["", "unknown", "tidak diketahui"]:
        return "Data Tidak Lengkap"
    
    x = str(x).lower()
    if any(k in x for k in ["syariat","syari'at","islam","agama","daulah","khilafah"]):
        return "Tegaknya Syariat/Daulah Islam"
    if any(k in x for k in ["hijrah","bergabung isis","bergabung daulah"]):
        return "Hijrah ke Daulah"
    if any(k in x for k in ["jihad","berjihad","mujahid"]):
        return "Jihad Fisik"
    if any(k in x for k in ["balas","dendam","sakit hati"]):
        return "Balas Dendam"
    if any(k in x for k in ["organisasi","kelompok","jaringan","loyalitas"]):
        return "Loyalitas Organisasi/Kelompok"
    if any(k in x for k in ["tidak puas","antipati","pemerintah","kebijakan"]):
        return "Antipati terhadap Pemerintah"
    if any(k in x for k in ["menyesal","ikut-ikut","pengaruh","terpaksa"]):
        return "Pengaruh Eksternal"
    
    return "Lainnya"

def classify_aggravating(x):
    if not x or str(x).strip().lower() in ["", "unknown", "tidak diketahui"]:
        return "Data Tidak Lengkap"
    
    x = str(x).lower()
    if any(k in x for k in ["keresahan","ketertiban","masyarakat"]):
        return "Keresahan Masyarakat"
    if any(k in x for k in ["korban massal","massa","banyak korban"]):
        return "Potensi Korban Massal"
    if any(k in x for k in ["dampak","meluas","luas","nasional","internasional"]):
        return "Dampak Meluas"
    if any(k in x for k in ["berencana","terencana","sistematis"]):
        return "Terencana/Sistematis"
    
    return "Lainnya"

def process_and_classify_list(raw_list, classify_func):
    if not raw_list:
        return []
    
    if isinstance(raw_list, str):
        raw_list = [raw_list]

    categories  = set()
    for source in raw_list:
        parts = str(source).split("\n---\n")
        for part in parts:
            part_clean = part.strip()

            if len(part_clean) > 3:
                kategori = classify_func(part_clean)
                if kategori not in ["Data Tidak Lengkap"]:
                    categories.add(kategori)

    return sorted(list(categories))

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

import re

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
    # Regex khusus gelar pakai kurung seperti CLL (S). Tidak pakai \b di akhir karena tutup kurung bukan karakter kata.
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
    data["why"]["classified_radicalization_sources"] = process_and_classify_list(raw_rad_sources, classify_source) 

    channel_result = classify_radicalization_channel(raw_rad_sources)
    if channel_result not in ["Data Tidak Lengkap", "Lainnya"]:
        data["why"]["radicalization_channel"] = channel_result
    else:
        data["why"]["radicalization_channel"] = ""

    raw_motivation = data["why"].get("motivation_factors", [])
    data["why"]["classified_motivation_factors"] = process_and_classify_list(raw_motivation, classify_motivation)

    raw_aggravating = data["why"].get("aggravating_factors", [])
    data["why"]["classified_aggravating_factors"] = process_and_classify_list(raw_aggravating, classify_aggravating)

    if "what" not in data or not isinstance(data["what"], dict):
        data["what"] = {}

    raw_perencanaan = data["what"].get("perencanaan_activities", [])
    raw_pelatihan = data["what"].get("pelatihan_activities", [])
    raw_tindakan = data["what"].get("tindakan_activities", [])

    # data["what"]["enriched_perencanaan"] = process_temporal_list(raw_perencanaan)
    # data["what"]["enriched_pelatihan"] = process_temporal_list(raw_pelatihan)
    # data["what"]["enriched_tindakan"] = process_temporal_list(raw_tindakan)

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

                    with open(output_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)

                    total += 1

                except Exception as e:
                    print(f"❌ Error di {file}: {e}")

    print(f"✅ Selesai. Total file diproses: {total}")


if __name__ == "__main__":
    main()