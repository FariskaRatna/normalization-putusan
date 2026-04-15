import os
import json
import pandas as pd
import re

# =========================
# CONFIG
# =========================
INPUT_FOLDER = "../dataset/result/output_loc-v1/output"
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

        words = re.findall(r'\b\w+\b', text_lower)
        for n in [4, 3, 2]: 
            if len(words) >= n:
                for i in range(len(words) - n + 1):
                    phrase = " ".join(words[i:i+n])
                    if phrase in MASTER_WILAYAH:
                        # Langsung return jika ketemu kelurahan yang terdaftar
                        return MASTER_WILAYAH[phrase]["prov"], MASTER_WILAYAH[phrase]["kota"]

        for city, prov in CITY_TO_PROVINCE.items():
            if text_lower == city:
                kab = '' if text_lower in PROVINCE_NAMES else text_to_search.title()
                return prov, kab

        for city, prov in CITY_TO_PROVINCE.items():
            if city not in PROVINCE_NAMES: 
                if re.search(r'\b' + re.escape(city) + r'\b', text_lower):
                    return prov, city.title()
                
        for prov_name in PROVINCE_NAMES:
            if re.search(r'\b' + re.escape(prov_name) + r'\b', text_lower):
                for k, v in CITY_TO_PROVINCE.items():
                    if v.lower() == prov_name:
                        return v, None
                return prov_name.title(), None
    
        return None, None
    
    prov, kota = search_in_text(cleaned_main)
    if prov is None and wilayah_text:
        cleaned_wilayah = fix_typos(re.sub(r'\s*\(Halaman\s*[\d]+\)', '', wilayah_text, flags=re.IGNORECASE).strip())
        prov, kota = search_in_text(cleaned_wilayah)
    
    return prov, kota

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


# =========================
# FUNGSI LAMA (TIDAK DIUBAH SAMA SEKALI)
# =========================
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