import os
import json
import pandas as pd
import re

# =========================
# CONFIG
# =========================
INPUT_FOLDER = "../dataset/result/output_loc-v1/output"
OUTPUT_FOLDER = "clean_json"
KAMUS_LOKASI = "lokasi.json"

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

try:
    with open(KAMUS_LOKASI, 'r', encoding='utf-8') as f:
        kamus_data = json.load(f)
        
    TYPO_FIX = kamus_data.get("TYPO_FIX", {})
    CITY_TO_PROVINCE = kamus_data.get("CITY_TO_PROVINCE", {})
    
    FOREIGN_LOCATIONS = set(kamus_data.get("FOREIGN_LOCATIONS", []))
    
    MANUAL_MAP = {k: tuple(v) for k, v in kamus_data.get("MANUAL_MAP", {}).items()}

    PROVINCE_NAMES = {v.lower() for v in CITY_TO_PROVINCE.values()} | {'kaltim', 'sumatra utara'}

    print("Kamus lokasi berhasil dimuat!")

except Exception as e:
    print("Gagal memuat kamus lokasi: ", e)
    exit()

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

    cleaned = clean_halaman(raw_text)
    cleaned = fix_typos(cleaned)

    if cleaned.lower() in FOREIGN_LOCATIONS:
        return 'Luar Negeri', cleaned

    key = cleaned.lower()
    if key in MANUAL_MAP:
        return MANUAL_MAP[key]

    for city, prov in CITY_TO_PROVINCE.items():
        if cleaned.lower() == city.lower():
            kab = '' if cleaned.lower() in PROVINCE_NAMES else cleaned
            return prov, kab

    for city, prov in CITY_TO_PROVINCE.items():
        if city.lower() not in PROVINCE_NAMES: 
            pattern = r'\b' + re.escape(city) + r'\b'
            if re.search(pattern, cleaned, flags=re.IGNORECASE):
                return prov, city
                
    for prov_name in PROVINCE_NAMES:
        if re.search(r'\b' + re.escape(prov_name) + r'\b', cleaned, flags=re.IGNORECASE):
            for k, v in CITY_TO_PROVINCE.items():
                if v.lower() == prov_name:
                    return v, None 

    return None, None

def enrich_activities(obj):
    activity_keys = ['pelatihan_activities', 'perencanaan_activities', 'tindakan_activities']
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in activity_keys and isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        if 'time' in item:
                            time_text = item['time']
                            item['estimated_year'] = extract_year(time_text)
                            item['estimated_month'] = extract_month(time_text)
                        if 'location' in item:
                            loc_text = item['location']
                            prov, kota = extract_location(loc_text)
                            item['estimated_province'] = prov
                            item['estimated_city'] = kota
            else:
                enrich_activities(v)
    elif isinstance(obj, list):
        for item in obj:
            enrich_activities(item)
    return obj


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