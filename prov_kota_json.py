import pandas as pd
import json
import re

# ==========================================
# 1. BACA SEMUA LEVEL CSV
# ==========================================
# dtype=str wajib agar ID '11.01.01.2002' tidak rusak jadi angka desimal
df_prov = pd.read_csv('provinsi.csv', dtype=str)
df_kota = pd.read_csv('kabupaten_kota.csv', dtype=str)
df_kec  = pd.read_csv('kecamatan.csv', dtype=str)
df_kel  = pd.read_csv('kelurahan.csv', dtype=str)

# ==========================================
# 2. BUAT DICTIONARY REFERENSI DASAR
# ==========================================
# Untuk mencari ID dengan cepat
prov_map = dict(zip(df_prov['id'], df_prov['name']))
kota_map = dict(zip(df_kota['id'], df_kota['name']))

MASTER_WILAYAH = {}

def clean_name(name, level):
    name = str(name)
    if level == 'kota':
        name = re.sub(r'^(Kab\.\s*|Kabupaten\s*|Kota\s*)', '', name, flags=re.IGNORECASE)
    elif level == 'kec':
        name = re.sub(r'^(Kec\.\s*|Kecamatan\s*)', '', name, flags=re.IGNORECASE)
    elif level == 'kel':
        name = re.sub(r'^(Desa\s*|Kelurahan\s*|Kel\.\s*|Gampong\s*|Kampung\s*)', '', name, flags=re.IGNORECASE)
    return name.strip().title()

def get_prov_and_kota(full_id):
    """Mengekstrak nama Provinsi dan Kota dari ID panjang"""
    parts = full_id.split('.')
    prov_id = parts[0]                # Contoh: '11'
    kota_id = f"{parts[0]}.{parts[1]}" # Contoh: '11.01'
    
    prov_name = prov_map.get(prov_id, "Unknown")
    kota_name = clean_name(kota_map.get(kota_id, "Unknown"), 'kota')
    return prov_name, kota_name

# ==========================================
# 3. MASUKKAN DATA KE MASTER WILAYAH
# ==========================================

# A. Masukkan Level Kota
for _, row in df_kota.iterrows():
    prov, kota = get_prov_and_kota(row['id'])
    MASTER_WILAYAH[kota.lower()] = {"prov": prov, "kota": kota}

# B. Masukkan Level Kecamatan
for _, row in df_kec.iterrows():
    prov, kota = get_prov_and_kota(row['id'])
    kecamatan = clean_name(row['name'], 'kec')
    
    # Masukkan ke kamus jika belum ada
    if kecamatan.lower() not in MASTER_WILAYAH:
        MASTER_WILAYAH[kecamatan.lower()] = {"prov": prov, "kota": kota}

# C. Masukkan Level Kelurahan/Desa
for _, row in df_kel.iterrows():
    prov, kota = get_prov_and_kota(row['id'])
    raw_kel = str(row['name'])
    
    # Cek apakah ada nama alternatif di dalam kurung (cth: "Ujong Mangki (Ujung Mangki)")
    match = re.match(r'^(.*?)\s*\((.*?)\)', raw_kel)
    
    if match:
        kel_1 = clean_name(match.group(1), 'kel') # Ujong Mangki
        kel_2 = clean_name(match.group(2), 'kel') # Ujung Mangki
        
        if kel_1.lower() not in MASTER_WILAYAH:
            MASTER_WILAYAH[kel_1.lower()] = {"prov": prov, "kota": kota}
        if kel_2.lower() not in MASTER_WILAYAH:
            MASTER_WILAYAH[kel_2.lower()] = {"prov": prov, "kota": kota}
    else:
        kel = clean_name(raw_kel, 'kel')
        if kel.lower() not in MASTER_WILAYAH:
            MASTER_WILAYAH[kel.lower()] = {"prov": prov, "kota": kota}

# ==========================================
# 4. SIMPAN KE JSON
# ==========================================
with open('kamus_super_wilayah.json', 'w', encoding='utf-8') as f:
    json.dump(MASTER_WILAYAH, f, indent=4, ensure_ascii=False)

print(f"✅ Selesai! {len(MASTER_WILAYAH)} wilayah berhasil digabungkan menjadi JSON.")