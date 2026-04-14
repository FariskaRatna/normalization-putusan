import json

# Data lengkapmu
TYPO_FIX = {
    r'\bmuara\s+ang\s*ke\b': 'Muara Angke', r'\bme\s+dan\b': 'Medan',
    r'\bjogja\b': 'Yogyakarta', r'\byogjakarta\b': 'Yogyakarta',
    r'\btanggerang\b': 'Tangerang', r'\bmakasar\b': 'Makassar',
    r'\btulung\s+agung\b': 'Tulungagung', r'\bkaranganyer\b': 'Karanganyar',
    r'\bbandung\s+tengah\b': 'Bandung', r'\bdenpasar\s+(utara|barat)\b': 'Denpasar',
    r'\bsolo\s+utara\b': 'Solo', r'\bjkt\b': 'Jakarta',
    r'\bpayakumbu\b': 'Payakumbuh', r'\bbukit\s+tinggi\b': 'Bukittinggi',
    r'\bsumatra\b': 'Sumatera',
}

CITY_TO_PROVINCE = {
    'Jakarta': 'DKI Jakarta', 'Jakarta Barat': 'DKI Jakarta', 'Jakarta Selatan': 'DKI Jakarta',
    'Jakarta Timur': 'DKI Jakarta', 'Jakarta Utara': 'DKI Jakarta', 'DKI Jakarta': 'DKI Jakarta',
    'Muara Angke': 'DKI Jakarta', 'Cipayung': 'DKI Jakarta',
    'Banten': 'Banten', 'Tangerang': 'Banten', 'Tangerang Selatan': 'Banten',
    'Kota Tangerang': 'Banten', 'Cilegon': 'Banten', 'Pamulang': 'Banten',
    'Banten Utara': 'Banten', 'Jati Uwung': 'Banten', 'Ciledug': 'Banten',
    'Serang': 'Banten', 'Pandeglang': 'Banten', 'Lebak': 'Banten',
    'Rangkasbitung': 'Banten', 'Serpong': 'Banten', 'Kragilan': 'Banten',
    'Tigaraksa': 'Banten',
    'Jawa Barat': 'Jawa Barat', 'Bandung': 'Jawa Barat', 'Bandung Barat': 'Jawa Barat',
    'Bekasi': 'Jawa Barat', 'Bogor': 'Jawa Barat', 'Cianjur': 'Jawa Barat',
    'Cirebon': 'Jawa Barat', 'Kota Cirebon': 'Jawa Barat', 'Depok': 'Jawa Barat',
    'Indramayu': 'Jawa Barat', 'Sukabumi': 'Jawa Barat', 'Tasikmalaya': 'Jawa Barat',
    'Pangalengan': 'Jawa Barat', 'Kota Bogor': 'Jawa Barat', 'Karawang': 'Jawa Barat',
    'Sumedang': 'Jawa Barat', 'Majalengka': 'Jawa Barat', 'Kuningan': 'Jawa Barat',
    'Purwakarta': 'Jawa Barat', 'Ciamis': 'Jawa Barat', 'Subang': 'Jawa Barat',
    'Garut': 'Jawa Barat', 'Cikampek': 'Jawa Barat', 'Ujung Berung': 'Jawa Barat',
    'Jawa Tengah': 'Jawa Tengah', 'Semarang': 'Jawa Tengah', 'Kota Semarang': 'Jawa Tengah',
    'Solo': 'Jawa Tengah', 'Surakarta': 'Jawa Tengah', 'Klaten': 'Jawa Tengah',
    'Sukoharjo': 'Jawa Tengah', 'Sragen': 'Jawa Tengah', 'Tegal': 'Jawa Tengah',
    'Batang': 'Jawa Tengah', 'Purbalingga': 'Jawa Tengah', 'Pemalang': 'Jawa Tengah',
    'Kendal': 'Jawa Tengah', 'Ungaran': 'Jawa Tengah', 'Cilacap': 'Jawa Tengah',
    'Nusa Kambangan': 'Jawa Tengah', 'Wonogiri': 'Jawa Tengah', 'Magelang': 'Jawa Tengah',
    'Banyumas': 'Jawa Tengah', 'Purwokerto': 'Jawa Tengah', 'Kebumen': 'Jawa Tengah',
    'Rembang': 'Jawa Tengah', 'Pati': 'Jawa Tengah', 'Kudus': 'Jawa Tengah',
    'Demak': 'Jawa Tengah', 'Temanggung': 'Jawa Tengah', 'Wonosobo': 'Jawa Tengah',
    'Boyolali': 'Jawa Tengah', 'Karanganyar': 'Jawa Tengah', 'Ambarawa': 'Jawa Tengah',
    'Purworejo': 'Jawa Tengah',
    'Yogyakarta': 'DI Yogyakarta', 'Kota Yogyakarta': 'DI Yogyakarta',
    'Kabupaten Bantul': 'DI Yogyakarta', 'Bantul': 'DI Yogyakarta',
    'Sleman': 'DI Yogyakarta', 'Gunung Kidul': 'DI Yogyakarta', 'Kulon Progo': 'DI Yogyakarta',
    'Jawa Timur': 'Jawa Timur', 'Surabaya': 'Jawa Timur', 'Malang': 'Jawa Timur',
    'Kediri': 'Jawa Timur', 'Blitar': 'Jawa Timur', 'Madiun': 'Jawa Timur',
    'Sidoarjo': 'Jawa Timur', 'Pasuruan': 'Jawa Timur', 'Probolinggo': 'Jawa Timur',
    'Lamongan': 'Jawa Timur', 'Bangil': 'Jawa Timur', 'Banyuwangi': 'Jawa Timur',
    'Jember': 'Jawa Timur', 'Lumajang': 'Jawa Timur', 'Gresik': 'Jawa Timur',
    'Mojokerto': 'Jawa Timur', 'Tulungagung': 'Jawa Timur', 'Nganjuk': 'Jawa Timur',
    'Jombang': 'Jawa Timur', 'Bojonegoro': 'Jawa Timur', 'Tuban': 'Jawa Timur',
    'Sumatera Utara': 'Sumatera Utara', 'Medan': 'Sumatera Utara',
    'Padang Sidempuan': 'Sumatera Utara', 'Padangsidempuan': 'Sumatera Utara',
    'Tanjungbalai': 'Sumatera Utara', 'Sidebudebu': 'Sumatera Utara',
    'Brastagi': 'Sumatera Utara', 'Langkat': 'Sumatera Utara', 'Stabat': 'Sumatera Utara',
    'Binjai': 'Sumatera Utara', 'Deliserdang': 'Sumatera Utara', 'Deli Serdang': 'Sumatera Utara',
    'Sibolangit': 'Sumatera Utara', 'Marelan': 'Sumatera Utara', 'Mabar': 'Sumatera Utara',
    'Sunggal': 'Sumatera Utara', 'Tembung': 'Sumatera Utara', 'Labuan': 'Sumatera Utara',
    'Sumatera Barat': 'Sumatera Barat', 'Kota Padang': 'Sumatera Barat',
    'Padang': 'Sumatera Barat', 'Payakumbuh': 'Sumatera Barat',
    'Kabupaten Tanah Datar': 'Sumatera Barat', 'Batu Sangkar': 'Sumatera Barat',
    'Bukittinggi': 'Sumatera Barat', 'Padang Panjang': 'Sumatera Barat',
    'Pariaman': 'Sumatera Barat', 'Tanah Datar': 'Sumatera Barat',
    'Agam': 'Sumatera Barat', 'Lima Puluh Kota': 'Sumatera Barat',
    'Kabupaten Agam': 'Sumatera Barat', 'Ampek Angkek': 'Sumatera Barat',
    'Riau': 'Riau', 'Pekanbaru': 'Riau', 'Dumai': 'Riau', 'Kampar': 'Riau',
    'Jambi': 'Jambi',
    'Sumatera Selatan': 'Sumatera Selatan', 'Palembang': 'Sumatera Selatan',
    'Bengkulu': 'Bengkulu',
    'Lampung': 'Lampung', 'Lampung Selatan': 'Lampung', 'Lampung Tengah': 'Lampung',
    'Bandar Lampung': 'Lampung', 'Pringsewu': 'Lampung', 'Pesawaran': 'Lampung',
    'Lampung Timur': 'Lampung', 'Lampung Utara': 'Lampung', 'Tulang Bawang': 'Lampung',
    'Metro': 'Lampung', 'Sukarame': 'Lampung', 'Kalianda': 'Lampung',
    'Gadingrejo': 'Lampung', 'Kemiling': 'Lampung',
    'Bangka Belitung': 'Kepulauan Bangka Belitung',
    'Bali': 'Bali', 'Denpasar': 'Bali', 'Bedugul': 'Bali', 'Kuta': 'Bali', 'Gianyar': 'Bali',
    'Nusa Tenggara Barat': 'Nusa Tenggara Barat', 'Bima': 'Nusa Tenggara Barat',
    'Kota Bima': 'Nusa Tenggara Barat', 'Mataram': 'Nusa Tenggara Barat',
    'Nusa Tenggara Timur': 'Nusa Tenggara Timur', 'Kupang': 'Nusa Tenggara Timur',
    'Soe': 'Nusa Tenggara Timur',
    'Kalimantan Barat': 'Kalimantan Barat', 'Pontianak': 'Kalimantan Barat',
    'Mempawah': 'Kalimantan Barat', 'Sungai Pinyuh': 'Kalimantan Barat',
    'Kalimantan Tengah': 'Kalimantan Tengah', 'Palangka Raya': 'Kalimantan Tengah',
    'Kalimantan Timur': 'Kalimantan Timur', 'Balikpapan': 'Kalimantan Timur',
    'Samarinda': 'Kalimantan Timur', 'Sangatta': 'Kalimantan Timur',
    'Sulawesi Selatan': 'Sulawesi Selatan', 'Makassar': 'Sulawesi Selatan',
    'Kota Makassar': 'Sulawesi Selatan', 'Pangkep': 'Sulawesi Selatan',
    'Mangkutana': 'Sulawesi Selatan', 'Luwu Timur': 'Sulawesi Selatan',
    'Bone': 'Sulawesi Selatan', 'Luwu': 'Sulawesi Selatan', 'Walenrang': 'Sulawesi Selatan',
    'Malili': 'Sulawesi Selatan',
    'Sulawesi Tengah': 'Sulawesi Tengah', 'Poso': 'Sulawesi Tengah', 'Palu': 'Sulawesi Tengah',
    'Kota Palu': 'Sulawesi Tengah', 'Kasimbar': 'Sulawesi Tengah',
    'Parigi Moutong': 'Sulawesi Tengah', 'Poso Pesisir': 'Sulawesi Tengah',
    'Morowali': 'Sulawesi Tengah', 'Tamanjeka': 'Sulawesi Tengah',
    'Sangginora': 'Sulawesi Tengah', 'Tangkura': 'Sulawesi Tengah',
    'Donggala': 'Sulawesi Tengah',
    'Sulawesi Barat': 'Sulawesi Barat', 'Polewali Mandar': 'Sulawesi Barat',
    'Gorontalo': 'Gorontalo', 'Pohuwato': 'Gorontalo',
    'Maluku': 'Maluku', 'Ambon': 'Maluku', 'Kabupaten Seram Bagian Barat': 'Maluku',
    'Kota Ambon': 'Maluku',
    'Maluku Utara': 'Maluku Utara', 'Tobelo': 'Maluku Utara',
    'Aceh': 'Aceh', 'Aceh Besar': 'Aceh', 'Banda Aceh': 'Aceh', 'Langsa': 'Aceh',
    'Aceh Tamiang': 'Aceh', 'Montasik': 'Aceh', 'Sabang': 'Aceh',
    'Papua': 'Papua', 'Merauke': 'Papua', 'Kabupaten Puncak': 'Papua', 'Nabire': 'Papua',
    'Papua Barat': 'Papua Barat',
}

FOREIGN_LOCATIONS = [
    'suriah', 'turki', 'filipina', 'istambul', 'istanbul',
    'marawi filipina', 'camp hudaibiyah mindanao philipina'
]

MANUAL_MAP = {
    'poso, sulawesi tengah': ['Sulawesi Tengah', 'Poso'],
    'medan, sumatera utara': ['Sumatera Utara', 'Medan']
    # Bisa tambahkan manual map lain ke format list [Provinsi, Kota] di sini jika perlu
}

# Gabungkan jadi satu dictionary
data_kamus = {
    "TYPO_FIX": TYPO_FIX,
    "CITY_TO_PROVINCE": CITY_TO_PROVINCE,
    "FOREIGN_LOCATIONS": FOREIGN_LOCATIONS,
    "MANUAL_MAP": MANUAL_MAP
}

# Tulis ke JSON
with open('kamus_lokasi.json', 'w', encoding='utf-8') as f:
    json.dump(data_kamus, f, indent=4, ensure_ascii=False)

print("✅ File kamus_lokasi.json berhasil dibuat secara lengkap!")