import os
import json

# =========================
# CONFIG
# =========================
# Folder sumber data mentah Anda
INPUT_FOLDER = "output" 

# Folder baru khusus untuk menyimpan hasil imputasi (sebelum dinormalisasi)
OUTPUT_FOLDER = "imputed_json" 

def impute_missing_articles(data):
    """
    Modul untuk menyuntikkan (imputasi) data default ke file JSON 
    yang tidak memiliki nilai charged_articles.
    """
    default_articles = [
        "Pasal 15 jo Pasal 7 Peraturan Pemerintah Pengganti Undang-Undang Nomor 1 Tahun 2002 Tentang Pemberantasan Tindak Pidana Terorisme",
        "Undang-Undang Nomor 15 Tahun 2003"
    ]

    if "what" not in data or not isinstance(data["what"], dict):
        data["what"] = {}

    raw_articles = data["what"].get("charged_articles", [])

    if not raw_articles:
        raw_articles = data.get("charged_articles", [])

    # LAKUKAN IMPUTASI JIKA KOSONG
    if not raw_articles or not isinstance(raw_articles, list) or len(raw_articles) == 0:
        data["what"]["charged_articles"] = default_articles
        data["what"]["is_imputed_articles"] = True
        # print(f"  [+] Imputasi dilakukan pada kasus: {data.get('case_id', 'Unknown')}")
    else:
        # data["what"]["is_imputed_articles"] = False
        pass

    return data

# =========================
# MAIN LOOP
# =========================
def main():
    total_files = 0
    total_imputed = 0

    print(f"Memulai proses imputasi dari folder: {INPUT_FOLDER}...")

    for root, _, files in os.walk(INPUT_FOLDER):
        for file in files:
            if file.endswith(".json"):
                input_path = os.path.join(root, file)

                # Mirror struktur folder
                relative_path = os.path.relpath(root, INPUT_FOLDER)
                output_dir = os.path.join(OUTPUT_FOLDER, relative_path)
                os.makedirs(output_dir, exist_ok=True)

                output_path = os.path.join(output_dir, file)

                try:
                    with open(input_path, "r", encoding="utf-8") as f:
                        data = json.load(f)

                    # Simpan status awal untuk cek apakah terjadi imputasi
                    awal_kosong = not data.get("what", {}).get("charged_articles") and not data.get("charged_articles")

                    # Jalankan fungsi imputasi
                    data = impute_missing_articles(data)

                    # Jika diimputasi, tambah counter
                    if data.get("what", {}).get("is_imputed_articles"):
                        total_imputed += 1

                    # Simpan hasilnya ke folder imputed_json
                    with open(output_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)

                    total_files += 1

                except Exception as e:
                    print(f"❌ Error memproses {file}: {e}")

    print("\n" + "="*30)
    print("PROSES IMPUTASI SELESAI")
    print("="*30)
    print(f"Total file diproses : {total_files}")
    print(f"Total file diimputasi: {total_imputed}")
    print(f"Hasil tersimpan di   : {OUTPUT_FOLDER}/")

if __name__ == "__main__":
    main()