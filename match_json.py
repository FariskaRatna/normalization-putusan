import os
import shutil

json_folder = "../dataset/result/output_loc-v1/output"
pdf_root_folder = "unknown_mitigating_pdfs"
output_folder = "matched_json_mitigating"

os.makedirs(output_folder, exist_ok=True)

# 1. Ambil semua nama PDF (tanpa ekstensi)
pdf_names = set()

for root, dirs, files in os.walk(pdf_root_folder):
    for file in files:
        if file.endswith(".pdf"):
            pdf_name = os.path.splitext(file)[0]
            pdf_names.add(pdf_name)

# 2. Loop semua JSON dan cocokkan
for file in os.listdir(json_folder):
    if file.endswith(".json"):
        json_name = os.path.splitext(file)[0]

        if json_name in pdf_names:
            src = os.path.join(json_folder, file)
            dst = os.path.join(output_folder, file)

            shutil.copy2(src, dst)
            print(f"Copied: {file}")

print("Selesai! Semua JSON yang match dengan PDF sudah dikumpulkan.")