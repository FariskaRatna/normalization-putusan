import os
import shutil
import pandas as pd

excel_file = "narkotika.xlsx"
pdf_root_folder = "no-terroris/"
output_folder = "narkotika/"

os.makedirs(output_folder, exist_ok=True)

# 1. Baca Excel
df = pd.read_excel(excel_file)

# 2. Ambil nama file dari kolom File_Name
# ubah .json → hilangkan ekstensi → jadi nama dasar
target_names = {
    str(name).lower().replace(".json", "").strip()
    for name in df["File_Name"].dropna()
}

# 3. Loop semua PDF di folder (termasuk subfolder)
for root, dirs, files in os.walk(pdf_root_folder):
    for file in files:
        if file.endswith(".pdf"):
            pdf_name = os.path.splitext(file)[0].lower().strip()

            if pdf_name in target_names:
                src = os.path.join(root, file)
                dst = os.path.join(output_folder, file)

                shutil.move(src, dst)  # <-- ini yang membedakan (MOVE, bukan COPY)
                print(f"Moved: {file}")

print("Selesai! Semua PDF sudah dipindahkan sesuai Excel.")