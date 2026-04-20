import os
import shutil
import pandas as pd

excel_file = "different-charge-articles.xlsx"
json_root_folder = "output/"
output_folder = "no-charge/"

os.makedirs(output_folder, exist_ok=True)

# 1. Baca Excel
df = pd.read_excel(excel_file)

# 2. Ambil nama file dari kolom File_Name
target_names = {
    str(name).lower().replace(".json", "").strip()
    for name in df["File_Name"].dropna()
}

# 3. Loop semua JSON di folder
for root, dirs, files in os.walk(json_root_folder):
    for file in files:
        if file.endswith(".json"):
            json_name = os.path.splitext(file)[0].lower().strip()

            if json_name in target_names:
                src = os.path.join(root, file)
                dst = os.path.join(output_folder, file)

                shutil.move(src, dst)
                print(f"Moved: {file}")

print("Selesai! Semua JSON sudah dipindahkan.")