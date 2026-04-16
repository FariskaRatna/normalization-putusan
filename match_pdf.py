import os
import shutil
import pandas as pd

excel_file = "unknown_mitigating.xlsx"
pdf_root_folder = "../dataset/terrorism"
output_folder = "unknown_mitigating_pdfs"

os.makedirs(output_folder, exist_ok=True)

# 1. Baca Excel
df = pd.read_excel(excel_file)

# 2. Ambil nama file dan ubah ke .pdf
pdf_names = {
    str(name).replace(".json", ".pdf").strip()
    for name in df["File_Name"].dropna()
}

# Optional: kalau mau cocok berdasarkan nama tanpa ekstensi
pdf_names_no_ext = {
    os.path.splitext(name)[0] for name in pdf_names
}

# 3. Loop semua PDF
for root, dirs, files in os.walk(pdf_root_folder):
    for file in files:
        if file.endswith(".pdf"):
            pdf_name_no_ext = os.path.splitext(file)[0]

            if pdf_name_no_ext in pdf_names_no_ext:
                src = os.path.join(root, file)
                dst = os.path.join(output_folder, file)

                shutil.copy2(src, dst)
                print(f"Copied: {file}")

print("Selesai! Semua PDF yang match dari Excel sudah dikumpulkan.")