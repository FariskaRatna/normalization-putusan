import json
import os
import glob
import re

def remove_halaman_only(text):
    if not isinstance(text, str):
        return text
    
    cleaned = re.sub(r'(?i)\s*\(\s*HALAMAN\s+\d+\s*\)', '', text)
    return re.sub(r'\s+', ' ', cleaned).strip()

def process_activities_only(data):
    if isinstance(data, dict) and 'what' in data:
        target_keys = ['perencanaan_activities', 'pelatihan_activities', 'tindakan_activities']
        
        for key in target_keys:
            if key in data['what']:
                activities = data['what'][key]
                
                if isinstance(activities, list):
                    for item in activities:
                        if isinstance(item, dict) and 'location' in item:
                            item['location'] = remove_halaman_only(item['location'])
                        elif isinstance(item, str):
                            idx = activities.index(item)
                            activities[idx] = remove_halaman_only(item)
    return data

def process_all_files(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)
    
    # Ambil semua file berakhiran .json di folder input
    json_files = glob.glob(os.path.join(input_folder, '*.json'))
    
    if not json_files:
        print(f"Tidak ditemukan file JSON di dalam folder {input_folder}")
        return

    print(f"Ditemukan {len(json_files)} file JSON. Memulai pembersihan...")
    
    for file_path in json_files:
        filename = os.path.basename(file_path)
        
        # Baca file JSON
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                print(f"❌ Error membaca {filename}: Format JSON tidak valid.")
                continue
        
        # Bersihkan data
        cleaned_data = process_activities_only(data)
        
        # Simpan file yang sudah bersih ke folder output
        output_path = os.path.join(output_folder, filename)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(cleaned_data, f, indent=4, ensure_ascii=False)
            
        print(f"Selesai dinormalisasi: {filename}")

if __name__ == "__main__":
    INPUT_DIR = '../dataset/result/output_loc-v1/output'     
    OUTPUT_DIR = './cleaned_json_data' 
    
    process_all_files(INPUT_DIR, OUTPUT_DIR)