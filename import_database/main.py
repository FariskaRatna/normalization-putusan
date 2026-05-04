import os
import glob
from case_entities import import_case_entities
from extract_province import import_provinces
from article_charges import import_article_charges
from case_evidence import import_case_evidence
from persons import import_persons
from co_defendants import import_co_defendants
from officials import import_officials_to_persons
from person_documents import import_person_documents
from cities import import_cities
# from location_from_activities import import_locations_from_activities
from court import import_court
from cases import import_cases
from classified_master import import_classified_master
from aggravating_factors import import_aggravating_factors
from motivation_factors import import_motivation_factors
from radicalization_source import import_radicalization_source
from mitigating_factors import import_mitigating_factors
from detention_timeline import import_detention_timeline
from case_charge_articles import import_case_charge_articles
from monetary_amount import import_monetary_amounts
from case_people import import_case_people
from case_defendant_details import import_case_defendant_details
from case_activities import import_case_activities

def run_master_pipeline(folder_path):
    json_files = glob.glob(os.path.join(folder_path, "*.json"))
    
    if not json_files:
        print(f"⚠️ Tidak ada file JSON yang ditemukan di folder: {folder_path}")
        return

    print(f"🚀 MEMULAI PIPELINE: Ditemukan {len(json_files)} dokumen.\n" + "="*50)

    for index, json_path in enumerate(json_files, start=1):
        nama_file = os.path.basename(json_path)
        print(f"\n[{index}/{len(json_files)}] ⏳ Memproses: {nama_file}")
        
        try:
            # ==========================================
            # LEVEL 1: TABEL MASTER ABSOLUT (Pondasi)
            # ==========================================
            import_provinces(json_path)
            import_court(json_path)
            import_persons(json_path)
            import_officials_to_persons(json_path) 
            # import_case_entities(json_path)
            import_classified_master(json_path)
            import_article_charges(json_path)

            # ==========================================
            # LEVEL 2: TABEL SUB-MASTER (Relasi Dasar)
            # ==========================================
            import_cities(json_path)             
            import_person_documents(json_path)   

            import_cases(json_path)               

        
            import_case_people(json_path)                   
            import_case_defendant_details(json_path)        
            import_co_defendants(json_path)                 
            import_case_entities(json_path)               
            import_case_charge_articles(json_path)        
            
            # import_locations_from_activities(json_path) 
            import_case_activities(json_path)            
            
            import_aggravating_factors(json_path)           
            import_motivation_factors(json_path)            
            import_radicalization_source(json_path)         
            import_mitigating_factors(json_path)            
            
            import_case_evidence(json_path)              
            import_monetary_amounts(json_path)           
            import_detention_timeline(json_path)         
            
            print(f"✅ SUKSES: Semua data dari {nama_file} berhasil diimpor.")
            
        except Exception as e:
            print(f"❌ ERROR FATAL pada {nama_file}: {e}")
            continue

    print("\n" + "="*50)
    print("SEMUA PROSES PIPELINE SELESAI! 🎉")

if __name__ == "__main__":
    TARGET_FOLDER = "./clean-json" 
    run_master_pipeline(TARGET_FOLDER)