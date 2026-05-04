import json
import psycopg2
import os
from psycopg2.extras import execute_values, RealDictCursor

def get_city_id(cursor, city_name):
    if not city_name or city_name == "Tidak Diketahui":
        return None
    
    query = "SELECT id FROM cities WHERE city_name = %s LIMIT 1"
    cursor.execute(query, (city_name,))
    result = cursor.fetchone()

    return result['id'] if result else None

def get_court_id(cursor, court_name):
    if not court_name or court_name == "Tidak Diketahui":
        return None
    
    query = "SELECT id FROM courts WHERE court_name = %s LIMIT 1"
    cursor.execute(query, (court_name,))
    result = cursor.fetchone()

    return result['id'] if result else None

def get_case_id(cursor, case):
    if not case or case == "Tidak Diketahui":
        return None
    
    query = "SELECT id FROM cases WHERE case_number = %s LIMIT 1"
    cursor.execute(query, (case,))
    result = cursor.fetchone()

    return result[0] if result else None

def get_classified_id(cursor, classified):
    if not classified:
        return []
    
    if isinstance(classified, str):
        classified = [classified]

    ids = []

    for cls in classified:
        cursor.execute("SELECT id FROM classified_factors_source WHERE LOWER(TRIM(classified_name)) = LOWER(trim(%s)) LIMIT 1", (cls,))
        result = cursor.fetchone()
        if result:
            ids.append(result[0])
        else:
            print(f"TIDAK DITEMUKAN: {cls}")

        return ids
    
def get_legal_id(cursor, article_text):
    if not article_text or article_text == "Tidak Diketahui":
        return None
    
    query = "SELECT id FROM legal_articles WHERE article_text = %s LIMIT 1"
    cursor.execute(query, (article_text,))
    result = cursor.fetchone()

    return result[0] if result else None

def get_person_id(cursor, person_name):
    """Mencari ID orang dari tabel master berdasarkan nama."""
    if not person_name:
        return None
    cursor.execute("SELECT id FROM public.persons WHERE fullname = %s LIMIT 1", (person_name.strip(),))
    result = cursor.fetchone()
    return result[0] if result else None