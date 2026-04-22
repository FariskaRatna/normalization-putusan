import os
import json
import pandas as pd
import re

def normalize_case_data(data):
    what_block = data.get("what") or {}
    how_much_block = data.get("how_much") or {}
    when_block = data.get("when") or {}

    case_number = str(what_block.get("case_number") or data.get("case_id") or "")

    raw_level = str(what_block.get("court_level", "")).lower().strip()
    process_level = "tidak diketahui"

    if raw_level and raw_level not in ["none", "", "null", "tidak diketahui"]:
        if "pertama" in raw_level: process_level = "tingkat pertama"
        elif "banding" in raw_level: process_level = "banding"
        elif "kasasi" in raw_level: process_level = "kasasi"
        elif "pk" in raw_level or "peninjauan kembali" in raw_level: process_level = "pk"
        else: process_level = "lainnya"
        
    if process_level == "tidak diketahui" and case_number:
        if re.search(r'\d+\s*PK/', case_number):
            process_level = "pk"
        elif re.search(r'\d+\s*K/', case_number):
            process_level = "kasasi"
        elif re.search(r'\bPT\b', case_number):
            process_level = "banding"
        elif re.search(r'\bPN\b', case_number):
            process_level = "tingkat pertama"


    raw_indictment = str(what_block.get("indictment_model", "")).lower().strip()
    indictment_model = "tidak diketahui"

    if raw_indictment and raw_indictment != "none" and raw_indictment != "":
        if "subsidair" in raw_indictment or "subsider" in raw_indictment:
            indictment_model = "subsidair"
        elif "alternatif" in raw_indictment or "atau" in raw_indictment:
            indictment_model = "alternatif"
        elif "kumulatif" in raw_indictment or "dan" in raw_indictment:
            indictment_model = "kumulatif"
        elif "kombinasi" in raw_indictment:
            indictment_model = "kombinasi"
        elif "tunggal" in raw_indictment:
            indictment_model = "tunggal"
        else:
            indictment_model = "lainnya"

    raw_verdict = str(what_block.get("verdict_per_charge", "")).lower().strip()
    verdict_outcome = "tidak diketahui"

    if raw_verdict and raw_verdict != "none" and raw_verdict != "":
        if "tidak terbukti" in raw_verdict or "bebas" in raw_verdict:
            verdict_outcome = "acquitted"
        elif "lepas" in raw_verdict:
            verdict_outcome = "released"
        elif "terbukti" in raw_verdict or "pidana" in raw_verdict or "bersalah" in raw_verdict:
            verdict_outcome = "convicted"

    prison_term = how_much_block.get("prison_term") or {}

    def safe_int(val):
        try:
            return int(val)
        except (ValueError, TypeError):
            return None
        
    prison_years = safe_int(prison_term.get("years"))
    prison_months = safe_int(prison_term.get("months"))

    raw_credit = what_block.get("detention_credit")

    if raw_credit is None or str(raw_credit).strip() == "":
        detention_credit = False
    elif isinstance(raw_credit, bool):
        detention_credit = raw_credit
    else:
        detention_credit = str(raw_credit).lower().strip() in ['true', 'ya', '1', 'benar']

    raw_has_plan = what_block.get("has_attack_plan")
    if raw_has_plan is None or str(raw_has_plan).strip() == "":
        has_attack_plan = False
    elif isinstance(raw_has_plan, bool):
        has_attack_plan = raw_has_plan
    else:
        has_attack_plan = str(raw_has_plan).lower().strip() in ['true', 'ya', '1', 'benar', 'ada']

    def clean_text_fallback(val):
        if val is None or val == [] or val == {}:
            return "tidak diketahui"
            
        val_str = str(val).strip()
        
        if val_str.lower() in ["", "none", "null", "[]", "{}", "nan", "n/a", "na", "-"]:
            return "tidak diketahui"
            
        return val_str

    attack_plan_summary = clean_text_fallback(what_block.get("attack_plan_summary"))
    appeal_timeline = clean_text_fallback(when_block.get("appeal_timeline"))
    
    what_block.update({
        "court_level": process_level,
        "indictment_model": indictment_model,
        "verdict_per_charge": verdict_outcome,
        "detention_credit": detention_credit,
        "attack_plan_summary": attack_plan_summary
    })

    how_much_block["prison_term_years"] = prison_years
    how_much_block["prison_term_months"] = prison_months

    when_block["appeal_timeline"] = appeal_timeline

    data["what"] = what_block
    data["how_much"] = how_much_block
    data["when"] = when_block
    return data