import pandas as pd
import zipfile
import os
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DISTRICT_EXCEL = r"C:\Users\ravis\Downloads\All_Districtof_India_2026-05-03_21-20-36.xlsx"
SUBDISTRICT_EXCEL = r"C:\Users\ravis\Downloads\All_Sub_Districtof_India_2026-05-03_21-19-20.xlsx"
VILLAGE_EXCEL = r"C:\Users\ravis\Downloads\All_Villagesof_India_2026-05-03_21-21-44.xlsx"

def process_districts():
    logging.info(f"Reading {DISTRICT_EXCEL}...")
    df = pd.read_excel(DISTRICT_EXCEL, skiprows=1)
    
    # Required: district_lgd, district_name, state_lgd
    mapped_df = pd.DataFrame({
        'district_lgd': df['District Code'],
        'district_name': df['District Name(In English)'],
        'state_lgd': df['State Code']
    })
    
    out_file = 'DISTRICT_STATE.csv'
    logging.info(f"Saving {out_file}...")
    mapped_df.to_csv(out_file, index=False)
    logging.info(f"Processed {len(mapped_df)} districts.")

def process_subdistricts():
    logging.info(f"Reading {SUBDISTRICT_EXCEL}...")
    df = pd.read_excel(SUBDISTRICT_EXCEL, skiprows=1)
    
    # Required: subdistrict_lgd, subdistrict_name, district_lgd, state_lgd
    mapped_df = pd.DataFrame({
        'subdistrict_lgd': df['Sub-district Code'],
        'subdistrict_name': df['Sub-district Name'],
        'district_lgd': df['District Code'],
        'state_lgd': df['State Code']
    })
    
    out_file = 'SUBDISTRICT_DISTRICT.csv'
    zip_file = 'SUBDISTRICT_DISTRICT.zip'
    
    logging.info(f"Saving {out_file}...")
    mapped_df.to_csv(out_file, index=False)
    
    logging.info(f"Compressing into {zip_file}...")
    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(out_file)
        
    os.remove(out_file)
    logging.info(f"Processed {len(mapped_df)} sub-districts.")

def process_villages():
    logging.info(f"Reading {VILLAGE_EXCEL}... (This will take a minute or two)")
    df = pd.read_excel(VILLAGE_EXCEL, skiprows=1)
    
    # Required: village_lgd, village_name, subdistrict_lgd, district_lgd, state_lgd
    mapped_df = pd.DataFrame({
        'village_lgd': df['Village Code'],
        'village_name': df['Village Name (In English)'],
        'subdistrict_lgd': df['Sub-District Code'],
        'district_lgd': df['District Code'],
        'state_lgd': df['State Code']
    })
    
    out_file = 'VILLAGE_SUBDISTRICT.csv'
    zip_file = 'VILLAGE_SUBDISTRICT.zip'
    
    logging.info(f"Saving {out_file}...")
    mapped_df.to_csv(out_file, index=False)
    
    logging.info(f"Compressing into {zip_file}...")
    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(out_file)
        
    os.remove(out_file)
    logging.info(f"Processed {len(mapped_df)} villages.")

def rebuild_database():
    logging.info("Rebuilding SQLite database from the newly compressed files...")
    subprocess.run(["python", "build_db.py"], check=True)
    logging.info("Database successfully rebuilt!")

if __name__ == "__main__":
    try:
        process_districts()
        process_subdistricts()
        process_villages()
        rebuild_database()
        logging.info("All data ingestion tasks completed successfully.")
    except Exception as e:
        logging.error(f"Error during ingestion: {e}")
