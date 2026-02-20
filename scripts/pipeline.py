import os
import glob
import pymysql
from dotenv import load_dotenv
from extract_pdf import process_pdf   # your working function

load_dotenv()

def get_connection():
    return pymysql.connect(
        host="localhost",
        port=3307, 
        user="appuser",
        password="apppass",
        database="oil_app",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )

# table schema: includes id, well_name, operator, api_number, county_state, field, completion_date, date_stimulated, stimulated_formation, top_ft, bottom_ft, stimulation_stages, volume, volume_units, type_treatment
# if it doesn't already exist
def create_table(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS wells (
            id INT AUTO_INCREMENT PRIMARY KEY,
            well_name VARCHAR(255),
            operator VARCHAR(255),
            api_number VARCHAR(50) UNIQUE,
            county_state VARCHAR(255),
            field VARCHAR(255),
            completion_date VARCHAR(50),
            date_stimulated VARCHAR(50),
            stimulated_formation VARCHAR(100),
            top_ft INT,
            bottom_ft INT,
            stimulation_stages INT,
            volume INT,
            volume_units VARCHAR(50),
            type_treatment VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

# actual upsert function - if api_number already exists, update the record instead of inserting a new one
def upsert_well(conn, data):
    with conn.cursor() as cursor:
        cursor.execute("""
        INSERT INTO wells (
            well_name,
            operator,
            api_number,
            county_state,
            field,
            completion_date,
            date_stimulated,
            stimulated_formation,
            top_ft,
            bottom_ft,
            stimulation_stages,
            volume,
            volume_units,
            type_treatment
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            well_name=VALUES(well_name),
            operator=VALUES(operator),
            county_state=VALUES(county_state),
            field=VALUES(field),
            completion_date=VALUES(completion_date),
            date_stimulated=VALUES(date_stimulated),
            stimulated_formation=VALUES(stimulated_formation),
            top_ft=VALUES(top_ft),
            bottom_ft=VALUES(bottom_ft),
            stimulation_stages=VALUES(stimulation_stages),
            volume=VALUES(volume),
            volume_units=VALUES(volume_units),
            type_treatment=VALUES(type_treatment)
        """, (
            data.get("well_name"),
            data.get("operator"),
            data.get("api_number"),
            data.get("county_state"),
            data.get("field"),
            data.get("completion_date"),
            data.get("date_stimulated"),
            data.get("stimulated_formation"),
            data.get("top_ft"),
            data.get("bottom_ft"),
            data.get("stimulation_stages"),
            data.get("volume"),
            data.get("volume_units"),
            data.get("type_treatment")
        ))

# actual pipeline function - iterates over every pdf in data
def run_pipeline():
    conn = get_connection()
    create_table(conn)

    pdf_files = glob.glob("data/*.pdf")
    print(f"Found {len(pdf_files)} PDFs")

    for pdf_path in pdf_files:
        print(f"Processing {pdf_path}")
        data = process_pdf(pdf_path)
        upsert_well(conn, data)

    conn.close()
    print("Pipeline complete.")


if __name__ == "__main__":
    run_pipeline()