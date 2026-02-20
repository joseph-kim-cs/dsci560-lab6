import os
import glob
import pymysql
from dotenv import load_dotenv
from extract_pdf import parse_fields

load_dotenv()

DB_HOST = os.getenv("MYSQL_HOST")
DB_PORT = int(os.getenv("MYSQL_PORT")) 
DB_USER = os.getenv("MYSQL_USER")
DB_PASS = os.getenv("MYSQL_PASSWORD")
DB_NAME = os.getenv("MYSQL_DB")

def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT, 
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )

# table schema: just source_pdf (prevent duplicates), well_name_and_number, api_number
def create_table(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS wells (
            id INT AUTO_INCREMENT PRIMARY KEY,

            source_pdf VARCHAR(255) NOT NULL,
            well_name_and_number VARCHAR(255),
            api_number_10 CHAR(10),

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

            UNIQUE KEY uniq_source_pdf (source_pdf),
            INDEX idx_api10 (api_number_10)
        )
        """)

# actual upsert function - if api_number already exists, update the record instead of inserting a new one
def upsert_well(conn, data):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO wells (
                source_pdf,
                well_name_and_number,
                api_number_10
            )
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                well_name_and_number=VALUES(well_name_and_number),
                api_number_10=VALUES(api_number_10)
            """,
            (
                data.get("source_pdf"),
                data.get("well_name_and_number"),
                data.get("api_number_10"),
            ),
        )

# actual pipeline function - iterates over every pdf in data
def run_pipeline():
    conn = get_connection()
    create_table(conn)

    pdf_files = glob.glob("data/*.pdf")
    print(f"Found {len(pdf_files)} PDFs")

    for pdf_path in pdf_files:
        base = os.path.basename(pdf_path)
        print(f"Processing {base}")

        extracted = parse_fields(pdf_path)

        # ensure schema keys exist + add source_pdf
        row = {
            "source_pdf": base,
            "well_name_and_number": extracted.get("well_name_and_number"),
            "api_number_10": extracted.get("api_number_10"),
        }

        print(row)

        upsert_well(conn, row)

    conn.close()
    print("Pipeline complete.")

if __name__ == "__main__":
    run_pipeline()