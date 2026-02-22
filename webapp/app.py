import os
import pymysql
from flask import Flask, render_template, jsonify
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

DB_HOST = os.getenv("MYSQL_HOST")
DB_PORT = int(os.getenv("MYSQL_PORT", "3306"))
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
        cursorclass=pymysql.cursors.DictCursor
    )


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/wells")
def wells():
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("SHOW COLUMNS FROM wells")
        cols = [row["Field"] for row in cur.fetchall()]

        if "latitude" in cols and "longitude" in cols:
            cur.execute("SELECT * FROM wells")
        else:
            cur.execute("SELECT id, source_pdf, well_name_and_number, api_number_10 FROM wells")

        rows = cur.fetchall()

    conn.close()
    return jsonify(rows)


if __name__ == "__main__":
    app.run(debug=True)