import os
import csv
import uuid
import json
import random
import string
import logging
import psycopg2
import time
import threading
from io import BytesIO
from datetime import datetime
from PIL import Image, ImageDraw
import qrcode
import zipfile
from psycopg2 import pool
from concurrent.futures import ThreadPoolExecutor

# Setup
logging.basicConfig(level=logging.INFO)
lock = threading.Lock()
seen_code_ids = set()
results = []

# Constants
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FOLDER = os.path.join(BASE_DIR, "qr-records", "qrs")
CSV_PATH = os.path.join(BASE_DIR, "files", "file.csv")
ZIP_FOLDER = os.path.join(BASE_DIR, "qr-records", "zips")
LOGO_PATH = "assets/logo.png"
DB_DSN = "postgresql://gouser:gopassword@localhost:5432/kulushorturl"
BATCH_SIZE = 100
QR_VERSION = 4
QR_BORDER = 0
FIXED_SIZE = (1700, 1700)

# Colors
FOREGROUND_COLOR = (10, 35, 96, 255)
EYEBALL_COLOR = (90, 166, 219, 255)
BACKGROUND_COLOR = (255, 255, 255, 255)

# Paths
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
os.makedirs(ZIP_FOLDER, exist_ok=True)
error_log_path = os.path.join(BASE_DIR, "qr-error.log")

# DB pool
db_pool = psycopg2.pool.SimpleConnectionPool(minconn=5, maxconn=50, dsn=DB_DSN)

# Helpers
def generate_code_id(length=10):
    return ''.join(random.choices(string.digits, k=length))

def is_finder_pattern_module(x, y):
    return (x == 0 or x == 6 or y == 0 or y == 6) or (2 <= x <= 4 and 2 <= y <= 4)

def is_eyeball(x, y):
    return 2 <= x <= 4 and 2 <= y <= 4

def fill_rect(img, x, y, width, height, color):
    draw = ImageDraw.Draw(img)
    draw.rectangle([x, y, x + width - 1, y + height - 1], fill=color)

def replace_finder_pattern(img, top_left_x, top_left_y, module_size):
    for row in range(7):
        for col in range(7):
            module_x = top_left_x + col * module_size
            module_y = top_left_y + row * module_size
            fill_color = EYEBALL_COLOR if is_eyeball(col, row) else FOREGROUND_COLOR if is_finder_pattern_module(col, row) else BACKGROUND_COLOR
            fill_rect(img, module_x, module_y, module_size, module_size, fill_color)

def stylize_finder_patterns(img, qr_matrix_size, border):
    width = img.size[0]
    module_size = width // (qr_matrix_size + 2 * border)
    offset = border * module_size
    positions = {
        "top_left": (offset, offset),
        "top_right": (width - offset - 7 * module_size, offset),
        "bottom_left": (offset, width - offset - 7 * module_size),
    }
    for pos in positions.values():
        replace_finder_pattern(img, pos[0], pos[1], module_size)
    return img

def create_qr_image(payload_str, filename):
    qr = qrcode.QRCode(
        version=QR_VERSION,
        error_correction=qrcode.constants.ERROR_CORRECT_H,
        box_size=10,
        border=QR_BORDER,
    )
    qr.add_data(payload_str)
    qr.make(fit=True)
    matrix_size = len(qr.get_matrix())

    img = qr.make_image(fill_color=FOREGROUND_COLOR[:3], back_color=BACKGROUND_COLOR[:3]).convert("RGBA")
    img = img.resize(FIXED_SIZE, resample=Image.Resampling.LANCZOS)
    img = stylize_finder_patterns(img, matrix_size, QR_BORDER)

    if os.path.exists(LOGO_PATH):
        logo = Image.open(LOGO_PATH).convert("RGBA")
        logo_size = int(FIXED_SIZE[0] * 0.2)
        logo = logo.resize((logo_size, logo_size), resample=Image.Resampling.LANCZOS)
        pos = ((FIXED_SIZE[0] - logo_size) // 2, (FIXED_SIZE[1] - logo_size) // 2)
        img.paste(logo, pos, mask=logo)

    img.save(filename)
    buffer = BytesIO()
    img.save(buffer, format="PNG")
    return filename, buffer.getvalue()


def create_qr_request(requested_count, user_name="system", requested_by="system") -> uuid.UUID:
    conn = db_pool.getconn()
    try:
        cur = conn.cursor()
        qr_request_id = uuid.uuid4()
        now = datetime.utcnow()
        cur.execute("""
            INSERT INTO qrcode_request (
                id, type, status, requested_count, created_count,
                zip_url, user_name, requested_by, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(qr_request_id), "Business", "Ready For Download", requested_count, requested_count,
            str(qr_request_id)+"/business.zip", user_name, requested_by, now, now
        ))
        conn.commit()
        return qr_request_id
    except Exception as e:
        conn.rollback()
        logging.error("Failed to create QRRequest: %s", e)
        raise
    finally:
        db_pool.putconn(conn)


def insert_batch_records(records,qr_request_id):
    conn = db_pool.getconn()
    try:
        cur = conn.cursor()
        for r in records:
            cur.execute("""
                INSERT INTO qrcode_record (
                    id, code_id, ext_id, qr_code_url, content_type,
                    status, type, created_at, updated_at,qr_request_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                str(uuid.uuid4()), r['code_id'], r['ext_id'], r['qr_code_url'],
                "image/png", "Active", "Business", r['created_at'], r['updated_at'],str(qr_request_id)
            ))
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
        logging.error("Batch DB insert failed: %s", e)
    finally:
        db_pool.putconn(conn)

def zip_and_cleanup(image_folder, batch_index, records_batch):
    zip_name = os.path.join(ZIP_FOLDER, f"qr_batch_{batch_index}.zip")
    with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zipf:
        for r in records_batch:
            file_path = os.path.join(BASE_DIR, r["qr_code_url"])
            if os.path.exists(file_path):
                zipf.write(file_path, arcname=os.path.basename(file_path))
                os.remove(file_path)

def process_row(row, posid_idx, wallid_idx):
    try:
        ext_id = row[wallid_idx].strip()
        code_id = row[posid_idx].strip().zfill(6) if posid_idx != -1 and row[posid_idx].strip() else generate_code_id()
        if not ext_id or code_id in seen_code_ids:
            return None
        with lock:
            seen_code_ids.add(code_id)

        payload = json.dumps({"external_id": ext_id, "code_id": code_id})
        full_path = os.path.join(OUTPUT_FOLDER, f"{code_id}.png")
        _, _, trimmed_path = full_path.partition("qr-records/")
        file_path, qr_bytes = create_qr_image(payload, os.path.join("qr-records", trimmed_path))

        now = datetime.utcnow()
        return {
            "code_id": code_id,
            "ext_id": ext_id,
            "qr_code_url": file_path,
            # "qr_code_byte": qr_bytes,
            "created_at": now,
            "updated_at": now
        }
    except Exception as e:
        with open(error_log_path, "a") as f:
            f.write(f"Error for ext_id={row[wallid_idx]}, posid={row[posid_idx] if posid_idx != -1 else 'N/A'}: {str(e)}\n")
        return None

def process_batch(batch_rows, posid_idx, wallid_idx, batch_num,qr_request_id):
    with ThreadPoolExecutor(max_workers=30) as executor:
        processed = list(executor.map(lambda r: process_row(r, posid_idx, wallid_idx), batch_rows))
        processed = [r for r in processed if r]
        insert_batch_records(processed,qr_request_id)
        results.extend(processed)

    print(f"✅ Batch {batch_num} complete. Created: {len(processed)} records.")

    # After every 1000 records, zip and clean up
    if len(results) % 1000 == 0:
        zip_index = len(results) // 1000
        zip_and_cleanup(OUTPUT_FOLDER, zip_index, results[-1000:])
        print(f"📦 Zipped and cleaned batch {zip_index} after 1000 QR codes.")

def seed_qr_codes_from_csv():
    with open(CSV_PATH, newline='') as csvfile:
        reader = csv.reader(csvfile)
        headers = [h.strip().lower() for h in next(reader)]
        posid_idx = headers.index("posid") if "posid" in headers else -1
        wallid_idx = headers.index("wallid") if "wallid" in headers else -1

        if wallid_idx == -1:
            raise ValueError("'wallid' column required")
        
        rows = list(reader)
        qr_request_id = create_qr_request(requested_count=len(rows))
        batch_rows = []
        batch_num = 1

        for row in rows:
            batch_rows.append(row)
            if len(batch_rows) >= BATCH_SIZE:
                process_batch(batch_rows, posid_idx, wallid_idx, batch_num, qr_request_id)
                batch_rows = []
                batch_num += 1


        if batch_rows:
            process_batch(batch_rows, posid_idx, wallid_idx, batch_num,qr_request_id)

if __name__ == "__main__":
    start = time.time()
    seed_qr_codes_from_csv()
    duration = time.time() - start
    print(f"✅ QR Generation Complete. Total created: {len(results)}")
    print(f"🕒 Time taken: {int(duration // 60)} minutes, {duration % 60:.2f} seconds")
