import os
import csv
import json
import logging
import time
import threading
from io import BytesIO
from datetime import datetime
from PIL import Image, ImageDraw
import qrcode
import zipfile
from concurrent.futures import ThreadPoolExecutor

# Setup
logging.basicConfig(level=logging.INFO)
lock = threading.Lock()
seen_code_ids = set()
results = []

# Constants
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FOLDER = os.path.join(BASE_DIR, "qr-records", "qrs")
CSV_PATH = os.path.join(BASE_DIR, "files", "Entrepreneur_1st_30000.csv")
ZIP_FOLDER = os.path.join(BASE_DIR, "qr-records", "zips")
LOGO_PATH = "assets/logo.png"
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

# Helpers
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

def process_row(row, codeid_idx, wallid_idx):
    try:
        code_id = row[codeid_idx].strip()
        wall_id = row[wallid_idx].strip().zfill(11)

        if not code_id or code_id in seen_code_ids:
            return None
        with lock:
            seen_code_ids.add(code_id)

        payload = json.dumps({
            "code_id": code_id,
            "ext_id": wall_id
        })

        full_path = os.path.join(OUTPUT_FOLDER, f"{code_id}.png")
        _, _, trimmed_path = full_path.partition("qr-records/")
        create_qr_image(payload, os.path.join("qr-records", trimmed_path))

        return {
            "code_id": code_id,
            "ext_id": wall_id,
            "qr_code_url": trimmed_path
        }
    except Exception as e:
        with open(error_log_path, "a") as f:
            f.write(f"Error for code_id={code_id}: {str(e)}\n")
        return None

def process_batch(batch_rows, codeid_idx, wallid_idx, batch_num):
    with ThreadPoolExecutor(max_workers=30) as executor:
        processed = list(executor.map(lambda r: process_row(r, codeid_idx, wallid_idx), batch_rows))
        processed = [r for r in processed if r]
        results.extend(processed)

    print(f"✅ Batch {batch_num} complete. Created: {len(processed)} records.")

    if len(results) % 1000 == 0:
        zip_index = len(results) // 1000
        zip_and_cleanup(OUTPUT_FOLDER, zip_index, results[-1000:])
        print(f"📦 Zipped and cleaned batch {zip_index} after 1000 QR codes.")

def zip_and_cleanup(image_folder, batch_index, records_batch):
    zip_name = os.path.join(ZIP_FOLDER, f"qr_batch_{batch_index}.zip")
    with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zipf:
        for r in records_batch:
            file_path = os.path.join(OUTPUT_FOLDER, os.path.basename(r["qr_code_url"]))
            if os.path.exists(file_path):
                zipf.write(file_path, arcname=os.path.basename(file_path))
                os.remove(file_path)

def seed_qr_codes_from_csv():
    with open(CSV_PATH, newline='') as csvfile:
        reader = csv.reader(csvfile)
        headers = [h.strip().lower() for h in next(reader)]
        codeid_idx = headers.index("code_id") if "code_id" in headers else -1
        wallid_idx = headers.index("ext_id") if "ext_id" in headers else -1

        if codeid_idx == -1 or wallid_idx == -1:
            raise ValueError("'code_id' and 'ext_id' columns are required")

        rows = list(reader)
        batch_rows = []
        batch_num = 1

        for row in rows:
            batch_rows.append(row)
            if len(batch_rows) >= BATCH_SIZE:
                process_batch(batch_rows, codeid_idx, wallid_idx, batch_num)
                batch_rows = []
                batch_num += 1

        # Process any remaining rows
        if batch_rows:
            process_batch(batch_rows, codeid_idx, wallid_idx, batch_num)

        # Handle remaining images that are not part of a full 1000 batch
        remaining = len(results) % 1000
        if remaining > 0:
            zip_index = (len(results) // 1000) + 1
            zip_and_cleanup(OUTPUT_FOLDER, zip_index, results[-remaining:])
            print(f"📦 Zipped and cleaned final batch {zip_index} of {remaining} remaining QR codes.")

if __name__ == "__main__":
    start = time.time()
    seed_qr_codes_from_csv()
    duration = time.time() - start
    print(f"✅ QR Generation Complete. Total created: {len(results)}")
    print(f"🕒 Time taken: {int(duration // 60)} minutes, {duration % 60:.2f} seconds")
