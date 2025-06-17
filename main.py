import os
import time
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
import tempfile
from pathlib import Path

# === Setup Paths ===
base_dir = Path(__file__).resolve().parent
log_dir = base_dir / "logs"
output_dir = base_dir / "output"
csv_path = output_dir / "realtime_30min_all_brands_long.csv"

# === Setup Logging ===
try:
    log_dir.mkdir(parents=True, exist_ok=True)
except OSError:
    print("⚠️ Could not write to logs/ — using /tmp instead.")
    log_dir = Path(tempfile.gettempdir())

print(f"📁 [DEBUG] Logging to: {log_dir}")

log_formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')

# Daily rotating log
realtime_log = log_dir / "realtime.log"
file_handler = TimedRotatingFileHandler(
    filename=realtime_log, when="midnight", interval=1, backupCount=7
)
file_handler.setFormatter(log_formatter)

# Error-only log
stderr_log = log_dir / "stderr.log"
error_handler = logging.FileHandler(stderr_log)
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(log_formatter)

# Console
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

# Assemble logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(error_handler)
logger.addHandler(console_handler)

logger.info("🚀 Starting real-time GA4 pipeline...")

try:
    # === Step 1: Extract data ===
    logger.info("📥 Running provider_extractor.py...")
    from provider_extractor import run_extraction
    run_extraction()
    logger.info("✅ Data extracted successfully.")

    # === Step 2: Wait for CSV to exist before publishing ===
    csv_path = "output/realtime_30min_all_brands_long.csv"
    max_retries = 5
    wait_seconds = 1.5

    for attempt in range(1, max_retries + 1):
        if os.path.exists(csv_path):
            logging.info("✅ CSV file exists, ready to publish.")
            break
        else:
            logging.warning(f"⏳ CSV not found yet. Retry {attempt}/{max_retries}...")
            time.sleep(wait_seconds)
    else:
        logging.error(f"❌ CSV still missing after retries: {csv_path}")
        raise FileNotFoundError(f"CSV not found after waiting: {csv_path}")

    # === Step 3: Publish to Tableau ===
    logger.info("📤 Publishing hyper file to Tableau Cloud...")
    from publish_workbook import publish_latest_hyper
    publish_latest_hyper()
    logger.info("✅ Hyper file published successfully.")

    logger.info("🎉 Real-time pipeline completed without errors.")

except Exception as e:
    logger.error("❌ Pipeline failed with error:", exc_info=True)

print("✅ Finished. Check logs for details.")
print("✅ Script finished and exited cleanly.")
