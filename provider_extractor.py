import os
import time
import logging
import pandas as pd
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, DateRange, Dimension, Metric, FilterExpression, Filter
)

# === Load environment ===
load_dotenv()
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]

# === Set up logging ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger()

# === GA4 Client ===
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
client = BetaAnalyticsDataClient(credentials=credentials)

# === GA4 Properties ===
PROPERTIES = {
    "Jetbahis": "271552729",
    "Betchip": "420555141",
    "Betelli": "374934400",
    "Betroad": "298828340",
    "Bluffbet": "423856871",
    "Davegas": "411328708",
    "Discount Casino": "271547113",
    "Genzobet": "353463445",
    "Hovarda": "298834809",
    "Intobet": "314402899",
    "Milyar": "424995907",
    "Rexbet": "271530837",
    "Slotbon": "448461531",
    "Winnit": "476027509",
    "VidaVegas - Brazil": "415075867",
    "VidaVegas - LATAM": "479167499",
    "Jokera": "481292744",
    "Hitpot": "490858540"
}

# === Fetch casino_bet_placed data grouped by provider and hour ===
def fetch_provider_activity(property_id: str, brand: str, cutoff_dt: datetime) -> pd.DataFrame:
    try:
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[
                Dimension(name="customEvent:provider"),
                Dimension(name="dateHour")
            ],
            metrics=[
                Metric(name="eventCount"),
                Metric(name="totalUsers"),
                Metric(name="activeUsers")
            ],
            date_ranges=[DateRange(start_date="1daysAgo", end_date="today")],
            dimension_filter=FilterExpression(
                filter=Filter(
                    field_name="eventName",
                    in_list_filter=Filter.InListFilter(values=["casino_bet_placed"])
                )
            )
        )
        response = client.run_report(request)
    except Exception as e:
        if "Field" in str(e) and "provider" in str(e):
            logger.warning(f"⚠️ Skipping {brand}: 'provider' parameter not available.")
            return pd.DataFrame()
        logger.error(f"❌ GA4 API error for {brand}: {e}", exc_info=True)
        return pd.DataFrame()

    rows = []
    for row in response.rows:
        try:
            provider = row.dimension_values[0].value
            dt_str = row.dimension_values[1].value
            dt = datetime.strptime(dt_str, "%Y%m%d%H").replace(tzinfo=timezone.utc)

            if dt < cutoff_dt:
                continue

            event_count = int(row.metric_values[0].value)
            total_users = int(row.metric_values[1].value)
            active_users = int(row.metric_values[2].value)

            rows.append({
                "datetime": dt,
                "brand": brand,
                "provider": provider,
                "event_count": event_count,
                "total_users": total_users,
                "active_users": active_users
            })
        except Exception as err:
            logger.warning(f"⚠️ Failed to parse row for {brand}: {err}")
            continue

    logger.info(f"✅ {brand}: {len(rows)} rows after cutoff")
    return pd.DataFrame(rows)

# === Main ===
def run_provider_extraction():
    logger.info("🚀 Starting provider-focused extractor (last 12 hours)...")
    all_data = []
    failed_brands = []
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=12)

    for brand, prop_id in PROPERTIES.items():
        logger.info(f"🔍 Fetching data for {brand}...")
        try:
            df = fetch_provider_activity(prop_id, brand, cutoff)
            if df.empty:
                logger.warning(f"⚠️ No data returned for {brand}")
            else:
                all_data.append(df)
        except Exception as e:
            logger.error(f"❌ Failed for {brand}: {e}", exc_info=True)
            failed_brands.append(brand)
        time.sleep(1)

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.sort_values(by=["datetime", "brand", "provider"], inplace=True)

        output_dir = os.path.join(os.path.dirname(__file__), "output")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "provider_12h_all_brands.csv")
        combined.to_csv(output_path, index=False)

        logger.info(f"📦 Saved to {output_path} ({len(combined)} rows)")
        print(f"✅ Done: {output_path}")
    else:
        logger.warning("❗ No data collected.")

    if failed_brands:
        logger.warning(f"🛛 Failed brands: {', '.join(failed_brands)}")

# === Entry point ===
if __name__ == "__main__":
    run_provider_extraction()
