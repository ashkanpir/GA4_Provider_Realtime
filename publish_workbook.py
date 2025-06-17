import os
import time
import logging
import pandas as pd
from dotenv import load_dotenv
import tableauserverclient as TSC
from tableauhyperapi import (
    HyperProcess, Connection, TableDefinition, SqlType, Telemetry,
    Inserter, CreateMode, TableName, Name
)

# === Load Environment ===
load_dotenv()

# === Paths & Credentials ===
base_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(base_dir, "output", "provider_12h_all_brands.csv")
hyper_path = csv_path.replace(".csv", ".hyper")

tableau_server = os.getenv("TABLEAU_SERVER")
tableau_site = os.getenv("TABLEAU_SITE")
tableau_project = os.getenv("TABLEAU_PROJECT")
datasource_name = os.getenv("TABLEAU_DATASOURCE")
token_name = os.getenv("TABLEAU_TOKEN_NAME")
token_secret = os.getenv("TABLEAU_TOKEN_SECRET")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s")

# === Helpers ===
def wait_for_csv(path: str, retries: int = 5, delay: int = 1) -> bool:
    for attempt in range(retries):
        if os.path.exists(path):
            logger.info("✅ CSV file exists, ready to publish.")
            return True
        logger.warning(f"⏳ CSV not found yet. Retry {attempt + 1}/{retries}...")
        time.sleep(delay)
    logger.error(f"❌ CSV still missing after retries: {path}")
    return False

def map_column_type(col_name: str, dtype: str) -> SqlType:
    dtype = dtype.lower()
    if col_name == "datetime":
        return SqlType.timestamp()
    elif col_name == "brand":
        return SqlType.text()
    elif "object" in dtype or "str" in dtype:
        return SqlType.text()
    elif "float" in dtype or "double" in dtype:
        return SqlType.double()
    elif "int" in dtype:
        return SqlType.big_int()
    else:
        logger.warning(f"⚠️ Defaulting to TEXT for column '{col_name}' (dtype={dtype})")
        return SqlType.text()

# === Main publishing function ===
def publish_latest_hyper():
    if not wait_for_csv(csv_path):
        raise FileNotFoundError(f"CSV not found after waiting: {csv_path}")

    logger.info(f"🔄 Converting CSV to .hyper and publishing to Tableau Cloud as '{datasource_name}'")

    # Step 1: Convert CSV to Hyper
    df = pd.read_csv(csv_path)
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"])

    headers = df.columns.tolist()
    column_types = [map_column_type(col, str(df.dtypes[col])) for col in headers]

    if os.path.exists(hyper_path):
        os.remove(hyper_path)
        logger.info(f"♻️ Removed existing .hyper file: {hyper_path}")

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_path, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            table = TableDefinition(table_name=TableName("Extract"))
            for col, col_type in zip(headers, column_types):
                table.add_column(name=Name(col), type=col_type)
            connection.catalog.create_table(table)
            with Inserter(connection, table) as inserter:
                inserter.add_rows(df.itertuples(index=False, name=None))
                inserter.execute()

    logger.info(f"✅ .hyper created at {hyper_path}")

    # Step 2: Publish to Tableau Cloud
    auth = TSC.PersonalAccessTokenAuth(token_name, token_secret, site_id=tableau_site)
    server = TSC.Server(tableau_server, use_server_version=True)

    with server.auth.sign_in(auth):
        all_projects, _ = server.projects.get()
        project_id = next((p.id for p in all_projects if p.name == tableau_project), None)
        if not project_id:
            raise Exception(f"Project '{tableau_project}' not found")

        datasource_item = TSC.DatasourceItem(project_id, name=datasource_name)
        published = server.datasources.publish(
            datasource_item, hyper_path, mode=TSC.Server.PublishMode.Overwrite
        )

        url = f"{tableau_server}/#/site/{tableau_site}/datasources/{published.id}"
        logger.info(f"📤 Published: {published.name} (ID: {published.id})")
        logger.info(f"🔗 Tableau URL: {url}")
        print(f"🔗 View in Tableau Cloud: {url}")
        print("🎉 Done: CSV → HYPER → Tableau Cloud")

# === Entry point ===
if __name__ == "__main__":
    publish_latest_hyper()
