"""Central configuration. Everything is overridable with environment variables (see .env.example)."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = Path(os.getenv("DATA_DIR", ROOT / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_CAISO_FUEL = "caiso.fuelmix"
TOPIC_CAISO_DEMAND = "caiso.demand"
TOPIC_EIA = "eia930.fueltype"
TOPIC_NWS = "nws.forecast"

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
KEY_CURRENT = "carbon:current"
KEY_RANKING = "carbon:ranking"      # sorted set: member = hour (UTC ISO), score = forecast kg/MWh

# CAISO Today's Outlook (verify these URLs in a browser once; CAISO occasionally reorganises them)
CAISO_BASE = os.getenv("CAISO_BASE", "https://www.caiso.com/outlook/SP/History")
CAISO_FUEL_FILE = "fuelsource.csv"
CAISO_DEMAND_FILE = "demand.csv"
CAISO_CO2_FILE = os.getenv("CAISO_CO2_FILE", "co2.csv")   # used only by eval/accuracy_check.py

EIA_API_KEY = os.getenv("EIA_API_KEY", "")
EIA_URL = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"
EIA_PAGE = 5000                      # EIA v2 returns at most 5000 rows per request

NWS_LAT = float(os.getenv("NWS_LAT", "37.3382"))     # San Jose
NWS_LON = float(os.getenv("NWS_LON", "-121.8863"))
NWS_USER_AGENT = os.getenv("NWS_USER_AGENT", "data226-carbon-pipeline (student project; contact: you@sjsu.edu)")

WATERMARK = "30 minutes"
WINDOW = "5 minutes"

# Snowflake (leave SNOWFLAKE_ACCOUNT empty to use the local SQLite fallback for development)
SF = dict(
    account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
    user=os.getenv("SNOWFLAKE_USER", ""),
    password=os.getenv("SNOWFLAKE_PASSWORD", ""),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "CARBON_WH"),
    database=os.getenv("SNOWFLAKE_DATABASE", "CARBON"),
    role=os.getenv("SNOWFLAKE_ROLE", ""),
)
SQLITE_PATH = DATA_DIR / "local_warehouse.db"
