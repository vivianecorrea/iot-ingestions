CATALOG = "sandbox_first_catalog"
SCHEMA = "transports"

LANDING_PATH = "/Volumes/sandbox_first_catalog/transports/landing-zone/"
CHECKPOINT = f"/Volumes/{CATALOG}/{SCHEMA}/_checkpoints/layer="


TBL_BRONZE = f"{CATALOG}.{SCHEMA}.rides_bronze"
TBL_SILVER = f"{CATALOG}.{SCHEMA}.rides_silver"
TBL_GOLD = f"{CATALOG}.{SCHEMA}.rides_daily_info_gold"
