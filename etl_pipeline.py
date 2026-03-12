"""
Project 2: End-to-End ETL Pipeline & Azure Data Warehouse Modernization
Aware Custom Biometric Wearables — Enterprise Internal (Confidential)

This script demonstrates the ETL architecture used to ingest raw IoT device
feeds, Shopify API order data, and Dynamics 365 CRM records into a
structured Azure Synapse Analytics warehouse layer.

NOTE: All credentials and connection strings are replaced with environment
variables. Synthetic data is used in place of production records.
"""

import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Configuration (replace with Azure Key Vault references in prod) ──────────
SYNAPSE_CONN   = os.getenv("AZURE_SYNAPSE_CONN_STR", "Driver={ODBC Driver 17};Server=<synapse_endpoint>;...")
ADLS_CONTAINER = os.getenv("ADLS_CONTAINER", "raw-data")
SHOPIFY_API    = os.getenv("SHOPIFY_API_KEY", "<key>")
CRM_API        = os.getenv("DYNAMICS365_API_KEY", "<key>")

RAW_IOT_PATH   = "data/iot_device_feed.csv"
RAW_ORDER_PATH = "data/shopify_orders_raw.csv"
OUTPUT_PATH    = "data/warehouse_ready/"
os.makedirs(OUTPUT_PATH, exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════
# EXTRACT LAYER
# ═══════════════════════════════════════════════════════════════════════════

def extract_iot_feed(path: str) -> pd.DataFrame:
    """
    Simulates ingestion from Azure Data Lake Storage Gen2.
    In production, this uses azure-storage-blob SDK to read
    Parquet partitions from ADLS into a Spark DataFrame via ADF.
    """
    logger.info("Extracting IoT biometric device feed...")
    df = pd.read_csv(path, parse_dates=["timestamp"])
    logger.info(f"  → {len(df):,} IoT records loaded.")
    return df


def extract_shopify_orders(path: str) -> pd.DataFrame:
    """
    Simulates Shopify API pagination via Azure Data Factory REST connector.
    Production pipeline uses OAuth2 + incremental watermark on created_at.
    """
    logger.info("Extracting Shopify order data (awarecbw.com / awaredefense.us / awareindustrial.com)...")
    df = pd.read_csv(path, parse_dates=["created_at"])
    logger.info(f"  → {len(df):,} order records loaded.")
    return df


# ═══════════════════════════════════════════════════════════════════════════
# TRANSFORM LAYER
# ═══════════════════════════════════════════════════════════════════════════

def transform_iot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and enriches raw IoT biometric readings:
      - Drops records with POOR signal quality
      - Normalises physiological ranges
      - Adds derived columns: is_anomaly, day_partition
    """
    logger.info("Transforming IoT feed...")

    initial = len(df)
    df = df[df["signal_quality"] != "POOR"].copy()
    logger.info(f"  → Signal quality filter: {initial - len(df)} records dropped.")

    # Range validation (medical-grade thresholds per Aware V2 specs)
    df = df[
        df["heart_rate_bpm"].between(40, 180) &
        df["spo2_pct"].between(85, 100) &
        df["core_temp_c"].between(35.0, 40.5)
    ]

    # Derived anomaly flag
    df["is_anomaly"] = (
        (df["heart_rate_bpm"] > 140) |
        (df["spo2_pct"] < 92) |
        (df["core_temp_c"] > 38.5)
    ).astype(int)

    # Partition column for ADLS file partitioning
    df["day_partition"] = df["timestamp"].dt.strftime("%Y-%m-%d")

    # Schema normalisation
    df.rename(columns={"product_sku": "sku_code"}, inplace=True)
    df["ingested_at"] = datetime.utcnow().isoformat()

    logger.info(f"  → IoT transform complete: {len(df):,} clean records.")
    return df


def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and enriches Shopify order records:
      - Removes cancelled orders from revenue aggregation
      - Calculates net revenue after discount
      - Maps SKU to product dimension keys
      - Adds fulfillment_days derived metric
    """
    logger.info("Transforming order data...")

    # Exclude hard-cancelled orders from revenue layer (retain in staging)
    revenue_df = df[df["fulfillment_status"] != "cancelled"].copy()

    revenue_df["net_revenue_usd"] = (
        revenue_df["quantity"] *
        revenue_df["unit_price"] *
        (1 - revenue_df["discount_pct"] / 100)
    ).round(2)

    revenue_df["channel"] = revenue_df["crm_account_id"].apply(
        lambda x: _map_channel(x)
    )

    revenue_df["ingested_at"] = datetime.utcnow().isoformat()
    logger.info(f"  → Order transform complete: {len(revenue_df):,} billable records.")
    return revenue_df


def _map_channel(crm_id: str) -> str:
    """Derive sales channel from CRM account ID prefix ranges."""
    num = int(crm_id.replace("CRM-", ""))
    if num < 2000:   return "awaredefense.us"
    if num < 3500:   return "awareindustrial.com"
    if num < 4500:   return "awarecbw.com"
    return "B2B_Direct"


# ═══════════════════════════════════════════════════════════════════════════
# LOAD LAYER
# ═══════════════════════════════════════════════════════════════════════════

def validate_schema(df: pd.DataFrame, required_cols: list, table: str) -> bool:
    """Pre-load schema validation gate."""
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        logger.error(f"[SCHEMA FAIL] {table}: missing columns {missing}")
        return False
    null_counts = df[required_cols].isnull().sum()
    critical_nulls = null_counts[null_counts > 0]
    if not critical_nulls.empty:
        logger.warning(f"[NULL WARNING] {table}:\n{critical_nulls}")
    return True


def load_to_warehouse(df: pd.DataFrame, table_name: str) -> None:
    """
    Simulates BULK INSERT into Azure Synapse Analytics.
    Production uses pyodbc with COPY INTO or SqlAlchemy + azure-synapse-analytics.
    Writes Parquet to ADLS as a fallback / audit trail.
    """
    out_file = os.path.join(OUTPUT_PATH, f"{table_name}.parquet")
    df.to_parquet(out_file, index=False)
    logger.info(f"  → Loaded {len(df):,} rows → [{table_name}]  (saved: {out_file})")


# ═══════════════════════════════════════════════════════════════════════════
# PIPELINE ORCHESTRATION
# ═══════════════════════════════════════════════════════════════════════════

def run_pipeline():
    logger.info("=" * 60)
    logger.info("Aware CBW — ETL Pipeline Starting")
    logger.info(f"Run time: {datetime.utcnow().isoformat()}")
    logger.info("=" * 60)

    # ── IoT Feed ─────────────────────────────────────────────
    iot_raw   = extract_iot_feed(RAW_IOT_PATH)
    iot_clean = transform_iot(iot_raw)
    if validate_schema(iot_clean, ["device_id","timestamp","sku_code","heart_rate_bpm","spo2_pct","is_anomaly"], "fact_biometric_readings"):
        load_to_warehouse(iot_clean, "fact_biometric_readings")

    # ── Shopify Orders ───────────────────────────────────────
    orders_raw   = extract_shopify_orders(RAW_ORDER_PATH)
    orders_clean = transform_orders(orders_raw)
    if validate_schema(orders_clean, ["order_id","created_at","sku","quantity","net_revenue_usd","channel"], "fact_sales_orders"):
        load_to_warehouse(orders_clean, "fact_sales_orders")

    # ── Summary Stats ────────────────────────────────────────
    logger.info("\n── Pipeline Summary ───────────────────────────────────")
    logger.info(f"  IoT records loaded:    {len(iot_clean):>8,}")
    logger.info(f"  Order records loaded:  {len(orders_clean):>8,}")
    anomaly_rate = iot_clean["is_anomaly"].mean() * 100
    logger.info(f"  Anomaly rate (IoT):    {anomaly_rate:>7.2f}%")
    logger.info("  Status: ✅ COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()
