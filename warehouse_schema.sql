-- ============================================================
-- Project 2: Azure Synapse Analytics – Data Warehouse Schema
-- Aware Custom Biometric Wearables — Enterprise Internal (Confidential)
-- Architecture: Medallion (Bronze → Silver → Gold)
-- ============================================================

-- ── BRONZE LAYER (Raw Ingestion) ─────────────────────────────

CREATE TABLE bronze.iot_device_raw (
    load_id         BIGINT IDENTITY(1,1),
    device_id       NVARCHAR(50),
    [timestamp]     DATETIME2,
    product_sku     NVARCHAR(50),
    heart_rate_bpm  INT,
    spo2_pct        DECIMAL(5,2),
    core_temp_c     DECIMAL(5,2),
    eeg_alpha_band  DECIMAL(8,4),
    motion_accel_x  DECIMAL(8,4),
    motion_accel_y  DECIMAL(8,4),
    signal_quality  NVARCHAR(20),
    batch_id        NVARCHAR(50),
    ingested_at     DATETIME2 DEFAULT GETUTCDATE()
)
WITH (DISTRIBUTION = ROUND_ROBIN, HEAP);

CREATE TABLE bronze.shopify_orders_raw (
    load_id             BIGINT IDENTITY(1,1),
    order_id            NVARCHAR(50),
    created_at          DATE,
    customer_id         NVARCHAR(50),
    sku                 NVARCHAR(50),
    quantity            INT,
    unit_price          DECIMAL(10,2),
    discount_pct        DECIMAL(5,2),
    fulfillment_status  NVARCHAR(30),
    shipping_country    NVARCHAR(10),
    crm_account_id      NVARCHAR(50),
    ingested_at         DATETIME2 DEFAULT GETUTCDATE()
)
WITH (DISTRIBUTION = ROUND_ROBIN, HEAP);

-- ── SILVER LAYER (Cleaned / Validated) ───────────────────────

CREATE TABLE silver.fact_biometric_readings (
    reading_id      BIGINT IDENTITY(1,1),
    device_id       NVARCHAR(50)    NOT NULL,
    reading_ts      DATETIME2       NOT NULL,
    sku_code        NVARCHAR(50),
    heart_rate_bpm  INT,
    spo2_pct        DECIMAL(5,2),
    core_temp_c     DECIMAL(5,2),
    eeg_alpha_band  DECIMAL(8,4),
    signal_quality  NVARCHAR(20),
    is_anomaly      BIT             DEFAULT 0,
    day_partition   DATE,
    ingested_at     DATETIME2
)
WITH (
    DISTRIBUTION = HASH(device_id),
    CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE silver.fact_sales_orders (
    order_id            NVARCHAR(50)    NOT NULL,
    order_date          DATE,
    customer_id         NVARCHAR(50),
    sku                 NVARCHAR(50),
    quantity            INT,
    unit_price          DECIMAL(10,2),
    discount_pct        DECIMAL(5,2),
    net_revenue_usd     DECIMAL(12,2),
    fulfillment_status  NVARCHAR(30),
    shipping_country    NVARCHAR(10),
    channel             NVARCHAR(60),   -- awarecbw.com / awaredefense.us / awareindustrial.com / B2B_Direct
    ingested_at         DATETIME2
)
WITH (
    DISTRIBUTION = HASH(order_id),
    CLUSTERED COLUMNSTORE INDEX
);

-- ── GOLD LAYER (Analytics-Ready Aggregates) ──────────────────

-- Daily revenue summary per channel
CREATE VIEW gold.vw_daily_revenue AS
SELECT
    order_date,
    channel,
    sku,
    SUM(quantity)           AS total_units,
    SUM(net_revenue_usd)    AS total_revenue,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM silver.fact_sales_orders
WHERE fulfillment_status NOT IN ('cancelled')
GROUP BY order_date, channel, sku;

-- Anomaly rate per device per day
CREATE VIEW gold.vw_device_anomaly_summary AS
SELECT
    day_partition,
    sku_code,
    COUNT(*)                AS total_readings,
    SUM(is_anomaly)         AS anomaly_count,
    ROUND(CAST(SUM(is_anomaly) AS FLOAT)
        / NULLIF(COUNT(*),0) * 100, 2) AS anomaly_rate_pct
FROM silver.fact_biometric_readings
GROUP BY day_partition, sku_code;
