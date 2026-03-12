# Project 2 — End-to-End ETL Pipeline & Azure Data Warehouse Modernization

**Organization:** Aware Custom Biometric Wearables  
**Domain:** Data Engineering | Cloud Architecture | ETL/ELT  
**Reported To:** CFO, COO, VPs, Directors  
**Confidentiality:** 🔒 Enterprise Internal Project — Defense-Adjacent Organization  
> *All production pipelines, connection strings, schema configurations, and IoT device data are confidential. This repository reproduces the architectural pattern and transformation logic using synthetic biometric and order data.*

---

## Business Problem

Aware CBW operated with siloed data sources — Shopify storefronts (awarecbw.com, awareindustrial.com, awaredefense.us), Dynamics 365 CRM, and IoT biometric device feeds were all disconnected. Analysts ran manual exports to Excel, data freshness was 24–48 hours behind, and there was no validated, analytics-ready layer for BI consumption.

## Solution

Architected a Medallion-layer data warehouse on Azure Synapse Analytics (Bronze → Silver → Gold), with Azure Data Factory pipelines ingesting raw data from three sources: Shopify REST API, Dynamics 365 CRM, and IoT device feeds from Aware's biometric hearables (EarBud, CEP, EarDefender). Python transformation scripts on Azure Databricks performed cleaning, anomaly flagging, and schema normalisation before loading into query-optimised columnstore tables.

## Technical Architecture

```
[Shopify API]         ─┐
[Dynamics 365 CRM]    ─┤──► Azure Data Factory ──► ADLS Gen2 (Bronze)
[IoT Device Feed]     ─┘         │
                                 ▼
                     Azure Synapse Analytics
                     ├── Bronze  (raw ingestion)
                     ├── Silver  (cleaned / validated)
                     └── Gold    (aggregated, BI-ready)
                                 │
                                 ▼
                          Power BI Dashboards
```

## Key Deliverables

- ADF pipelines with OAuth2 Shopify API connector and incremental watermark loading  
- Python ETL scripts for IoT biometric data cleaning and anomaly detection  
- Medallion warehouse schema with HASH distribution and columnstore indexes  
- CI/CD pipeline deployment via Azure DevOps  
- ETL architecture runbooks and transformation documentation  

## Impact

| Metric | Result |
|---|---|
| Operational processing effort | **40% reduction** |
| Data load speed | **65% improvement** |
| Sources consolidated | Shopify + CRM + IoT → 1 warehouse |
| Manual analyst exports eliminated | ✅ |

## Repository Contents

```
Project_02_ETL_Pipeline_Azure/
├── etl/
│   └── etl_pipeline.py         # Full Extract → Transform → Load pipeline
├── sql/
│   └── warehouse_schema.sql    # Bronze / Silver / Gold schema (Azure Synapse)
├── data/
│   ├── iot_device_feed.csv     # Synthetic IoT biometric device readings (600 rows)
│   └── shopify_orders_raw.csv  # Synthetic Shopify order data (400 rows)
└── README.md
```

## Running the ETL Demo

```bash
pip install pandas numpy pyarrow
python etl/etl_pipeline.py
# Output: data/warehouse_ready/fact_biometric_readings.parquet
#         data/warehouse_ready/fact_sales_orders.parquet
```

## Tools & Technologies

Azure Data Factory · Azure Synapse Analytics · ADLS Gen2 · SSIS · SQL Server · SSMS · Python (Pandas, NumPy) · Azure DevOps (CI/CD)

---
*For technical discussion, connect via [LinkedIn](https://linkedin.com/in/ankitbharti2834).*
