# Medallion Architecture using Databricks DLT Pipelines

An end-to-end **Supply Chain Analytics Platform** built on **Databricks Delta Live Tables (DLT)**, implementing a full Medallion architecture (Bronze → Silver → Gold) using SAP and IBP datasets. Business-ready fact and dimension tables power **Power BI dashboards** for procurement, inventory, and demand analytics.

---

## Overview

This project transforms raw SAP CSV extracts into an analytics-ready lakehouse using a config-driven, single-notebook DLT pipeline. All three layers, raw ingestion, cleansing & transformation, and star schema modeling, are declared in one DLT notebook and executed as a managed pipeline in Databricks.

---

## Architecture  

<img width="1026" height="300" alt="image" src="https://github.com/user-attachments/assets/92fa853b-ce4f-476e-a59e-5b60c155bb06" />  
  
```
SAP / IBP Source Data
  └── 37 SAP tables (CSV) + IBP Demand Actual/Forecast
        │
        ▼  [Bronze Layer]
        │  • Config-driven ingestion via config.json
        │  • Single reusable function + loop loads all tables
        │  • Schema inference, column name cleaning
        │  • Audit column: dw_load_ts
        │  • No transformations - raw data preserved as Delta tables
        │
Bronze Delta Tables (bronze.*)
        │
        ▼  [Silver Layer]
        │  • Column renaming + data type casting
        │  • Null handling + DQ checks via @dlt.expect_or_drop
        │  • Deduplication (dropDuplicates)
        │  • Multi-table joins (SAP tables joined using business keys)
        │  • Business rule application (PO status, PO type, sourcing type)
        │
Silver Delta Tables (silver.*)
        │
        ▼  [Gold Layer]
        │  • Star Schema - Fact + Dimension tables
        │  • SCD Type 1 via dlt.apply_changes()
        │  • Sequenced by dw_load_ts
        │
Gold Delta Tables (gold.*)
        │
        ▼
Power BI Dashboards
  ├── Purchase Orders Report
  ├── Inventory Overview Report
  └── Demand Analysis Report
```

---

## Project Structure

```
medallion-lakehouse-pipeline/
│
├── data/
│   └── nbk_dlt_load_to_bronze_silver_gold.csv     # Raw SAP + IBP CSV source files (37 tables)
│
├── databricks/
│   ├── nbk_dlt_load_to_bronze_silver_gold.ipynb   # Main DLT pipeline notebook
│   └── config.json                                # Pipeline configuration (table names + source path)
│
├── pbi/
│   └── supply_chain_report.pbix                   # Power BI report file
│
└── README.md
```

---

## Notebook - `nbk_dlt_load_to_bronze_silver_gold.ipynb`

A single Databricks DLT notebook that declares all Bronze, Silver, and Gold tables. Executed as a Delta Live Tables pipeline in Databricks.

### Helper Functions

| Function | Description |
|---|---|
| `path_exists(path)` | Checks if a file path exists in DBFS before attempting to load |
| `clean_column_names(name)` | Lowercases column names and replaces special characters with `_`, collapsing consecutive underscores |

### Config

The pipeline reads `config.json` via `spark.conf.get("config_path")` - the config path is passed as a DLT pipeline parameter.

---

## Bronze Layer

All 43 raw CSV files are ingested using a **single reusable function** combined with a for loop, no separate notebook per table.

```python
def create_bronze_table(table_name, source_path):
    @dlt.table(name=f"bronze.{table_name}", table_properties={"quality": "bronze"})
    def bronze_table():
        df = spark.read.format("csv").option("header", "true").load(source_path)
        # Clean column names
        # Add dw_load_ts audit column
        return df

for bronze_table in bronze_tables:
    if path_exists(f"{bronze_source_relative_path}/{bronze_table}.csv"):
        create_bronze_table(bronze_table, source_path)
```

**Bronze tables ingested (43 total):**

| Category | Tables |
|---|---|
| Demand | `DEMAND_ACTUAL`, `DEMAND_FORECAST` |
| Master Data | `MASTER_CUSTOMER`, `MASTER_CUSTOMER_PRODUCT`, `MASTER_LOCATION`, `MASTER_LOCATION_PRODUCT` |
| Quality | `AUSP`, `AUSP_BATCH`, `QALS`, `QAVE` |
| Purchasing | `EKBE`, `EKET`, `EKKO`, `EKPO` |
| Vendor | `LFA1`, `LFB1`, `LFM1` |
| Material | `MAKT`, `MARA`, `MARC`, `MARCH`, `MARD`, `MARDH`, `MBEW`, `MBEWH`, `MCH1`, `MCHB`, `MCHBH`, `MSLB`, `MSLBH` |
| Production | `PLKO` |
| Config/Text | `T001`, `T001K`, `T001L`, `T001W`, `T023T`, `T024`, `T024E`, `T077K`, `T134T`, `TCURF`, `TQ30T`, `TQ31T` |

---

## Silver Layer

Silver tables apply **DQ checks**, **transformations**, and **joins** using `@dlt.expect_or_drop` for data quality enforcement. All tables include a `dw_load_ts` audit column.

### Silver Tables

| Table | Source Bronze Tables | Key DQ Checks |
|---|---|---|
| `silver.customer` | `MASTER_CUSTOMER` | `market is not null` |
| `silver.customer_product` | `MASTER_CUSTOMER_PRODUCT` | `market`, `material` not null |
| `silver.location` | `MASTER_LOCATION` | `plant is not null` |
| `silver.location_product` | `MASTER_LOCATION_PRODUCT` | `plant`, `material` not null |
| `silver.product` | `MARA`, `MAKT`, `MARC`, `T023T`, `T134T` | `material is not null` |
| `silver.batch` | `MCH1`, `MARA`, `MARC`, `MCHB` | `batch is not null` |
| `silver.supplier` | `LFA1`, `LFB1`, `LFM1` | `supplier_number is not null` |
| `silver.storage` | `T001L`, `T001W` | `plant`, `storage_location` not null |
| `silver.uom` | `MARA` | `uom is not null` |
| `silver.currency` | `TCURF` | `currency is not null` |
| `silver.date` | Generated | `date is not null` |
| `silver.purchase_order` | `EKPO`, `EKKO`, `EKET`, `T001`, `T001W`, `LFA1`, `T023T`, `MAKT`, `T024`, `T024E` | `purchase_order`, `item` not null |
| `silver.inventory` | `MCHB`, `MCH1`, `MARA`, `MAKT`, `MARC`, `T001W`, `T001`, `LFA1`, `T001K` | `material`, `plant`, `storage_location`, `batch` not null |
| `silver.inventory_month_end_stock` | `MCHBH`, `MCH1`, `MARA`, `MAKT`, `MARC`, `T001W` | `material`, `plant`, `storage_location`, `batch`, `period_id` not null |
| `silver.batch_release_external` | `QALS`, `T001W`, `TQ30T`, `TQ31T`, `T001L`, `LFA1`, `MARA`, `T023T`, `T134T`, `MCH1`, `MARC`, `QAVE` | `inspection_lot is not null` |
| `silver.batch_release_internal` | `QALS`, `T001W`, `TQ30T`, `TQ31T`, `T001L`, `QAVE` | `inspection_lot is not null` |
| `silver.demand_actual` | `DEMAND_ACTUAL` | `period_id`, `material`, `market` not null |
| `silver.demand_forecast` | `DEMAND_FORECAST` | `period_id`, `material`, `market` not null |

### Business Rules Applied (Purchase Order)

| Rule | Logic |
|---|---|
| `po_status` | `open` if still_to_deliver > 0, else `closed` |
| `po_type` | `direct` (NB), `indirect` (ZARB), `sto` (UB), else `other` |
| `po_sub_type` | `subcontract po` (L), `turn key po` (blank/null), else `other` |
| `sourcing_type` | `Internal Mfg` (UB), else `External Mfg` |

---

## Gold Layer

Gold tables use `dlt.apply_changes()` with **SCD Type 1** (overwrite), sequenced by `dw_load_ts`. Implements a **Star Schema** for analytical reporting.

### Dimension Tables

| Table | Source | Natural Key |
|---|---|---|
| `gold.dim_customer` | `silver.customer` | `market` |
| `gold.dim_customer_product` | `silver.customer_product` | `market`, `material` |
| `gold.dim_location` | `silver.location` | `plant` |
| `gold.dim_location_product` | `silver.location_product` | `plant`, `material` |
| `gold.dim_batch` | `silver.batch` | `batch` |
| `gold.dim_supplier` | `silver.supplier` | `supplier_number` |
| `gold.dim_uom` | `silver.uom` | `uom` |
| `gold.dim_storage` | `silver.storage` | `plant`, `storage_location` |
| `gold.dim_currency` | `silver.currency` | `currency` |

### Fact Tables

| Table | Source | Natural Key |
|---|---|---|
| `gold.fact_purchase_order` | `silver.purchase_order` | `purchase_order`, `item` |
| `gold.fact_inventory` | `silver.inventory` | `material`, `plant`, `storage_location`, `batch` |
| `gold.fact_inventory_month_end_stock` | `silver.inventory_month_end_stock` | `material`, `plant`, `storage_location`, `batch`, `period_id` |
| `gold.fact_batch_release_external` | `silver.batch_release_external` | `inspection_lot` |
| `gold.fact_batch_release_internal` | `silver.batch_release_internal` | `inspection_lot` |
| `gold.fact_demand_actual` | `silver.demand_actual` | `period_id`, `material`, `market` |
| `gold.fact_demand_forecast` | `silver.demand_forecast` | `period_id`, `material`, `market` |

---

## Config File - `config.json`

The pipeline reads its configuration from `config.json`, which lists all Bronze table names and the source data path. Passed to the DLT pipeline as a parameter (`config_path`).

```json
{
  "bronze_tables": [
    "DEMAND_ACTUAL",
    "DEMAND_FORECAST",
    "MASTER_CUSTOMER",
    ...
  ],
  "bronze_source_relative_path": "/path/to/raw_data"
}
```

To add a new table: add its name to `bronze_tables` and place the corresponding `.csv` file in the `raw_data` directory - no code changes required.

---

## Power BI Reports

Connected to Databricks via HTTP path and server hostname. Three reports built on Gold layer tables:

| Report | Key Metrics |
|---|---|
| Purchase Orders | Open PO value, delivery status, PO type breakdown, supplier performance |
| Inventory Overview | Stock by plant/batch, month-end stock trends, shelf life tracking |
| Demand Analysis | Actual vs forecast demand, market and material-level demand trends |

---

## Getting Started

1. Clone this repository.
2. Upload the CSV files from `data/` to your Databricks workspace raw data directory.
3. Update `config.json` with your actual source path:
   ```json
   "bronze_source_relative_path": "/Workspace/Users/<your-email>/project/raw_data"
   ```
4. Create a Databricks **Delta Live Tables pipeline** and point it to `databricks/nbk_dlt_load_to_bronze_silver_gold.ipynb`.
5. Add a pipeline parameter:
   ```
   config_path = /path/to/databricks/config.json
   ```
6. Run the pipeline - all Bronze, Silver, and Gold tables will be created automatically.
7. Connect Power BI to your Databricks SQL warehouse using the HTTP path and server hostname.

---

## Tech Stack

![Databricks](https://img.shields.io/badge/Databricks-DLT-red)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-enabled-green)
![PySpark](https://img.shields.io/badge/PySpark-3.x-orange)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow)
![SAP](https://img.shields.io/badge/Source-SAP%20%2B%20IBP-blue)
