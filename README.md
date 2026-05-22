# SQL Data Warehouse Project

A SQL Server data warehouse built on a **bronze / silver / gold medallion architecture**, integrating customer, product, and sales data from two source systems (CRM + ERP) into a clean, analytics-ready star schema.

The project covers the full warehouse lifecycle — database and schema setup, layered ETL stored procedures, naming conventions, data-quality tests, and gold-layer dimensional models.

---

## Architecture

![Data architecture](docs/data_architecture.png)

| Layer | Purpose | Object type | Load pattern |
|---|---|---|---|
| **Bronze** | Raw landing zone — source data ingested as-is from CRM and ERP CSV files. | Tables (`bronze.*`) | `TRUNCATE` + `BULK INSERT`, full-load |
| **Silver** | Cleaned, conformed, deduplicated. Type-casted columns, standardized codes, derived columns, and audit metadata (`dwh_*`). | Tables (`silver.*`) | Full-load, source-to-silver transforms |
| **Gold** | Business-ready star schema. Surrogate keys, conformed dimensions, fact tables. | Views (`gold.*`) | Logical only — built on top of silver |

---

## Source systems

Two source systems flow into the warehouse:

- **CRM** — `cust_info`, `prd_info`, `sales_details`
- **ERP** — `CUST_AZ12` (customer demographics), `LOC_A101` (location), `PX_CAT_G1V2` (product category)

Raw CSV files live in [`datasets/`](datasets/).

---

## Gold layer — star schema

The gold layer exposes business-ready dimensional models as views:

| View | Type | Description |
|---|---|---|
| `gold.dim_customers` | Dimension | Customer master with surrogate `customer_key`. Blends CRM as the primary source (gender) with ERP fallback (birthdate, country). |
| `gold.dim_products` | Dimension | Active products (where `prd_end_dt IS NULL`) joined to ERP category data. |
| `gold.fact_sales` | Fact | Order-grain sales fact joined to the customer and product dimensions on surrogate keys. |

See [`scripts/gold/ddl_gold.sql`](scripts/gold/ddl_gold.sql) for the full DDL.

---

## Repo layout

```
sql-data-warehouse-project/
├── datasets/
│   ├── source_crm/        # CRM source CSVs
│   └── source_erp/        # ERP source CSVs
├── docs/
│   ├── data_architecture.png   # Layered architecture diagram
│   ├── data_flow.png           # End-to-end data flow
│   ├── data_integration.png    # CRM + ERP integration view
│   ├── data_model.png          # Gold-layer star schema
│   ├── ETL.png                 # ETL pattern overview
│   ├── data_catalog.md         # Field-level catalog of the gold layer
│   └── naming_conventions.md   # Schema / table / column / SP naming rules
├── scripts/
│   ├── init_database.sql       # Creates DataWarehouse DB + bronze/silver/gold schemas
│   ├── bronze/
│   │   ├── ddl_bronze.sql      # Bronze table DDL
│   │   └── proc_load_bronze.sql# Bronze load stored procedure
│   ├── silver/
│   │   ├── ddl_silver.sql      # Silver table DDL
│   │   └── proc_load_silver.sql# Silver transform + load stored procedure
│   └── gold/
│       └── ddl_gold.sql        # Gold-layer view DDL (dim + fact)
└── tests/
    ├── quality_checks_silver.sql
    └── quality_checks_gold.sql
```

---

## How to run

> Prerequisites — SQL Server (or SQL Server Express / Azure SQL) and SSMS / Azure Data Studio. Update the bronze `BULK INSERT` paths in [`proc_load_bronze.sql`](scripts/bronze/proc_load_bronze.sql) to point at your local clone of the `datasets/` folder.

```sql
-- 1. Create the database and bronze/silver/gold schemas (DROPS existing DataWarehouse DB)
:r scripts/init_database.sql

-- 2. Create bronze + silver tables
:r scripts/bronze/ddl_bronze.sql
:r scripts/silver/ddl_silver.sql

-- 3. Load bronze (raw CSV ingest), then silver (transformed)
EXEC bronze.load_bronze;
EXEC silver.load_silver;

-- 4. Create the gold-layer star-schema views
:r scripts/gold/ddl_gold.sql

-- 5. Run quality checks
:r tests/quality_checks_silver.sql
:r tests/quality_checks_gold.sql
```

---

## Conventions

- **snake_case** for all object names. English only. No reserved words.
- **Bronze + Silver**: `<sourcesystem>_<entity>` matching the source name (e.g. `crm_cust_info`).
- **Gold**: `<category>_<entity>` where category is `dim_`, `fact_`, or `agg_` (e.g. `dim_customers`, `fact_sales`).
- **Surrogate keys**: `<table>_key` (e.g. `customer_key`).
- **Technical columns**: `dwh_<name>` (e.g. `dwh_load_date`).
- **Load procedures**: `load_<layer>` (e.g. `silver.load_silver`).

Full rules in [`docs/naming_conventions.md`](docs/naming_conventions.md).

---

## License

MIT — see [LICENSE](LICENSE).
