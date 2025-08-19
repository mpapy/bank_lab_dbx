# Raiffeisen Bank Data Platform

## Overview

Simple Bronze-Silver-Gold data platform for Raiffeisen Bank with row-level security and column masking.

## Architecture

```
Volume Storage → Bronze Layer → Silver Layer → Gold Layer
     ↓              ↓             ↓            ↓
Landing Data → Raw Data → Clean Data → Analytics
```

### Layers

#### Bronze Layer (`pipelines/bronze_pipeline.ipynb`)
- Raw data ingestion from source systems
- Minimal transformation, just adding metadata
- Customers, accounts, cards, transactions, etc.

#### Silver Layer (`pipelines/silver_pipeline.ipynb`) 
- Cleaned and validated data
- Business rules applied
- SCD Type 2 for historical tracking
- Data quality checks

#### Gold Layer (`pipelines/gold_pipeline.ipynb`)
- Business-ready analytics tables
- Aggregated metrics and KPIs
- Customer analytics and transaction summaries

## Security

### Row-Level Security (`security/row_level_security.sql`)
- **Executives**: See all data from all regions
- **Managers**: See only data from their assigned regions  
- **Analysts**: See all regions but limited time period (90 days)

### Column Masking
- **Names**: Full → First letter + *** → ***
- **Emails**: Full → Partial masking → Complete masking
- **Phone**: Full → Last 3 digits → Complete masking
- **Financial amounts**: Full → Full → Rounded to thousands
- **SSN**: Executives only → Masked for others

### User Groups
| Group | Data Access | Regional Filter | Time Filter |
|-------|-------------|----------------|-------------|
| EXECUTIVE | Full | All regions | All time |
| MANAGER | Partial masking | Assigned regions only | All time |
| ANALYST | Heavy masking | All regions | Last 90 days |

## Getting Started

### Setup
1. Create catalog and schemas:
```sql
CREATE CATALOG IF NOT EXISTS raiffka_catalog;
CREATE SCHEMA IF NOT EXISTS raiffka_catalog.dev_bronze;
CREATE SCHEMA IF NOT EXISTS raiffka_catalog.dev_silver;
CREATE SCHEMA IF NOT EXISTS raiffka_catalog.dev_gold;
CREATE SCHEMA IF NOT EXISTS raiffka_catalog.security;
```

2. Deploy the three DLT pipelines:
   - Bronze pipeline: `pipelines/bronze_pipeline.ipynb`
   - Silver pipeline: `pipelines/silver_pipeline.ipynb`  
   - Gold pipeline: `pipelines/gold_pipeline.ipynb`

3. Setup security:
```sql
%run security/row_level_security.sql
```

### Usage
- Use the secure views for data access:
  - `raiffka_catalog.security.secure_customers`
  - `raiffka_catalog.security.secure_transactions`
- Data is automatically filtered and masked based on your user group

## Data Sources
- Customer data, accounts, cards, transactions
- Employee and branch information
- Geographic and product data

That's it! Simple Bronze-Silver-Gold with security.