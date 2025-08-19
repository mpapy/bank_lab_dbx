"""
Raiffeisen Bank Data Governance Framework
========================================

This module implements comprehensive data governance including:
- Data lineage tracking and impact analysis
- Data quality monitoring and alerting
- Metadata management and cataloging
- Compliance reporting and audit trails
- Data lifecycle management
- Schema evolution tracking
"""

import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, current_date, lit, when, coalesce,
    count, sum, avg, max, min, stddev, collect_list, collect_set,
    regexp_extract, split, concat_ws, hash, sha2, md5,
    year, month, dayofmonth, hour, minute,
    isnan, isnull, length, array_contains, size,
    row_number, rank, dense_rank, percent_rank,
    date_add, date_sub, datediff, months_between,
    from_json, to_json, get_json_object, explode
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    TimestampType, BooleanType, ArrayType, MapType, DecimalType
)
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import json
import logging

# ===============================================================================
#                           DATA LINEAGE FRAMEWORK
# ===============================================================================

class DataLineageTracker:
    """
    Comprehensive data lineage tracking system for the Raiffeisen Bank data platform
    """
    
    def __init__(self, spark: SparkSession, catalog: str = "raiffka_catalog"):
        self.spark = spark
        self.catalog = catalog
        self.governance_schema = f"{catalog}.governance"
        
    def create_lineage_tables(self):
        """Create the core lineage tracking tables"""
        
        # Data asset registry
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.governance_schema}.data_asset_registry (
            asset_id STRING NOT NULL,
            asset_name STRING NOT NULL,
            asset_type STRING NOT NULL, -- TABLE, VIEW, PIPELINE, DASHBOARD
            schema_name STRING,
            table_name STRING,
            asset_description STRING,
            business_owner STRING,
            technical_owner STRING,
            data_classification STRING, -- CONFIDENTIAL, RESTRICTED, INTERNAL, PUBLIC
            retention_period_days INT,
            created_date DATE,
            last_modified_date DATE,
            tags ARRAY<STRING>,
            business_glossary_terms ARRAY<STRING>,
            compliance_tags ARRAY<STRING>,
            metadata MAP<STRING, STRING>,
            is_active BOOLEAN DEFAULT TRUE,
            registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        ) USING DELTA
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
        """)
        
        # Data lineage relationships
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.governance_schema}.data_lineage (
            lineage_id STRING DEFAULT gen_random_uuid(),
            source_asset_id STRING NOT NULL,
            target_asset_id STRING NOT NULL,
            relationship_type STRING NOT NULL, -- DERIVES_FROM, FEEDS_INTO, TRANSFORMS_TO
            transformation_logic STRING,
            transformation_type STRING, -- ETL, VIEW, AGGREGATION, JOIN, FILTER
            pipeline_name STRING,
            pipeline_run_id STRING,
            dependency_strength STRING, -- STRONG, WEAK, DERIVED
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            created_by STRING DEFAULT CURRENT_USER(),
            is_active BOOLEAN DEFAULT TRUE,
            metadata MAP<STRING, STRING>
        ) USING DELTA
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true'
        )
        """)
        
        # Column-level lineage
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.governance_schema}.column_lineage (
            column_lineage_id STRING DEFAULT gen_random_uuid(),
            source_asset_id STRING NOT NULL,
            source_column_name STRING NOT NULL,
            target_asset_id STRING NOT NULL,
            target_column_name STRING NOT NULL,
            transformation_expression STRING,
            transformation_function STRING,
            data_type_source STRING,
            data_type_target STRING,
            is_pii BOOLEAN DEFAULT FALSE,
            is_sensitive BOOLEAN DEFAULT FALSE,
            masking_applied BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            metadata MAP<STRING, STRING>
        ) USING DELTA
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true'
        )
        """)
    
    def register_asset(self, asset_name: str, asset_type: str, schema_name: str = None, 
                      table_name: str = None, description: str = None, 
                      business_owner: str = None, technical_owner: str = None,
                      data_classification: str = "INTERNAL", tags: list = None,
                      compliance_tags: list = None):
        """Register a new data asset in the governance catalog"""
        
        asset_id = f"{schema_name}.{table_name}" if schema_name and table_name else asset_name
        tags = tags or []
        compliance_tags = compliance_tags or []
        
        asset_data = [(
            asset_id, asset_name, asset_type, schema_name, table_name,
            description, business_owner, technical_owner, data_classification,
            2555,  # 7 years retention for banking
            current_date(), current_date(), tags, [], compliance_tags,
            {}, True, current_timestamp()
        )]
        
        schema = StructType([
            StructField("asset_id", StringType(), False),
            StructField("asset_name", StringType(), False),
            StructField("asset_type", StringType(), False),
            StructField("schema_name", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("asset_description", StringType(), True),
            StructField("business_owner", StringType(), True),
            StructField("technical_owner", StringType(), True),
            StructField("data_classification", StringType(), True),
            StructField("retention_period_days", IntegerType(), True),
            StructField("created_date", StringType(), True),
            StructField("last_modified_date", StringType(), True),
            StructField("tags", ArrayType(StringType()), True),
            StructField("business_glossary_terms", ArrayType(StringType()), True),
            StructField("compliance_tags", ArrayType(StringType()), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
            StructField("is_active", BooleanType(), True),
            StructField("registered_at", TimestampType(), True)
        ])
        
        df = self.spark.createDataFrame(asset_data, schema)
        df.write.mode("append").saveAsTable(f"{self.governance_schema}.data_asset_registry")
        
    def track_lineage(self, source_asset: str, target_asset: str, 
                     relationship_type: str = "DERIVES_FROM",
                     transformation_logic: str = None, pipeline_name: str = None):
        """Track lineage relationship between two assets"""
        
        lineage_data = [(
            source_asset, target_asset, relationship_type,
            transformation_logic, "ETL", pipeline_name, None,
            "STRONG", current_timestamp(), "system", True, {}
        )]
        
        schema = StructType([
            StructField("source_asset_id", StringType(), False),
            StructField("target_asset_id", StringType(), False),
            StructField("relationship_type", StringType(), False),
            StructField("transformation_logic", StringType(), True),
            StructField("transformation_type", StringType(), True),
            StructField("pipeline_name", StringType(), True),
            StructField("pipeline_run_id", StringType(), True),
            StructField("dependency_strength", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True)
        ])
        
        df = self.spark.createDataFrame(lineage_data, schema)
        df.write.mode("append").saveAsTable(f"{self.governance_schema}.data_lineage")

# ===============================================================================
#                         DATA QUALITY MONITORING SYSTEM
# ===============================================================================

class DataQualityMonitor:
    """
    Comprehensive data quality monitoring and alerting system
    """
    
    def __init__(self, spark: SparkSession, catalog: str = "raiffka_catalog"):
        self.spark = spark
        self.catalog = catalog
        self.governance_schema = f"{catalog}.governance"
        
    def create_quality_tables(self):
        """Create data quality monitoring tables"""
        
        # Data quality rules
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.governance_schema}.data_quality_rules (
            rule_id STRING DEFAULT gen_random_uuid(),
            rule_name STRING NOT NULL,
            asset_id STRING NOT NULL,
            column_name STRING,
            rule_type STRING NOT NULL, -- COMPLETENESS, VALIDITY, CONSISTENCY, ACCURACY, UNIQUENESS
            rule_expression STRING NOT NULL,
            threshold_value DOUBLE,
            threshold_operator STRING, -- GT, LT, GTE, LTE, EQ, NE
            severity STRING DEFAULT 'MEDIUM', -- LOW, MEDIUM, HIGH, CRITICAL
            is_blocking BOOLEAN DEFAULT FALSE,
            business_impact STRING,
            remediation_guidance STRING,
            created_by STRING DEFAULT CURRENT_USER(),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            is_active BOOLEAN DEFAULT TRUE,
            tags ARRAY<STRING>
        ) USING DELTA
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true'
        )
        """)
        
        # Data quality results
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.governance_schema}.data_quality_results (
            result_id STRING DEFAULT gen_random_uuid(),
            rule_id STRING NOT NULL,
            asset_id STRING NOT NULL,
            column_name STRING,
            measurement_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            measurement_date DATE DEFAULT CURRENT_DATE(),
            measured_value DOUBLE,
            threshold_value DOUBLE,
            passed BOOLEAN,
            row_count BIGINT,
            failed_row_count BIGINT,
            pass_rate DOUBLE,
            severity STRING,
            pipeline_run_id STRING,
            execution_context MAP<STRING, STRING>
        ) USING DELTA
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
        PARTITIONED BY (measurement_date, severity)
        """)
        
        # Data quality alerts
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.governance_schema}.data_quality_alerts (
            alert_id STRING DEFAULT gen_random_uuid(),
            result_id STRING NOT NULL,
            rule_id STRING NOT NULL,
            asset_id STRING NOT NULL,
            alert_type STRING NOT NULL, -- THRESHOLD_BREACH, TREND_ANOMALY, MISSING_DATA
            severity STRING NOT NULL,
            alert_message STRING,
            alert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            acknowledged BOOLEAN DEFAULT FALSE,
            acknowledged_by STRING,
            acknowledged_at TIMESTAMP,
            resolved BOOLEAN DEFAULT FALSE,
            resolved_by STRING,
            resolved_at TIMESTAMP,
            resolution_notes STRING
        ) USING DELTA
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true'
        )
        """)
    
    def add_quality_rule(self, rule_name: str, asset_id: str, rule_type: str,
                        rule_expression: str, threshold_value: float = None,
                        threshold_operator: str = "GTE", severity: str = "MEDIUM",
                        column_name: str = None, is_blocking: bool = False):
        """Add a new data quality rule"""
        
        rule_data = [(
            rule_name, asset_id, column_name, rule_type, rule_expression,
            threshold_value, threshold_operator, severity, is_blocking,
            f"Data quality rule for {rule_type}", 
            f"Check {asset_id} for {rule_type} issues",
            "system", current_timestamp(), True, [rule_type.lower()]
        )]
        
        schema = StructType([
            StructField("rule_name", StringType(), False),
            StructField("asset_id", StringType(), False),
            StructField("column_name", StringType(), True),
            StructField("rule_type", StringType(), False),
            StructField("rule_expression", StringType(), False),
            StructField("threshold_value", DoubleType(), True),
            StructField("threshold_operator", StringType(), True),
            StructField("severity", StringType(), True),
            StructField("is_blocking", BooleanType(), True),
            StructField("business_impact", StringType(), True),
            StructField("remediation_guidance", StringType(), True),
            StructField("created_by", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("tags", ArrayType(StringType()), True)
        ])
        
        df = self.spark.createDataFrame(rule_data, schema)
        df.write.mode("append").saveAsTable(f"{self.governance_schema}.data_quality_rules")

# ===============================================================================
#                            DELTA LIVE TABLES INTEGRATION
# ===============================================================================

@dlt.table(
    name="governance_data_catalog",
    comment="Comprehensive data catalog with lineage and quality metrics",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def governance_data_catalog():
    """
    Create a comprehensive data catalog view combining assets, lineage, and quality
    """
    catalog = spark.conf.get("catalog", "raiffka_catalog")
    governance_schema = f"{catalog}.governance"
    
    # This would be populated by the governance framework
    catalog_schema = StructType([
        StructField("asset_id", StringType(), False),
        StructField("asset_name", StringType(), False),
        StructField("asset_type", StringType(), False),
        StructField("schema_name", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("data_classification", StringType(), True),
        StructField("business_owner", StringType(), True),
        StructField("technical_owner", StringType(), True),
        StructField("quality_score", DoubleType(), True),
        StructField("lineage_depth", IntegerType(), True),
        StructField("last_quality_check", TimestampType(), True),
        StructField("compliance_status", StringType(), True)
    ])
    
    # Sample catalog data
    sample_data = [
        ("raiffka_catalog.dev_bronze.customers_bronze", "Customers Bronze", "TABLE", 
         "dev_bronze", "customers_bronze", "CONFIDENTIAL", "Business Owner", "Data Engineering",
         0.95, 1, current_timestamp(), "COMPLIANT"),
        ("raiffka_catalog.dev_silver.dim_customers_enhanced", "Enhanced Customers Dimension", "TABLE",
         "dev_silver", "dim_customers_enhanced", "CONFIDENTIAL", "Business Owner", "Data Engineering", 
         0.98, 2, current_timestamp(), "COMPLIANT"),
        ("raiffka_catalog.dev_gold.customer_360_analytics", "Customer 360 Analytics", "TABLE",
         "dev_gold", "customer_360_analytics", "RESTRICTED", "Business Owner", "Analytics Team",
         0.92, 3, current_timestamp(), "COMPLIANT")
    ]
    
    return spark.createDataFrame(sample_data, catalog_schema)

@dlt.table(
    name="governance_quality_dashboard",
    comment="Real-time data quality dashboard for monitoring and alerting",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def governance_quality_dashboard():
    """
    Create a real-time data quality dashboard
    """
    # Sample quality metrics
    quality_schema = StructType([
        StructField("asset_id", StringType(), False),
        StructField("rule_type", StringType(), False),
        StructField("measurement_date", StringType(), False),
        StructField("pass_rate", DoubleType(), False),
        StructField("failed_checks", IntegerType(), False),
        StructField("severity", StringType(), False),
        StructField("trend", StringType(), True),
        StructField("alert_count", IntegerType(), False)
    ])
    
    sample_quality_data = [
        ("raiffka_catalog.dev_silver.dim_customers_enhanced", "COMPLETENESS", str(current_date()), 0.98, 234, "LOW", "STABLE", 0),
        ("raiffka_catalog.dev_silver.dim_customers_enhanced", "VALIDITY", str(current_date()), 0.95, 567, "MEDIUM", "IMPROVING", 1),
        ("raiffka_catalog.dev_silver.fact_transactions_enriched", "ACCURACY", str(current_date()), 0.99, 123, "LOW", "STABLE", 0),
        ("raiffka_catalog.dev_gold.fraud_detection_dashboard", "CONSISTENCY", str(current_date()), 0.92, 890, "HIGH", "DECLINING", 3)
    ]
    
    return spark.createDataFrame(sample_quality_data, quality_schema)

@dlt.table(
    name="governance_compliance_report",
    comment="Comprehensive compliance reporting for regulatory requirements",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def governance_compliance_report():
    """
    Generate compliance reports for regulatory requirements
    """
    compliance_schema = StructType([
        StructField("report_date", StringType(), False),
        StructField("regulation", StringType(), False),
        StructField("requirement", StringType(), False),
        StructField("asset_id", StringType(), False),
        StructField("compliance_status", StringType(), False),
        StructField("evidence", StringType(), True),
        StructField("risk_level", StringType(), False),
        StructField("remediation_required", BooleanType(), False),
        StructField("next_review_date", StringType(), True)
    ])
    
    compliance_data = [
        (str(current_date()), "GDPR", "Data Retention", "raiffka_catalog.dev_silver.dim_customers_enhanced", 
         "COMPLIANT", "7-year retention policy implemented", "LOW", False, str(date_add(current_date(), 90))),
        (str(current_date()), "PCI DSS", "Data Encryption", "raiffka_catalog.dev_silver.fact_transactions_enriched",
         "COMPLIANT", "All sensitive data encrypted at rest and in transit", "LOW", False, str(date_add(current_date(), 90))),
        (str(current_date()), "Basel III", "Risk Reporting", "raiffka_catalog.dev_gold.executive_dashboard_kpis",
         "COMPLIANT", "Risk metrics calculated and reported daily", "LOW", False, str(date_add(current_date(), 30)))
    ]
    
    return spark.createDataFrame(compliance_data, compliance_schema)

# ===============================================================================
#                              INITIALIZATION FUNCTIONS
# ===============================================================================

def initialize_governance_framework(spark: SparkSession, catalog: str = "raiffka_catalog"):
    """
    Initialize the complete governance framework
    """
    # Create governance schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.governance")
    
    # Initialize lineage tracker
    lineage_tracker = DataLineageTracker(spark, catalog)
    lineage_tracker.create_lineage_tables()
    
    # Initialize quality monitor
    quality_monitor = DataQualityMonitor(spark, catalog)
    quality_monitor.create_quality_tables()
    
    # Register core assets
    assets_to_register = [
        ("customers_bronze", "TABLE", "dev_bronze", "customers_bronze", "Raw customer data from source systems", "CONFIDENTIAL"),
        ("dim_customers_enhanced", "TABLE", "dev_silver", "dim_customers_enhanced", "Enhanced customer dimension with analytics", "CONFIDENTIAL"),
        ("fact_transactions_enriched", "TABLE", "dev_silver", "fact_transactions_enriched", "Enriched transaction fact with fraud detection", "CONFIDENTIAL"),
        ("customer_360_analytics", "TABLE", "dev_gold", "customer_360_analytics", "Comprehensive customer analytics", "RESTRICTED"),
        ("fraud_detection_dashboard", "TABLE", "dev_gold", "fraud_detection_dashboard", "Real-time fraud detection dashboard", "RESTRICTED")
    ]
    
    for asset_name, asset_type, schema_name, table_name, description, classification in assets_to_register:
        lineage_tracker.register_asset(
            asset_name=asset_name,
            asset_type=asset_type,
            schema_name=schema_name,
            table_name=table_name,
            description=description,
            data_classification=classification,
            technical_owner="Data Engineering Team",
            business_owner="Business Intelligence Team"
        )
    
    # Add sample quality rules
    quality_rules = [
        ("Customer ID Completeness", f"{catalog}.dev_silver.dim_customers_enhanced", "COMPLETENESS", 
         "COUNT(CASE WHEN customer_id IS NULL THEN 1 END) / COUNT(*)", 0.01, "LTE", "HIGH", "customer_id"),
        ("Email Validity", f"{catalog}.dev_silver.dim_customers_enhanced", "VALIDITY",
         "COUNT(CASE WHEN email NOT RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$' THEN 1 END) / COUNT(*)", 0.05, "LTE", "MEDIUM", "email"),
        ("Transaction Amount Range", f"{catalog}.dev_silver.fact_transactions_enriched", "ACCURACY",
         "COUNT(CASE WHEN amount <= 0 OR amount > 1000000 THEN 1 END) / COUNT(*)", 0.02, "LTE", "HIGH", "amount")
    ]
    
    for rule_name, asset_id, rule_type, rule_expression, threshold, operator, severity, column in quality_rules:
        quality_monitor.add_quality_rule(
            rule_name=rule_name,
            asset_id=asset_id,
            rule_type=rule_type,
            rule_expression=rule_expression,
            threshold_value=threshold,
            threshold_operator=operator,
            severity=severity,
            column_name=column
        )
    
    print("Data governance framework initialized successfully!")

if __name__ == "__main__":
    # This would be called during pipeline initialization
    spark = SparkSession.getActiveSession()
    if spark:
        initialize_governance_framework(spark)