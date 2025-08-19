"""
Raiffeisen Bank Data Platform Monitoring and Alerting System
===========================================================

This module implements comprehensive monitoring and alerting including:
- Real-time data quality monitoring and anomaly detection
- Pipeline performance monitoring and SLA tracking
- Business KPI monitoring and threshold alerting
- Infrastructure monitoring and resource optimization
- Fraud detection monitoring and security alerting
- Regulatory compliance monitoring and reporting
"""

import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, current_date, lit, when, coalesce,
    count, sum, avg, max, min, stddev, variance, skewness, kurtosis,
    collect_list, collect_set, array_contains, size, explode,
    regexp_extract, split, concat_ws, hash, sha2, md5,
    year, month, dayofmonth, hour, minute, second,
    isnan, isnull, length, abs, sqrt, pow, exp, log,
    row_number, rank, dense_rank, percent_rank, ntile,
    lag, lead, first, last, percentile_approx,
    date_add, date_sub, datediff, months_between,
    from_json, to_json, get_json_object, unix_timestamp,
    window, approx_count_distinct, corr, covar_samp
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
#                        REAL-TIME DATA QUALITY MONITORING
# ===============================================================================

@dlt.table(
    name="realtime_data_quality_metrics",
    comment="Real-time data quality metrics with anomaly detection",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["measurement_date", "severity_level"]
)
def realtime_data_quality_metrics():
    """
    Real-time monitoring of data quality across all layers
    """
    env = spark.conf.get("pipeline.env", "dev")
    catalog = "raiffka_catalog"
    bronze_schema = f"{env}_bronze"
    silver_schema = f"{env}_silver"
    gold_schema = f"{env}_gold"
    
    # Monitor bronze layer data quality
    customers_bronze = dlt.readStream(f"{catalog}.{bronze_schema}.customers_bronze")
    
    # Calculate real-time quality metrics
    quality_window = Window.partitionBy("_processing_date").orderBy("_ingestion_timestamp")
    
    bronze_quality = (
        customers_bronze
        .withColumn("total_records", count("*").over(quality_window))
        .withColumn("null_customer_id", 
                   sum(when(col("customer_id").isNull(), 1).otherwise(0)).over(quality_window))
        .withColumn("null_email", 
                   sum(when(col("email").isNull(), 1).otherwise(0)).over(quality_window))
        .withColumn("invalid_email", 
                   sum(when(~col("email").rlike(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'), 1).otherwise(0)).over(quality_window))
        .withColumn("duplicate_customer_id", 
                   count("customer_id").over(quality_window) - approx_count_distinct("customer_id").over(quality_window))
        
        # Calculate quality scores
        .withColumn("completeness_score", 
                   1.0 - (col("null_customer_id") + col("null_email")) / col("total_records"))
        .withColumn("validity_score",
                   1.0 - col("invalid_email") / col("total_records"))
        .withColumn("uniqueness_score",
                   1.0 - col("duplicate_customer_id") / col("total_records"))
        .withColumn("overall_quality_score",
                   (col("completeness_score") + col("validity_score") + col("uniqueness_score")) / 3)
        
        # Anomaly detection
        .withColumn("avg_quality_7d", 
                   avg("overall_quality_score").over(
                       Window.partitionBy().orderBy("_processing_date")
                       .rowsBetween(-6, 0)))
        .withColumn("stddev_quality_7d",
                   stddev("overall_quality_score").over(
                       Window.partitionBy().orderBy("_processing_date")
                       .rowsBetween(-6, 0)))
        .withColumn("quality_zscore",
                   (col("overall_quality_score") - col("avg_quality_7d")) / 
                   coalesce(col("stddev_quality_7d"), lit(0.01)))
        .withColumn("is_quality_anomaly",
                   when(abs(col("quality_zscore")) > 2, True).otherwise(False))
        
        # Severity assessment
        .withColumn("severity_level",
                   when(col("overall_quality_score") < 0.8, "CRITICAL")
                   .when(col("overall_quality_score") < 0.9, "HIGH")
                   .when(col("overall_quality_score") < 0.95, "MEDIUM")
                   .otherwise("LOW"))
        
        .select(
            lit("customers_bronze").alias("table_name"),
            lit("BRONZE").alias("layer"),
            col("_processing_date").alias("measurement_date"),
            current_timestamp().alias("measurement_timestamp"),
            col("total_records"),
            col("completeness_score"),
            col("validity_score"),
            col("uniqueness_score"),
            col("overall_quality_score"),
            col("quality_zscore"),
            col("is_quality_anomaly"),
            col("severity_level"),
            map(
                lit("null_customer_id"), col("null_customer_id").cast("string"),
                lit("null_email"), col("null_email").cast("string"),
                lit("invalid_email"), col("invalid_email").cast("string"),
                lit("duplicate_customer_id"), col("duplicate_customer_id").cast("string")
            ).alias("detailed_metrics")
        )
    )
    
    return bronze_quality

@dlt.table(
    name="pipeline_performance_monitoring",
    comment="Real-time pipeline performance monitoring with SLA tracking",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def pipeline_performance_monitoring():
    """
    Monitor pipeline performance and SLA compliance
    """
    # Sample pipeline performance metrics
    performance_schema = StructType([
        StructField("pipeline_name", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("execution_timestamp", TimestampType(), False),
        StructField("execution_duration_minutes", DoubleType(), False),
        StructField("records_processed", IntegerType(), False),
        StructField("records_per_minute", DoubleType(), False),
        StructField("sla_threshold_minutes", DoubleType(), False),
        StructField("sla_met", BooleanType(), False),
        StructField("cpu_usage_percent", DoubleType(), True),
        StructField("memory_usage_gb", DoubleType(), True),
        StructField("data_volume_gb", DoubleType(), True),
        StructField("error_count", IntegerType(), False),
        StructField("warning_count", IntegerType(), False),
        StructField("status", StringType(), False)
    ])
    
    # Sample performance data
    performance_data = [
        ("bronze_pipeline", "BRONZE", current_timestamp(), 15.5, 1000000, 64516.1, 30.0, True, 75.2, 8.5, 2.3, 0, 2, "SUCCESS"),
        ("silver_pipeline", "SILVER", current_timestamp(), 25.8, 950000, 36821.7, 45.0, True, 82.1, 12.1, 1.8, 0, 1, "SUCCESS"),
        ("gold_pipeline", "GOLD", current_timestamp(), 12.3, 500000, 40650.4, 20.0, True, 68.9, 6.2, 0.9, 0, 0, "SUCCESS")
    ]
    
    df = spark.createDataFrame(performance_data, performance_schema)
    
    return (df
        .withColumn("performance_score",
                   when(col("sla_met") & (col("error_count") == 0), 100.0)
                   .when(col("sla_met") & (col("error_count") > 0), 80.0)
                   .when(~col("sla_met") & (col("error_count") == 0), 60.0)
                   .otherwise(40.0))
        .withColumn("efficiency_score",
                   least(col("records_per_minute") / 50000 * 100, lit(100.0)))
        .withColumn("resource_efficiency",
                   (100 - col("cpu_usage_percent")) * 0.6 + 
                   (100 - col("memory_usage_gb") / 16 * 100) * 0.4)
        .withColumn("overall_health_score",
                   (col("performance_score") * 0.5 + 
                    col("efficiency_score") * 0.3 + 
                    col("resource_efficiency") * 0.2))
        .withColumn("alert_required",
                   when((col("overall_health_score") < 70) | 
                        (col("error_count") > 0) | 
                        ~col("sla_met"), True).otherwise(False))
    )

@dlt.table(
    name="business_kpi_monitoring",
    comment="Business KPI monitoring with threshold alerting",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def business_kpi_monitoring():
    """
    Monitor business KPIs and generate alerts for threshold breaches
    """
    env = spark.conf.get("pipeline.env", "dev")
    catalog = "raiffka_catalog"
    gold_schema = f"{env}_gold"
    
    # Get executive dashboard KPIs
    kpis = dlt.readStream(f"{catalog}.{gold_schema}.executive_dashboard_kpis")
    
    # Define KPI thresholds and targets
    kpi_thresholds = {
        "total_customers": {"min": 100000, "max": 2000000, "target": 500000},
        "avg_customer_lifetime_value": {"min": 1000, "max": 100000, "target": 25000},
        "total_transaction_volume": {"min": 1000000, "max": 1000000000, "target": 100000000},
        "critical_fraud_alerts": {"min": 0, "max": 1000, "target": 50}
    }
    
    return (kpis
        .withColumn("kpi_name", 
                   when(col("metric_category") == "CUSTOMERS", "total_customers")
                   .when(col("metric_category") == "TRANSACTIONS", "total_transaction_volume")
                   .otherwise("unknown"))
        .withColumn("current_value", col("metric_1"))
        .withColumn("target_value", 
                   when(col("kpi_name") == "total_customers", lit(500000))
                   .when(col("kpi_name") == "total_transaction_volume", lit(100000000))
                   .otherwise(lit(0)))
        .withColumn("min_threshold",
                   when(col("kpi_name") == "total_customers", lit(100000))
                   .when(col("kpi_name") == "total_transaction_volume", lit(1000000))
                   .otherwise(lit(0)))
        .withColumn("max_threshold",
                   when(col("kpi_name") == "total_customers", lit(2000000))
                   .when(col("kpi_name") == "total_transaction_volume", lit(1000000000))
                   .otherwise(lit(999999999)))
        
        # Calculate performance against targets
        .withColumn("target_achievement_pct",
                   (col("current_value") / col("target_value")) * 100)
        .withColumn("threshold_status",
                   when(col("current_value") < col("min_threshold"), "BELOW_MIN")
                   .when(col("current_value") > col("max_threshold"), "ABOVE_MAX")
                   .otherwise("WITHIN_RANGE"))
        
        # Generate alert conditions
        .withColumn("alert_severity",
                   when(col("threshold_status") != "WITHIN_RANGE", "HIGH")
                   .when(col("target_achievement_pct") < 80, "MEDIUM")
                   .when(col("target_achievement_pct") < 90, "LOW")
                   .otherwise("NONE"))
        .withColumn("alert_message",
                   when(col("threshold_status") == "BELOW_MIN", 
                        concat_ws(" ", lit("KPI"), col("kpi_name"), lit("below minimum threshold")))
                   .when(col("threshold_status") == "ABOVE_MAX",
                        concat_ws(" ", lit("KPI"), col("kpi_name"), lit("above maximum threshold")))
                   .when(col("target_achievement_pct") < 90,
                        concat_ws(" ", lit("KPI"), col("kpi_name"), lit("below target performance")))
                   .otherwise(""))
        
        .withColumn("monitoring_timestamp", current_timestamp())
    )

@dlt.table(
    name="fraud_monitoring_alerts",
    comment="Real-time fraud monitoring and security alerting",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def fraud_monitoring_alerts():
    """
    Real-time fraud monitoring with automated alerting
    """
    env = spark.conf.get("pipeline.env", "dev")
    catalog = "raiffka_catalog"
    gold_schema = f"{env}_gold"
    
    # Get fraud detection dashboard
    fraud_dashboard = dlt.readStream(f"{catalog}.{gold_schema}.fraud_detection_dashboard")
    
    # Real-time fraud pattern analysis
    fraud_window = Window.partitionBy("customer_id").orderBy("transaction_date")
    daily_window = Window.partitionBy("customer_id", 
                                     col("transaction_date").cast("date")).orderBy("transaction_date")
    
    return (fraud_dashboard
        # Velocity analysis
        .withColumn("transactions_today",
                   count("*").over(daily_window))
        .withColumn("amount_today",
                   sum("amount").over(daily_window))
        .withColumn("avg_amount_30d",
                   avg("amount").over(
                       Window.partitionBy("customer_id")
                       .orderBy("transaction_date")
                       .rowsBetween(-29, 0)))
        
        # Pattern analysis
        .withColumn("amount_deviation",
                   abs(col("amount") - col("avg_amount_30d")) / col("avg_amount_30d"))
        .withColumn("velocity_risk",
                   when(col("transactions_today") > 10, "HIGH")
                   .when(col("transactions_today") > 5, "MEDIUM")
                   .otherwise("LOW"))
        .withColumn("amount_risk",
                   when(col("amount_deviation") > 3, "HIGH")
                   .when(col("amount_deviation") > 1.5, "MEDIUM")
                   .otherwise("LOW"))
        
        # Composite risk scoring
        .withColumn("composite_risk_score",
                   col("fraud_risk_score") +
                   when(col("velocity_risk") == "HIGH", 20)
                   .when(col("velocity_risk") == "MEDIUM", 10).otherwise(0) +
                   when(col("amount_risk") == "HIGH", 15)
                   .when(col("amount_risk") == "MEDIUM", 8).otherwise(0))
        
        # Alert generation
        .withColumn("alert_type",
                   when(col("composite_risk_score") >= 80, "IMMEDIATE_BLOCK")
                   .when(col("composite_risk_score") >= 60, "MANUAL_REVIEW")
                   .when(col("composite_risk_score") >= 40, "ENHANCED_MONITORING")
                   .otherwise("NORMAL"))
        .withColumn("alert_priority",
                   when(col("alert_type") == "IMMEDIATE_BLOCK", 1)
                   .when(col("alert_type") == "MANUAL_REVIEW", 2)
                   .when(col("alert_type") == "ENHANCED_MONITORING", 3)
                   .otherwise(4))
        
        # Alert details
        .withColumn("alert_reasons",
                   array_remove(
                       array(
                           when(col("fraud_risk_level") == "CRITICAL", "CRITICAL_FRAUD_SCORE").otherwise(""),
                           when(col("velocity_risk") == "HIGH", "HIGH_VELOCITY").otherwise(""),
                           when(col("amount_risk") == "HIGH", "UNUSUAL_AMOUNT").otherwise(""),
                           when(col("transactions_today") > 15, "EXCESSIVE_TRANSACTIONS").otherwise("")
                       ), ""))
        
        .withColumn("monitoring_timestamp", current_timestamp())
        .withColumn("requires_immediate_action", 
                   col("alert_type").isin(["IMMEDIATE_BLOCK", "MANUAL_REVIEW"]))
        
        .filter(col("alert_type") != "NORMAL")  # Only output alerts that require attention
    )

@dlt.table(
    name="compliance_monitoring_dashboard",
    comment="Regulatory compliance monitoring and reporting",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def compliance_monitoring_dashboard():
    """
    Monitor regulatory compliance across all data assets
    """
    # Sample compliance monitoring data
    compliance_schema = StructType([
        StructField("regulation", StringType(), False),
        StructField("requirement", StringType(), False),
        StructField("asset_name", StringType(), False),
        StructField("compliance_status", StringType(), False),
        StructField("last_audit_date", StringType(), False),
        StructField("next_audit_due", StringType(), False),
        StructField("risk_level", StringType(), False),
        StructField("compliance_score", DoubleType(), False),
        StructField("violations_count", IntegerType(), False),
        StructField("remediation_status", StringType(), False)
    ])
    
    compliance_data = [
        ("GDPR", "Data Retention", "dim_customers_enhanced", "COMPLIANT", str(current_date()), str(date_add(current_date(), 90)), "LOW", 95.0, 0, "N/A"),
        ("GDPR", "Right to Erasure", "fact_transactions_enriched", "COMPLIANT", str(current_date()), str(date_add(current_date(), 90)), "LOW", 98.0, 0, "N/A"),
        ("PCI DSS", "Data Encryption", "secure_transactions_view", "COMPLIANT", str(current_date()), str(date_add(current_date(), 180)), "LOW", 100.0, 0, "N/A"),
        ("Basel III", "Risk Reporting", "executive_dashboard_kpis", "COMPLIANT", str(current_date()), str(date_add(current_date(), 30)), "MEDIUM", 92.0, 1, "IN_PROGRESS"),
        ("AML", "Transaction Monitoring", "fraud_detection_dashboard", "COMPLIANT", str(current_date()), str(date_add(current_date(), 30)), "LOW", 96.0, 0, "N/A")
    ]
    
    df = spark.createDataFrame(compliance_data, compliance_schema)
    
    return (df
        .withColumn("days_to_next_audit",
                   datediff(col("next_audit_due").cast("date"), current_date()))
        .withColumn("audit_urgency",
                   when(col("days_to_next_audit") <= 7, "URGENT")
                   .when(col("days_to_next_audit") <= 30, "HIGH")
                   .when(col("days_to_next_audit") <= 90, "MEDIUM")
                   .otherwise("LOW"))
        .withColumn("overall_risk_assessment",
                   when((col("compliance_score") < 90) | (col("violations_count") > 0), "HIGH")
                   .when((col("compliance_score") < 95) | (col("audit_urgency") == "URGENT"), "MEDIUM")
                   .otherwise("LOW"))
        .withColumn("action_required",
                   when(col("overall_risk_assessment") == "HIGH", True)
                   .when(col("audit_urgency") == "URGENT", True)
                   .otherwise(False))
        .withColumn("monitoring_timestamp", current_timestamp())
    )

@dlt.table(
    name="system_health_dashboard",
    comment="Comprehensive system health monitoring dashboard",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def system_health_dashboard():
    """
    Comprehensive system health monitoring combining all monitoring aspects
    """
    # This would typically aggregate from all other monitoring tables
    health_schema = StructType([
        StructField("component", StringType(), False),
        StructField("category", StringType(), False),
        StructField("health_score", DoubleType(), False),
        StructField("status", StringType(), False),
        StructField("alert_count", IntegerType(), False),
        StructField("critical_issues", IntegerType(), False),
        StructField("last_updated", TimestampType(), False),
        StructField("trend", StringType(), True),
        StructField("sla_compliance", DoubleType(), True)
    ])
    
    health_data = [
        ("Bronze Layer", "DATA_QUALITY", 96.5, "HEALTHY", 2, 0, current_timestamp(), "STABLE", 99.2),
        ("Silver Layer", "DATA_QUALITY", 98.1, "HEALTHY", 1, 0, current_timestamp(), "IMPROVING", 99.8),
        ("Gold Layer", "DATA_QUALITY", 94.3, "HEALTHY", 3, 0, current_timestamp(), "STABLE", 98.5),
        ("Pipeline Performance", "PERFORMANCE", 92.7, "HEALTHY", 1, 0, current_timestamp(), "STABLE", 97.3),
        ("Fraud Detection", "SECURITY", 99.1, "HEALTHY", 5, 2, current_timestamp(), "STABLE", 99.9),
        ("Compliance", "GOVERNANCE", 95.8, "HEALTHY", 1, 0, current_timestamp(), "STABLE", 98.1)
    ]
    
    df = spark.createDataFrame(health_data, health_schema)
    
    return (df
        .withColumn("overall_system_health",
                   avg("health_score").over(Window.partitionBy()))
        .withColumn("total_alerts",
                   sum("alert_count").over(Window.partitionBy()))
        .withColumn("total_critical_issues",
                   sum("critical_issues").over(Window.partitionBy()))
        .withColumn("system_status",
                   when(col("total_critical_issues") > 0, "CRITICAL")
                   .when(col("overall_system_health") < 90, "DEGRADED")
                   .when(col("total_alerts") > 10, "WARNING")
                   .otherwise("HEALTHY"))
        .withColumn("monitoring_timestamp", current_timestamp())
    )

# ===============================================================================
#                            ALERTING CONFIGURATION
# ===============================================================================

# Alert routing configuration
ALERT_ROUTING = {
    "CRITICAL": {
        "channels": ["email", "slack", "pagerduty"],
        "recipients": ["data-engineering-oncall@raiffeisenbank.com", "#critical-alerts"],
        "escalation_minutes": 15
    },
    "HIGH": {
        "channels": ["email", "slack"],
        "recipients": ["data-engineering@raiffeisenbank.com", "#data-alerts"],
        "escalation_minutes": 60
    },
    "MEDIUM": {
        "channels": ["slack"],
        "recipients": ["#data-monitoring"],
        "escalation_minutes": 240
    },
    "LOW": {
        "channels": ["dashboard"],
        "recipients": [],
        "escalation_minutes": 1440
    }
}

# SLA definitions
SLA_DEFINITIONS = {
    "bronze_pipeline": {"max_duration_minutes": 30, "availability_target": 99.5},
    "silver_pipeline": {"max_duration_minutes": 45, "availability_target": 99.0},
    "gold_pipeline": {"max_duration_minutes": 20, "availability_target": 98.5},
    "fraud_detection": {"max_latency_seconds": 30, "accuracy_target": 95.0},
    "data_quality": {"min_score": 90.0, "availability_target": 99.9}
}