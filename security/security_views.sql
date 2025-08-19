-- ===============================================================================
--                    RAIFFEISEN BANK DATA SECURITY FRAMEWORK
-- ===============================================================================
-- 
-- This file implements comprehensive data security including:
-- - Column-level masking for PII and sensitive data
-- - Row-level security based on user groups and roles
-- - Data classification and access controls
-- - Audit logging and compliance tracking
-- 
-- Security Groups:
-- - EXECUTIVES: Full access to all data including sensitive financial metrics
-- - MANAGERS: Access to aggregated data, masked PII, regional restrictions
-- - ANALYSTS: Access to analytical data, heavily masked PII, no sensitive financials
-- - COMPLIANCE: Access to audit logs, data lineage, quality metrics
-- - EXTERNAL: Very limited access, heavily masked data only
-- 
-- ===============================================================================

-- Create security schema if not exists
CREATE SCHEMA IF NOT EXISTS raiffka_catalog.security;
USE SCHEMA raiffka_catalog.security;

-- ===============================================================================
--                           USER GROUP MANAGEMENT
-- ===============================================================================

-- User group definitions table
CREATE OR REPLACE TABLE user_groups (
    group_name STRING NOT NULL,
    group_description STRING,
    access_level INT, -- 1=EXTERNAL, 2=ANALYST, 3=MANAGER, 4=EXECUTIVE, 5=COMPLIANCE
    data_classification_access ARRAY<STRING>, -- CONFIDENTIAL, RESTRICTED, INTERNAL, PUBLIC
    geographic_restrictions ARRAY<STRING>, -- Region codes for row-level filtering
    pii_access_level STRING, -- NONE, MASKED, PARTIAL, FULL
    financial_data_access BOOLEAN,
    audit_log_access BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Insert default user groups
INSERT INTO user_groups VALUES
('EXECUTIVES', 'C-level executives with full data access', 5, array('CONFIDENTIAL', 'RESTRICTED', 'INTERNAL', 'PUBLIC'), array(), 'FULL', true, true, current_timestamp(), current_timestamp()),
('MANAGERS', 'Department managers with regional access', 4, array('RESTRICTED', 'INTERNAL', 'PUBLIC'), array(), 'PARTIAL', true, false, current_timestamp(), current_timestamp()),
('ANALYSTS', 'Data analysts with analytical access only', 3, array('INTERNAL', 'PUBLIC'), array(), 'MASKED', false, false, current_timestamp(), current_timestamp()),
('COMPLIANCE', 'Compliance officers with audit access', 4, array('CONFIDENTIAL', 'RESTRICTED', 'INTERNAL'), array(), 'PARTIAL', false, true, current_timestamp(), current_timestamp()),
('EXTERNAL', 'External users with minimal access', 1, array('PUBLIC'), array(), 'NONE', false, false, current_timestamp(), current_timestamp());

-- User to group mapping
CREATE OR REPLACE TABLE user_group_mapping (
    user_email STRING NOT NULL,
    group_name STRING NOT NULL,
    assigned_regions ARRAY<STRING>, -- Specific regions for this user
    effective_from DATE DEFAULT CURRENT_DATE(),
    effective_to DATE,
    assigned_by STRING,
    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- ===============================================================================
--                        COLUMN MASKING FUNCTIONS
-- ===============================================================================

-- Email masking function
CREATE OR REPLACE FUNCTION mask_email(email STRING, access_level STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
    if access_level == 'FULL':
        return email
    elif access_level == 'PARTIAL':
        if email and '@' in email:
            parts = email.split('@')
            username = parts[0]
            domain = parts[1]
            if len(username) > 2:
                masked_username = username[0] + '*' * (len(username) - 2) + username[-1]
            else:
                masked_username = '*' * len(username)
            return f"{masked_username}@{domain}"
        return email
    elif access_level == 'MASKED':
        return '***@***.***' if email else None
    else:  # NONE
        return None
$$;

-- Phone number masking function
CREATE OR REPLACE FUNCTION mask_phone(phone STRING, access_level STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
    if access_level == 'FULL':
        return phone
    elif access_level == 'PARTIAL':
        if phone and len(phone) > 4:
            return phone[:3] + '*' * (len(phone) - 6) + phone[-3:]
        return phone
    elif access_level == 'MASKED':
        return '***-***-***' if phone else None
    else:  # NONE
        return None
$$;

-- Name masking function
CREATE OR REPLACE FUNCTION mask_name(name STRING, access_level STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
    if access_level == 'FULL':
        return name
    elif access_level == 'PARTIAL':
        if name and len(name) > 1:
            return name[0] + '*' * (len(name) - 1)
        return name
    elif access_level == 'MASKED':
        return '***' if name else None
    else:  # NONE
        return None
$$;

-- Financial amount masking function
CREATE OR REPLACE FUNCTION mask_amount(amount DECIMAL(20,2), access_level STRING, user_group STRING)
RETURNS DECIMAL(20,2)
LANGUAGE PYTHON
AS $$
    if user_group in ['EXECUTIVES', 'MANAGERS'] and access_level in ['FULL', 'PARTIAL']:
        return amount
    elif user_group == 'ANALYSTS':
        # Round to nearest 1000 for analysts
        if amount:
            return round(float(amount) / 1000) * 1000
        return amount
    else:
        return None
$$;

-- Social security number masking
CREATE OR REPLACE FUNCTION mask_ssn(ssn STRING, access_level STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
    if access_level == 'FULL':
        return ssn
    elif access_level == 'PARTIAL':
        if ssn and len(ssn) >= 6:
            return '***' + ssn[-3:]
        return ssn
    else:
        return None
$$;

-- ===============================================================================
--                     SECURE CUSTOMER VIEW WITH MASKING
-- ===============================================================================

CREATE OR REPLACE VIEW secure_customers_view AS
SELECT 
    customer_id,
    -- Apply name masking based on current user's access level
    CASE 
        WHEN is_member('EXECUTIVES') THEN jmeno
        WHEN is_member('MANAGERS') THEN mask_name(jmeno, 'PARTIAL')
        WHEN is_member('ANALYSTS') THEN mask_name(jmeno, 'MASKED')
        WHEN is_member('COMPLIANCE') THEN mask_name(jmeno, 'PARTIAL')
        ELSE mask_name(jmeno, 'NONE')
    END AS jmeno,
    
    CASE 
        WHEN is_member('EXECUTIVES') THEN prijmeni
        WHEN is_member('MANAGERS') THEN mask_name(prijmeni, 'PARTIAL')
        WHEN is_member('ANALYSTS') THEN mask_name(prijmeni, 'MASKED')
        WHEN is_member('COMPLIANCE') THEN mask_name(prijmeni, 'PARTIAL')
        ELSE mask_name(prijmeni, 'NONE')
    END AS prijmeni,
    
    -- Email masking
    CASE 
        WHEN is_member('EXECUTIVES') THEN email
        WHEN is_member('MANAGERS') THEN mask_email(email, 'PARTIAL')
        WHEN is_member('ANALYSTS') THEN mask_email(email, 'MASKED')
        WHEN is_member('COMPLIANCE') THEN mask_email(email, 'PARTIAL')
        ELSE mask_email(email, 'NONE')
    END AS email,
    
    -- Phone masking
    CASE 
        WHEN is_member('EXECUTIVES') THEN tel_cislo
        WHEN is_member('MANAGERS') THEN mask_phone(tel_cislo, 'PARTIAL')
        WHEN is_member('ANALYSTS') THEN mask_phone(tel_cislo, 'MASKED')
        WHEN is_member('COMPLIANCE') THEN mask_phone(tel_cislo, 'PARTIAL')
        ELSE mask_phone(tel_cislo, 'NONE')
    END AS tel_cislo,
    
    -- SSN masking
    CASE 
        WHEN is_member('EXECUTIVES') THEN rodne_cislo
        WHEN is_member('COMPLIANCE') THEN mask_ssn(rodne_cislo, 'PARTIAL')
        ELSE mask_ssn(rodne_cislo, 'NONE')
    END AS rodne_cislo,
    
    -- Financial data masking
    CASE 
        WHEN is_member('EXECUTIVES') THEN prijem
        WHEN is_member('MANAGERS') THEN mask_amount(prijem, 'FULL', 'MANAGERS')
        WHEN is_member('ANALYSTS') THEN mask_amount(prijem, 'MASKED', 'ANALYSTS')
        ELSE NULL
    END AS prijem,
    
    -- Non-sensitive demographic data (available to all authorized users)
    client_age,
    pohlavi,
    mesto,
    zeme,
    
    -- Customer segments (available to business users)
    CASE 
        WHEN is_member('EXECUTIVES') OR is_member('MANAGERS') OR is_member('ANALYSTS') THEN customer_segment
        ELSE NULL
    END AS customer_segment,
    
    CASE 
        WHEN is_member('EXECUTIVES') OR is_member('MANAGERS') OR is_member('ANALYSTS') THEN risk_category
        ELSE NULL
    END AS risk_category,
    
    -- Metadata
    snapshot_date,
    current_timestamp() AS view_accessed_at,
    current_user() AS accessed_by
    
FROM raiffka_catalog.dev_silver.dim_customers_enhanced
WHERE 
    -- Row-level security: filter by region if user has geographic restrictions
    CASE 
        WHEN is_member('EXECUTIVES') THEN TRUE
        WHEN is_member('MANAGERS') THEN 
            -- Managers can see their assigned regions only
            EXISTS (
                SELECT 1 FROM user_group_mapping ugm 
                WHERE ugm.user_email = current_user() 
                AND ugm.group_name = 'MANAGERS'
                AND (array_size(ugm.assigned_regions) = 0 OR array_contains(ugm.assigned_regions, mesto))
                AND (ugm.effective_to IS NULL OR ugm.effective_to >= current_date())
            )
        ELSE TRUE -- Analysts and others see all records but with masked data
    END;

-- ===============================================================================
--                     SECURE TRANSACTION VIEW WITH MASKING
-- ===============================================================================

CREATE OR REPLACE VIEW secure_transactions_view AS
SELECT 
    transaction_id,
    transaction_type,
    customer_id,
    
    -- Amount masking based on user role
    CASE 
        WHEN is_member('EXECUTIVES') THEN amount
        WHEN is_member('MANAGERS') THEN mask_amount(amount, 'FULL', 'MANAGERS')
        WHEN is_member('ANALYSTS') THEN mask_amount(amount, 'MASKED', 'ANALYSTS')
        ELSE NULL
    END AS amount,
    
    transaction_date,
    transaction_category,
    
    -- Fraud detection info (security-sensitive)
    CASE 
        WHEN is_member('EXECUTIVES') OR is_member('COMPLIANCE') THEN fraud_risk_score
        WHEN is_member('MANAGERS') THEN 
            CASE WHEN fraud_risk_level IN ('HIGH', 'CRITICAL') THEN fraud_risk_score ELSE NULL END
        ELSE NULL
    END AS fraud_risk_score,
    
    CASE 
        WHEN is_member('EXECUTIVES') OR is_member('COMPLIANCE') THEN fraud_risk_level
        WHEN is_member('MANAGERS') THEN 
            CASE WHEN fraud_risk_level IN ('HIGH', 'CRITICAL') THEN fraud_risk_level ELSE 'NORMAL' END
        ELSE NULL
    END AS fraud_risk_level,
    
    -- Customer context (masked according to customer view rules)
    customer_segment_at_transaction,
    
    -- Metadata
    processed_at,
    current_timestamp() AS view_accessed_at,
    current_user() AS accessed_by
    
FROM raiffka_catalog.dev_silver.fact_transactions_enriched
WHERE 
    -- Row-level security based on transaction date and user role
    CASE 
        WHEN is_member('EXECUTIVES') OR is_member('COMPLIANCE') THEN TRUE
        WHEN is_member('MANAGERS') THEN transaction_date >= date_sub(current_date(), 365) -- Last year only
        WHEN is_member('ANALYSTS') THEN transaction_date >= date_sub(current_date(), 90) -- Last 3 months only
        ELSE FALSE
    END
    -- Additional geographic filtering for managers
    AND CASE 
        WHEN is_member('EXECUTIVES') THEN TRUE
        WHEN is_member('MANAGERS') THEN 
            EXISTS (
                SELECT 1 FROM secure_customers_view scv 
                WHERE scv.customer_id = fact_transactions_enriched.customer_id
            )
        ELSE TRUE
    END;

-- ===============================================================================
--                           AUDIT AND COMPLIANCE VIEWS
-- ===============================================================================

-- Data access audit log
CREATE OR REPLACE TABLE data_access_audit (
    access_id STRING DEFAULT gen_random_uuid(),
    user_email STRING NOT NULL,
    user_group STRING,
    table_accessed STRING NOT NULL,
    access_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    access_type STRING, -- SELECT, INSERT, UPDATE, DELETE
    rows_accessed BIGINT,
    columns_accessed ARRAY<STRING>,
    filters_applied STRING,
    ip_address STRING,
    session_id STRING,
    application_name STRING,
    success BOOLEAN DEFAULT TRUE,
    error_message STRING
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.logRetentionDuration' = 'interval 7 years' -- Banking compliance requirement
);

-- Compliance dashboard view
CREATE OR REPLACE VIEW compliance_dashboard AS
SELECT 
    date_trunc('day', access_timestamp) AS access_date,
    user_group,
    table_accessed,
    access_type,
    COUNT(*) AS access_count,
    COUNT(DISTINCT user_email) AS unique_users,
    SUM(rows_accessed) AS total_rows_accessed,
    COUNT(CASE WHEN success = FALSE THEN 1 END) AS failed_accesses,
    COUNT(CASE WHEN table_accessed LIKE '%customer%' THEN 1 END) AS pii_accesses,
    COUNT(CASE WHEN table_accessed LIKE '%transaction%' THEN 1 END) AS financial_accesses
FROM data_access_audit
WHERE access_timestamp >= date_sub(current_date(), 30) -- Last 30 days
GROUP BY date_trunc('day', access_timestamp), user_group, table_accessed, access_type
ORDER BY access_date DESC, access_count DESC;

-- Data classification inventory
CREATE OR REPLACE VIEW data_classification_inventory AS
SELECT 
    'raiffka_catalog.dev_silver.dim_customers_enhanced' AS table_name,
    'CONFIDENTIAL' AS data_classification,
    array('jmeno', 'prijmeni', 'email', 'tel_cislo', 'rodne_cislo') AS pii_columns,
    array('prijem', 'sum_prm_uspor_12m', 'estimated_clv_score') AS sensitive_financial_columns,
    'Customer personal and financial information' AS description,
    current_timestamp() AS last_updated

UNION ALL

SELECT 
    'raiffka_catalog.dev_silver.fact_transactions_enriched' AS table_name,
    'CONFIDENTIAL' AS data_classification,
    array('customer_id') AS pii_columns,
    array('amount', 'fraud_risk_score') AS sensitive_financial_columns,
    'Transaction data with fraud detection scores' AS description,
    current_timestamp() AS last_updated

UNION ALL

SELECT 
    'raiffka_catalog.dev_gold.executive_dashboard_kpis' AS table_name,
    'RESTRICTED' AS data_classification,
    array() AS pii_columns,
    array('metric_1', 'metric_2', 'metric_3', 'metric_4') AS sensitive_financial_columns,
    'Executive KPIs and strategic metrics' AS description,
    current_timestamp() AS last_updated;

-- ===============================================================================
--                              GRANT PERMISSIONS
-- ===============================================================================

-- Grant permissions to security schema
GRANT USAGE ON SCHEMA raiffka_catalog.security TO `EXECUTIVES`;
GRANT USAGE ON SCHEMA raiffka_catalog.security TO `MANAGERS`;
GRANT USAGE ON SCHEMA raiffka_catalog.security TO `ANALYSTS`;
GRANT USAGE ON SCHEMA raiffka_catalog.security TO `COMPLIANCE`;

-- Grant table permissions
GRANT SELECT ON TABLE raiffka_catalog.security.secure_customers_view TO `EXECUTIVES`;
GRANT SELECT ON TABLE raiffka_catalog.security.secure_customers_view TO `MANAGERS`;
GRANT SELECT ON TABLE raiffka_catalog.security.secure_customers_view TO `ANALYSTS`;
GRANT SELECT ON TABLE raiffka_catalog.security.secure_customers_view TO `COMPLIANCE`;

GRANT SELECT ON TABLE raiffka_catalog.security.secure_transactions_view TO `EXECUTIVES`;
GRANT SELECT ON TABLE raiffka_catalog.security.secure_transactions_view TO `MANAGERS`;
GRANT SELECT ON TABLE raiffka_catalog.security.secure_transactions_view TO `ANALYSTS`;

-- Compliance-specific permissions
GRANT SELECT ON TABLE raiffka_catalog.security.data_access_audit TO `COMPLIANCE`;
GRANT SELECT ON TABLE raiffka_catalog.security.compliance_dashboard TO `COMPLIANCE`;
GRANT SELECT ON TABLE raiffka_catalog.security.data_classification_inventory TO `COMPLIANCE`;
GRANT SELECT ON TABLE raiffka_catalog.security.data_classification_inventory TO `EXECUTIVES`;

-- ===============================================================================
--                                 TRIGGERS
-- ===============================================================================

-- Note: In a real implementation, you would set up triggers or use Unity Catalog's
-- built-in audit logging to automatically populate the data_access_audit table
-- whenever the secure views are accessed. This would typically be done through
-- Databricks Unity Catalog audit logs or custom logging in the application layer.