-- ===============================================================================
--                    ROW-LEVEL SECURITY AND COLUMN MASKING
-- ===============================================================================
-- Simple implementation for Raiffeisen Bank data platform

-- Create user groups table
CREATE OR REPLACE TABLE raiffka_catalog.security.user_groups (
    user_email STRING,
    user_group STRING, -- EXECUTIVE, MANAGER, ANALYST
    allowed_regions ARRAY<STRING>, -- For row filtering
    access_level INT -- 1=ANALYST, 2=MANAGER, 3=EXECUTIVE
);

-- Insert sample user groups
INSERT INTO raiffka_catalog.security.user_groups VALUES
('executive@raiffeisenbank.com', 'EXECUTIVE', array(), 3),
('manager.prague@raiffeisenbank.com', 'MANAGER', array('Prague', 'Brno'), 2),
('manager.bratislava@raiffeisenbank.com', 'MANAGER', array('Bratislava', 'Kosice'), 2),
('analyst@raiffeisenbank.com', 'ANALYST', array(), 1);

-- ===============================================================================
--                           COLUMN MASKING FUNCTIONS
-- ===============================================================================

-- Mask customer names based on user access level
CREATE OR REPLACE FUNCTION mask_name(name STRING, user_group STRING)
RETURNS STRING
LANGUAGE SQL
AS $$
  CASE 
    WHEN user_group = 'EXECUTIVE' THEN name
    WHEN user_group = 'MANAGER' THEN CONCAT(LEFT(name, 1), '***')
    ELSE '***'
  END
$$;

-- Mask email addresses
CREATE OR REPLACE FUNCTION mask_email(email STRING, user_group STRING)
RETURNS STRING
LANGUAGE SQL
AS $$
  CASE 
    WHEN user_group = 'EXECUTIVE' THEN email
    WHEN user_group = 'MANAGER' THEN CONCAT(LEFT(email, 3), '***@***.com')
    ELSE '***@***.com'
  END
$$;

-- Mask phone numbers
CREATE OR REPLACE FUNCTION mask_phone(phone STRING, user_group STRING)
RETURNS STRING
LANGUAGE SQL
AS $$
  CASE 
    WHEN user_group = 'EXECUTIVE' THEN phone
    WHEN user_group = 'MANAGER' THEN CONCAT('***', RIGHT(phone, 3))
    ELSE '***-***-***'
  END
$$;

-- Mask financial amounts
CREATE OR REPLACE FUNCTION mask_amount(amount DECIMAL(20,2), user_group STRING)
RETURNS DECIMAL(20,2)
LANGUAGE SQL
AS $$
  CASE 
    WHEN user_group IN ('EXECUTIVE', 'MANAGER') THEN amount
    ELSE ROUND(amount, -3) -- Round to nearest 1000 for analysts
  END
$$;

-- ===============================================================================
--                           SECURE VIEWS WITH ROW FILTERING
-- ===============================================================================

-- Secure customers view with column masking and row filtering
CREATE OR REPLACE VIEW raiffka_catalog.security.secure_customers AS
SELECT 
    customer_id,
    -- Apply column masking based on current user's group
    mask_name(jmeno, 
        (SELECT user_group FROM raiffka_catalog.security.user_groups 
         WHERE user_email = current_user())) AS jmeno,
    mask_name(prijmeni,
        (SELECT user_group FROM raiffka_catalog.security.user_groups 
         WHERE user_email = current_user())) AS prijmeni,
    mask_email(email,
        (SELECT user_group FROM raiffka_catalog.security.user_groups 
         WHERE user_email = current_user())) AS email,
    mask_phone(tel_cislo,
        (SELECT user_group FROM raiffka_catalog.security.user_groups 
         WHERE user_email = current_user())) AS tel_cislo,
    
    -- Show sensitive data only to executives
    CASE 
        WHEN (SELECT user_group FROM raiffka_catalog.security.user_groups 
              WHERE user_email = current_user()) = 'EXECUTIVE' 
        THEN rodne_cislo 
        ELSE '***' 
    END AS rodne_cislo,
    
    -- Mask financial data
    mask_amount(prijem,
        (SELECT user_group FROM raiffka_catalog.security.user_groups 
         WHERE user_email = current_user())) AS prijem,
    
    -- Non-sensitive fields available to all
    client_age,
    pohlavi,
    mesto,
    zeme,
    snapshot_date
FROM raiffka_catalog.dev_silver.dim_customers
WHERE 
    -- Row-level filtering based on user's allowed regions
    CASE 
        WHEN (SELECT user_group FROM raiffka_catalog.security.user_groups 
              WHERE user_email = current_user()) = 'EXECUTIVE' 
        THEN TRUE  -- Executives see all regions
        
        WHEN (SELECT user_group FROM raiffka_catalog.security.user_groups 
              WHERE user_email = current_user()) = 'MANAGER'
        THEN mesto IN (
            SELECT EXPLODE(allowed_regions) 
            FROM raiffka_catalog.security.user_groups 
            WHERE user_email = current_user()
        )  -- Managers see only their regions
        
        ELSE TRUE  -- Analysts see all but with heavy masking
    END;

-- Secure transactions view with row filtering and column masking
CREATE OR REPLACE VIEW raiffka_catalog.security.secure_transactions AS
SELECT 
    transaction_id,
    customer_id,
    -- Mask transaction amounts
    mask_amount(amount,
        (SELECT user_group FROM raiffka_catalog.security.user_groups 
         WHERE user_email = current_user())) AS amount,
    transaction_date,
    transaction_type,
    
    -- Show fraud scores only to executives and managers
    CASE 
        WHEN (SELECT user_group FROM raiffka_catalog.security.user_groups 
              WHERE user_email = current_user()) IN ('EXECUTIVE', 'MANAGER')
        THEN fraud_risk_score
        ELSE NULL
    END AS fraud_risk_score,
    
    CASE 
        WHEN (SELECT user_group FROM raiffka_catalog.security.user_groups 
              WHERE user_email = current_user()) IN ('EXECUTIVE', 'MANAGER')
        THEN fraud_risk_level
        ELSE NULL
    END AS fraud_risk_level
    
FROM raiffka_catalog.dev_silver.fact_transactions_enriched t
WHERE 
    -- Row filtering: limit data access based on user role
    CASE 
        WHEN (SELECT user_group FROM raiffka_catalog.security.user_groups 
              WHERE user_email = current_user()) = 'EXECUTIVE'
        THEN TRUE  -- Executives see all transactions
        
        WHEN (SELECT user_group FROM raiffka_catalog.security.user_groups 
              WHERE user_email = current_user()) = 'MANAGER'
        THEN EXISTS (
            -- Managers see transactions only for customers in their regions
            SELECT 1 FROM raiffka_catalog.security.secure_customers c
            WHERE c.customer_id = t.customer_id
        )
        
        ELSE transaction_date >= DATE_SUB(CURRENT_DATE(), 90)  -- Analysts see only last 90 days
    END;

-- ===============================================================================
--                                 GRANTS
-- ===============================================================================

-- Grant access to secure views
GRANT SELECT ON raiffka_catalog.security.secure_customers TO `executives`;
GRANT SELECT ON raiffka_catalog.security.secure_customers TO `managers`;  
GRANT SELECT ON raiffka_catalog.security.secure_customers TO `analysts`;

GRANT SELECT ON raiffka_catalog.security.secure_transactions TO `executives`;
GRANT SELECT ON raiffka_catalog.security.secure_transactions TO `managers`;
GRANT SELECT ON raiffka_catalog.security.secure_transactions TO `analysts`;