-- models/staging/customers/stg_customers.sql
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw_adventureworks', 'raw_db_sales_customer') }}
),

renamed_and_cleaned AS (
    SELECT
        -- IDs
        CustomerID AS customer_id,
        PersonID AS person_id,
        StoreID AS store_id,
        TerritoryID AS territory_id,
        
        -- Informações do cliente
        COALESCE(AccountNumber, 'UNKNOWN') AS account_number,
        
        -- Dados temporais
        ModifiedDate AS modified_date,
        
        -- Chave surrogada
        {{ dbt_utils.generate_surrogate_key(['CustomerID']) }} AS customer_sk
        
    FROM source
    WHERE CustomerID IS NOT NULL
),

categorized AS (
    SELECT
        *,
        -- Categorização por tipo de cliente
        CASE 
            WHEN person_id IS NOT NULL AND store_id IS NULL THEN 'Individual'
            WHEN store_id IS NOT NULL AND person_id IS NULL THEN 'Store_Business'
            WHEN store_id IS NOT NULL AND person_id IS NOT NULL THEN 'Store_Contact'
            ELSE 'Unknown'
        END AS customer_type,
        
        -- Segmento de negócio
        CASE 
            WHEN person_id IS NOT NULL AND store_id IS NULL THEN 'B2C'
            WHEN store_id IS NOT NULL THEN 'B2B'
            ELSE 'Unknown'
        END AS business_segment,
        
        -- Status do cliente
        CASE 
            WHEN account_number != 'UNKNOWN' THEN 'Active'
            ELSE 'Inactive'
        END AS customer_status
        
    FROM renamed_and_cleaned
)

SELECT * FROM categorized