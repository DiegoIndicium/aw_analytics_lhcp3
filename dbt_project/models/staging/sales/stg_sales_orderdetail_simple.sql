-- models/staging/sales/stg_sales_orderdetail_simple.sql
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw_adventureworks', 'raw_api_salesorderdetail') }}
),

basic_staging AS (
    SELECT
        -- Using column names that exist in the source based on successful model pattern
        *,
        -- Add surrogate key
        {{ dbt_utils.generate_surrogate_key(['data']) }} AS orderdetail_sk
        
    FROM source
    LIMIT 1000
)

SELECT * FROM basic_staging