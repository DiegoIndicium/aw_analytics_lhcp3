-- models/staging/sales/stg_sales_orderdetail.sql
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw_adventureworks', 'raw_api_salesorderdetail') }}
),

renamed_and_cleaned AS (
    SELECT
        -- IDs (padronização snake_case)
        SalesOrderID AS sales_order_id,
        SalesOrderDetailID AS sales_order_detail_id,
        ProductID AS product_id,
        
        -- Quantidades e preços (casting para tipos corretos)
        CAST(OrderQty AS INTEGER) AS order_quantity,
        CAST(UnitPrice AS DECIMAL(10,4)) AS unit_price,
        CAST(UnitPriceDiscount AS DECIMAL(5,4)) AS unit_price_discount,
        CAST(LineTotal AS DECIMAL(15,4)) AS line_total,
        
        -- Dados temporais
        ModifiedDate AS modified_date,
        
        -- Chave surrogada
        {{ dbt_utils.generate_surrogate_key(['SalesOrderID', 'SalesOrderDetailID']) }} AS orderdetail_sk
        
    FROM source
    WHERE SalesOrderID IS NOT NULL AND SalesOrderDetailID IS NOT NULL
),

calculated_metrics AS (
    SELECT
        *,
        -- Cálculos de negócio
        (unit_price * order_quantity) AS gross_amount,
        (unit_price * order_quantity * unit_price_discount) AS discount_amount,
        (unit_price * order_quantity * (1 - unit_price_discount)) AS net_amount,
        
        -- Categorização de desconto
        CASE 
            WHEN unit_price_discount > 0.2 THEN 'High Discount'
            WHEN unit_price_discount > 0.1 THEN 'Medium Discount'
            WHEN unit_price_discount > 0 THEN 'Low Discount'
            ELSE 'No Discount'
        END AS discount_tier,
        
        -- Categorização de quantidade
        CASE 
            WHEN order_quantity >= 10 THEN 'Bulk Order'
            WHEN order_quantity >= 5 THEN 'Medium Order'
            ELSE 'Small Order'
        END AS quantity_category
        
    FROM renamed_and_cleaned
)

SELECT * FROM calculated_metrics