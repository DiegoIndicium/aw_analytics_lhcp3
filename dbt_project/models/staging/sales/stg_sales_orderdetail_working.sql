-- models/staging/sales/stg_sales_orderdetail_working.sql
{{ config(materialized='view') }}

WITH source AS (
    SELECT 
        explode(data) as order_detail_data
    FROM {{ source('raw_adventureworks', 'raw_api_salesorderdetail') }}
),

flattened AS (
    SELECT
        -- Extract fields from the JSON data structure
        order_detail_data.SalesOrderID AS sales_order_id,
        order_detail_data.SalesOrderDetailID AS sales_order_detail_id,
        order_detail_data.ProductID AS product_id,
        order_detail_data.OrderQty AS order_quantity,
        order_detail_data.UnitPrice AS unit_price,
        order_detail_data.UnitPriceDiscount AS unit_price_discount,
        order_detail_data.LineTotal AS line_total,
        order_detail_data.ModifiedDate AS modified_date,
        
        -- Calculate business metrics
        (order_detail_data.UnitPrice * order_detail_data.OrderQty) AS gross_amount,
        (order_detail_data.UnitPrice * order_detail_data.OrderQty * order_detail_data.UnitPriceDiscount) AS discount_amount,
        (order_detail_data.UnitPrice * order_detail_data.OrderQty * (1 - order_detail_data.UnitPriceDiscount)) AS net_amount,
        
        -- Business categorization
        CASE 
            WHEN order_detail_data.UnitPriceDiscount > 0.2 THEN 'High Discount'
            WHEN order_detail_data.UnitPriceDiscount > 0.1 THEN 'Medium Discount'
            WHEN order_detail_data.UnitPriceDiscount > 0 THEN 'Low Discount'
            ELSE 'No Discount'
        END AS discount_tier,
        
        CASE 
            WHEN order_detail_data.OrderQty >= 10 THEN 'Bulk Order'
            WHEN order_detail_data.OrderQty >= 5 THEN 'Medium Order'
            ELSE 'Small Order'
        END AS quantity_category,
        
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['order_detail_data.SalesOrderID', 'order_detail_data.SalesOrderDetailID']) }} AS orderdetail_sk
        
    FROM source
    WHERE order_detail_data.SalesOrderID IS NOT NULL
)

SELECT * FROM flattened