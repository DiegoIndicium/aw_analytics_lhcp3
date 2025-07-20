-- models/intermediate/int_sales_order_enriched.sql
{{ config(materialized='view') }}

WITH order_details AS (
    SELECT * FROM {{ ref('stg_sales__orderdetail') }}
),

order_headers AS (
    SELECT * FROM {{ ref('stg_sales__orderheader') }}
),

order_summary AS (
    SELECT
        od.sales_order_id,
        COUNT(*) AS total_line_items,
        SUM(od.order_quantity) AS total_quantity,
        SUM(od.gross_amount) AS total_gross_amount,
        SUM(od.discount_amount) AS total_discount_amount,
        SUM(od.net_amount) AS total_net_amount,
        AVG(od.unit_price) AS avg_unit_price,
        MAX(od.unit_price) AS max_unit_price,
        MIN(od.unit_price) AS min_unit_price,
        
        -- Métricas de desconto
        AVG(od.unit_price_discount) AS avg_discount_rate,
        MAX(od.unit_price_discount) AS max_discount_rate,
        
        -- Contagem por categoria de desconto
        SUM(CASE WHEN od.discount_tier = 'High Discount' THEN 1 ELSE 0 END) AS high_discount_items,
        SUM(CASE WHEN od.discount_tier = 'Medium Discount' THEN 1 ELSE 0 END) AS medium_discount_items,
        SUM(CASE WHEN od.discount_tier = 'Low Discount' THEN 1 ELSE 0 END) AS low_discount_items,
        SUM(CASE WHEN od.discount_tier = 'No Discount' THEN 1 ELSE 0 END) AS no_discount_items,
        
        -- Contagem por categoria de quantidade
        SUM(CASE WHEN od.quantity_category = 'Bulk Order' THEN 1 ELSE 0 END) AS bulk_order_items,
        SUM(CASE WHEN od.quantity_category = 'Medium Order' THEN 1 ELSE 0 END) AS medium_order_items,
        SUM(CASE WHEN od.quantity_category = 'Small Order' THEN 1 ELSE 0 END) AS small_order_items
        
    FROM order_details od
    GROUP BY od.sales_order_id
),

enriched_orders AS (
    SELECT
        oh.*,
        os.total_line_items,
        os.total_quantity,
        os.total_gross_amount,
        os.total_discount_amount,
        os.total_net_amount,
        os.avg_unit_price,
        os.max_unit_price,
        os.min_unit_price,
        os.avg_discount_rate,
        os.max_discount_rate,
        os.high_discount_items,
        os.medium_discount_items,
        os.low_discount_items,
        os.no_discount_items,
        os.bulk_order_items,
        os.medium_order_items,
        os.small_order_items,
        
        -- Categorização do pedido baseada nos detalhes
        CASE 
            WHEN os.total_net_amount > 10000 THEN 'High Value'
            WHEN os.total_net_amount > 1000 THEN 'Medium Value'
            WHEN os.total_net_amount > 100 THEN 'Low Value'
            ELSE 'Micro Value'
        END AS detailed_value_tier,
        
        CASE 
            WHEN os.total_line_items > 10 THEN 'Complex Order'
            WHEN os.total_line_items > 5 THEN 'Medium Order'
            ELSE 'Simple Order'
        END AS order_complexity,
        
        -- Indicadores de desconto
        CASE 
            WHEN os.avg_discount_rate > 0.15 THEN 'High Discount Order'
            WHEN os.avg_discount_rate > 0.05 THEN 'Medium Discount Order'
            WHEN os.avg_discount_rate > 0 THEN 'Low Discount Order'
            ELSE 'No Discount Order'
        END AS discount_profile
        
    FROM order_headers oh
    LEFT JOIN order_summary os ON oh.sales_order_id = os.sales_order_id
)

SELECT * FROM enriched_orders