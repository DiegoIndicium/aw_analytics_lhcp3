-- models/intermediate/int_sales_order_enhanced.sql
{{ config(materialized='view') }}

WITH order_details AS (
    SELECT * FROM {{ ref('stg_sales_orderdetail_corrected') }}
),

order_headers AS (
    SELECT * FROM {{ ref('stg_sales_orderheader_corrected') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

order_aggregation AS (
    SELECT
        sales_order_id,
        -- Contadores
        COUNT(*) AS total_line_items,
        SUM(order_quantity) AS total_quantity,
        COUNT(DISTINCT product_id) AS unique_products,
        
        -- Valores financeiros
        SUM(gross_amount) AS total_gross_amount,
        SUM(discount_amount) AS total_discount_amount,
        SUM(net_amount) AS total_net_amount,
        
        -- Métricas de desconto
        AVG(unit_price_discount) AS avg_discount_rate,
        MAX(unit_price_discount) AS max_discount_rate,
        
        -- Categorização de desconto
        SUM(CASE WHEN discount_tier = 'High Discount' THEN 1 ELSE 0 END) AS high_discount_items,
        SUM(CASE WHEN discount_tier = 'Medium Discount' THEN 1 ELSE 0 END) AS medium_discount_items,
        SUM(CASE WHEN discount_tier = 'Low Discount' THEN 1 ELSE 0 END) AS low_discount_items,
        SUM(CASE WHEN discount_tier = 'No Discount' THEN 1 ELSE 0 END) AS no_discount_items,
        
        -- Categorização de quantidade
        SUM(CASE WHEN quantity_category = 'Bulk Order' THEN 1 ELSE 0 END) AS bulk_order_items,
        SUM(CASE WHEN quantity_category = 'Medium Order' THEN 1 ELSE 0 END) AS medium_order_items,
        SUM(CASE WHEN quantity_category = 'Small Order' THEN 1 ELSE 0 END) AS small_order_items,
        
        -- Qualidade dos dados
        SUM(CASE WHEN has_data_quality_issues THEN 1 ELSE 0 END) AS items_with_quality_issues,
        
        -- Métricas de preço
        AVG(unit_price) AS avg_unit_price,
        MAX(unit_price) AS max_unit_price,
        MIN(unit_price) AS min_unit_price
        
    FROM order_details
    GROUP BY sales_order_id
),

enriched_orders AS (
    SELECT
        oh.sales_order_sk,
        oh.sales_order_id,
        oh.customer_id,
        oh.sales_person_id,
        oh.territory_id,
        
        -- Datas
        oh.order_date,
        oh.due_date,
        oh.ship_date,
        
        -- Status e canal
        oh.order_status_desc,
        oh.sales_channel,
        oh.order_value_tier,
        oh.tax_freight_category,
        
        -- Valores do header
        oh.subtotal,
        oh.tax_amount,
        oh.freight_amount,
        oh.total_due,
        
        -- Métricas calculadas do header
        oh.days_to_due,
        oh.days_to_ship,
        oh.on_time_delivery,
        oh.has_data_quality_issues AS header_quality_issues,
        
        -- Métricas agregadas dos itens
        oa.total_line_items,
        oa.total_quantity,
        oa.unique_products,
        oa.total_gross_amount,
        oa.total_discount_amount,
        oa.total_net_amount,
        oa.avg_discount_rate,
        oa.max_discount_rate,
        oa.high_discount_items,
        oa.medium_discount_items,
        oa.low_discount_items,
        oa.no_discount_items,
        oa.bulk_order_items,
        oa.medium_order_items,
        oa.small_order_items,
        oa.items_with_quality_issues,
        oa.avg_unit_price,
        oa.max_unit_price,
        oa.min_unit_price,
        
        -- Informações do cliente
        c.customer_type,
        c.business_segment,
        c.customer_status,
        
        -- Métricas calculadas combinadas
        CASE 
            WHEN oa.total_net_amount > 0 THEN oa.total_discount_amount / oa.total_net_amount
            ELSE 0
        END AS effective_discount_rate,
        
        CASE 
            WHEN oa.total_line_items > 0 THEN CAST(oa.items_with_quality_issues AS DOUBLE) / CAST(oa.total_line_items AS DOUBLE)
            ELSE 0
        END AS quality_issue_rate,
        
        CASE 
            WHEN oh.total_due > 0 THEN ABS(oh.total_due - oa.total_net_amount) / oh.total_due
            ELSE 0
        END AS header_detail_variance_rate,
        
        -- Score de complexidade do pedido
        CASE 
            WHEN oa.unique_products > 10 AND oa.total_line_items > 20 THEN 'High Complexity'
            WHEN oa.unique_products > 5 AND oa.total_line_items > 10 THEN 'Medium Complexity'
            ELSE 'Low Complexity'
        END AS order_complexity,
        
        -- Flag de qualidade geral
        CASE 
            WHEN oh.has_data_quality_issues OR oa.items_with_quality_issues > 0 THEN TRUE
            ELSE FALSE
        END AS has_overall_quality_issues,
        
        oh.modified_date
        
    FROM order_headers oh
    INNER JOIN order_aggregation oa ON oh.sales_order_id = oa.sales_order_id
    LEFT JOIN customers c ON oh.customer_id = c.customer_id
)

SELECT * FROM enriched_orders