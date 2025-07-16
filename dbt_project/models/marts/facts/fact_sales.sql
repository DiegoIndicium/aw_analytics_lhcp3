-- models/marts/facts/fact_sales.sql
{{ config(materialized='table') }}

WITH order_details AS (
    SELECT * FROM {{ ref('stg_sales_orderdetail') }}
),

order_headers AS (
    SELECT * FROM {{ ref('stg_sales_orderheader') }}
),

customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),

sales_facts AS (
    SELECT
        -- Chaves
        od.orderdetail_sk,
        od.sales_order_id,
        od.sales_order_detail_id,
        od.product_id,
        oh.customer_id,
        c.customer_sk,
        oh.sales_person_id,
        oh.territory_id,
        
        -- Datas (chaves de tempo)
        oh.order_date,
        oh.due_date,
        oh.ship_date,
        
        -- Métricas de quantidade
        od.order_quantity,
        
        -- Métricas financeiras do item
        od.unit_price,
        od.unit_price_discount,
        od.line_total,
        od.gross_amount,
        od.discount_amount,
        od.net_amount,
        
        -- Métricas financeiras do pedido
        oh.subtotal,
        oh.tax_amount,
        oh.freight_amount,
        oh.total_due,
        
        -- Dimensões categóricas
        od.discount_tier,
        od.quantity_category,
        oh.order_status_desc,
        oh.order_value_tier,
        oh.sales_channel,
        
        -- Métricas calculadas
        oh.days_to_due,
        oh.days_to_ship,
        
        -- Indicadores
        CASE WHEN oh.ship_date <= oh.due_date THEN 1 ELSE 0 END AS on_time_delivery,
        CASE WHEN od.unit_price_discount > 0 THEN 1 ELSE 0 END AS has_discount,
        CASE WHEN oh.order_status_desc = 'Shipped' THEN 1 ELSE 0 END AS is_shipped,
        CASE WHEN oh.order_status_desc = 'Cancelled' THEN 1 ELSE 0 END AS is_cancelled,
        
        -- Métricas de margem (assumindo custo padrão de 70% do preço)
        od.net_amount * 0.3 AS estimated_profit,
        
        -- Metadados
        od.modified_date,
        CURRENT_TIMESTAMP() AS created_at
        
    FROM order_details od
    INNER JOIN order_headers oh ON od.sales_order_id = oh.sales_order_id
    LEFT JOIN customers c ON oh.customer_id = c.customer_id
)

SELECT * FROM sales_facts