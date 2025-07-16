-- models/marts/facts/fact_sales_summary.sql
{{ config(materialized='table') }}

WITH daily_sales AS (
    SELECT
        order_date,
        customer_id,
        territory_id,
        sales_channel,
        
        -- Métricas agregadas
        COUNT(DISTINCT sales_order_id) AS total_orders,
        COUNT(*) AS total_line_items,
        SUM(order_quantity) AS total_quantity,
        SUM(net_amount) AS total_net_revenue,
        SUM(discount_amount) AS total_discount_amount,
        SUM(estimated_profit) AS total_estimated_profit,
        
        -- Métricas médias
        AVG(unit_price) AS avg_unit_price,
        AVG(net_amount) AS avg_line_value,
        AVG(unit_price_discount) AS avg_discount_rate,
        
        -- Contadores por categoria
        SUM(CASE WHEN discount_tier = 'High Discount' THEN 1 ELSE 0 END) AS high_discount_items,
        SUM(CASE WHEN discount_tier = 'Medium Discount' THEN 1 ELSE 0 END) AS medium_discount_items,
        SUM(CASE WHEN discount_tier = 'Low Discount' THEN 1 ELSE 0 END) AS low_discount_items,
        SUM(CASE WHEN discount_tier = 'No Discount' THEN 1 ELSE 0 END) AS no_discount_items,
        
        SUM(CASE WHEN quantity_category = 'Bulk Order' THEN 1 ELSE 0 END) AS bulk_order_items,
        SUM(CASE WHEN quantity_category = 'Medium Order' THEN 1 ELSE 0 END) AS medium_order_items,
        SUM(CASE WHEN quantity_category = 'Small Order' THEN 1 ELSE 0 END) AS small_order_items,
        
        -- Indicadores de performance
        SUM(on_time_delivery) AS on_time_deliveries,
        SUM(is_shipped) AS shipped_items,
        SUM(is_cancelled) AS cancelled_items,
        
        -- Chave surrogada
        {{ dbt_utils.generate_surrogate_key(['order_date', 'customer_id', 'territory_id', 'sales_channel']) }} AS sales_summary_sk
        
    FROM {{ ref('fact_sales') }}
    GROUP BY order_date, customer_id, territory_id, sales_channel
)

SELECT
    -- Chaves
    sales_summary_sk,
    order_date,
    customer_id,
    territory_id,
    sales_channel,
    
    -- Métricas de volume
    total_orders,
    total_line_items,
    total_quantity,
    
    -- Métricas financeiras
    total_net_revenue,
    total_discount_amount,
    total_estimated_profit,
    
    -- Métricas calculadas
    avg_unit_price,
    avg_line_value,
    avg_discount_rate,
    
    -- Taxa de margem estimada
    CASE 
        WHEN total_net_revenue > 0 THEN total_estimated_profit / total_net_revenue
        ELSE 0
    END AS profit_margin_rate,
    
    -- Taxa de desconto
    CASE 
        WHEN (total_net_revenue + total_discount_amount) > 0 THEN total_discount_amount / (total_net_revenue + total_discount_amount)
        ELSE 0
    END AS discount_rate,
    
    -- Contadores por categoria
    high_discount_items,
    medium_discount_items,
    low_discount_items,
    no_discount_items,
    bulk_order_items,
    medium_order_items,
    small_order_items,
    
    -- Métricas de qualidade
    on_time_deliveries,
    shipped_items,
    cancelled_items,
    
    -- Taxas de performance
    CASE 
        WHEN total_line_items > 0 THEN CAST(on_time_deliveries AS DOUBLE) / CAST(total_line_items AS DOUBLE)
        ELSE 0
    END AS on_time_delivery_rate,
    
    CASE 
        WHEN total_line_items > 0 THEN CAST(shipped_items AS DOUBLE) / CAST(total_line_items AS DOUBLE)
        ELSE 0
    END AS fulfillment_rate,
    
    CASE 
        WHEN total_line_items > 0 THEN CAST(cancelled_items AS DOUBLE) / CAST(total_line_items AS DOUBLE)
        ELSE 0
    END AS cancellation_rate,
    
    -- Metadados
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
    
FROM daily_sales