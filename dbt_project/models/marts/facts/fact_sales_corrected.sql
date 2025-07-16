-- models/marts/facts/fact_sales_corrected.sql
{{ config(materialized='table') }}

WITH enhanced_orders AS (
    SELECT * FROM {{ ref('int_sales_order_enhanced') }}
),

order_details AS (
    SELECT * FROM {{ ref('stg_sales_orderdetail_corrected') }}
),

sales_facts AS (
    SELECT
        -- Chaves principais
        od.orderdetail_sk,
        od.sales_order_id,
        od.sales_order_detail_id,
        od.product_id,
        eo.customer_id,
        eo.sales_person_id,
        eo.territory_id,
        
        -- Datas (dimensões de tempo)
        eo.order_date,
        eo.due_date,
        eo.ship_date,
        
        -- Métricas do item
        od.order_quantity,
        od.unit_price,
        od.unit_price_discount,
        od.line_total,
        od.gross_amount,
        od.discount_amount,
        od.net_amount,
        
        -- Métricas do pedido
        eo.subtotal,
        eo.tax_amount,
        eo.freight_amount,
        eo.total_due,
        
        -- Categorias e classificações
        od.discount_tier,
        od.quantity_category,
        eo.order_status_desc,
        eo.order_value_tier,
        eo.sales_channel,
        eo.tax_freight_category,
        eo.order_complexity,
        
        -- Métricas calculadas
        eo.days_to_due,
        eo.days_to_ship,
        eo.effective_discount_rate,
        
        -- Indicadores de performance
        eo.on_time_delivery,
        CASE WHEN od.unit_price_discount > 0 THEN 1 ELSE 0 END AS has_discount,
        CASE WHEN eo.order_status_desc = 'Shipped' THEN 1 ELSE 0 END AS is_shipped,
        CASE WHEN eo.order_status_desc = 'Cancelled' THEN 1 ELSE 0 END AS is_cancelled,
        
        -- Métricas de qualidade
        od.has_data_quality_issues AS item_quality_issues,
        eo.has_overall_quality_issues AS order_quality_issues,
        eo.quality_issue_rate,
        eo.header_detail_variance_rate,
        
        -- Score de qualidade consolidado (0-100)
        CASE 
            WHEN NOT od.has_data_quality_issues AND NOT eo.has_overall_quality_issues THEN 100
            WHEN od.has_data_quality_issues AND eo.has_overall_quality_issues THEN 30
            WHEN od.has_data_quality_issues OR eo.has_overall_quality_issues THEN 70
            ELSE 50
        END AS data_quality_score,
        
        -- Métricas de margem (assumindo custo padrão de 70% do preço)
        od.net_amount * 0.30 AS estimated_profit,
        
        -- Contexto do cliente
        eo.customer_type,
        eo.business_segment,
        eo.customer_status,
        
        -- Métricas do pedido agregadas
        eo.total_line_items,
        eo.total_quantity,
        eo.unique_products,
        eo.avg_unit_price,
        eo.max_unit_price,
        eo.min_unit_price,
        
        -- Metadados
        od.modified_date,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at
        
    FROM order_details od
    INNER JOIN enhanced_orders eo ON od.sales_order_id = eo.sales_order_id
)

SELECT * FROM sales_facts