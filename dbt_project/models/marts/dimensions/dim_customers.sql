-- models/marts/dimensions/dim_customers.sql
{{ config(materialized='table') }}

WITH enriched_customers AS (
    SELECT * FROM {{ ref('int_customer_enriched') }}
),

final AS (
    SELECT
        -- Chaves
        customer_sk,
        customer_id,
        
        -- Informações básicas do cliente
        account_number,
        customer_type,
        business_segment,
        customer_status,
        territory_id,
        
        -- Métricas de compras
        total_orders,
        total_revenue,
        avg_order_value,
        max_order_value,
        min_order_value,
        
        -- Datas importantes
        first_order_date,
        last_order_date,
        days_since_last_order,
        customer_lifetime_days,
        
        -- Contadores por categoria
        shipped_orders,
        cancelled_orders,
        pending_orders,
        online_orders,
        sales_rep_orders,
        high_value_orders,
        medium_value_orders,
        low_value_orders,
        micro_value_orders,
        
        -- Segmentação de clientes
        customer_value_segment,
        purchase_frequency_segment,
        recency_segment,
        preferred_channel,
        
        -- Indicadores calculados
        CASE 
            WHEN total_orders > 0 THEN CAST(cancelled_orders AS DOUBLE) / CAST(total_orders AS DOUBLE)
            ELSE 0
        END AS cancellation_rate,
        
        CASE 
            WHEN total_orders > 0 THEN CAST(shipped_orders AS DOUBLE) / CAST(total_orders AS DOUBLE)
            ELSE 0
        END AS fulfillment_rate,
        
        CASE 
            WHEN total_orders > 0 THEN CAST(online_orders AS DOUBLE) / CAST(total_orders AS DOUBLE)
            ELSE 0
        END AS online_preference_rate,
        
        -- Classificação RFM simplificada
        CASE 
            WHEN recency_segment = 'Recent' AND purchase_frequency_segment IN ('High Frequency', 'Medium Frequency') AND customer_value_segment IN ('VIP', 'Premium') THEN 'Champions'
            WHEN recency_segment = 'Recent' AND customer_value_segment IN ('VIP', 'Premium') THEN 'Loyal Customers'
            WHEN recency_segment = 'Recent' AND purchase_frequency_segment IN ('High Frequency', 'Medium Frequency') THEN 'Potential Loyalists'
            WHEN recency_segment = 'Recent' THEN 'New Customers'
            WHEN customer_value_segment IN ('VIP', 'Premium') THEN 'At Risk'
            WHEN recency_segment = 'Moderate' THEN 'Cannot Lose Them'
            ELSE 'Hibernating'
        END AS rfm_segment,
        
        -- Metadados
        modified_date,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at
        
    FROM enriched_customers
)

SELECT * FROM final