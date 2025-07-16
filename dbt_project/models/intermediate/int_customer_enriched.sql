-- models/intermediate/int_customer_enriched.sql
{{ config(materialized='view') }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

order_summary AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(total_due) AS total_revenue,
        AVG(total_due) AS avg_order_value,
        MAX(total_due) AS max_order_value,
        MIN(total_due) AS min_order_value,
        MAX(order_date) AS last_order_date,
        MIN(order_date) AS first_order_date,
        
        -- Contagem por status do pedido
        SUM(CASE WHEN order_status_desc = 'Shipped' THEN 1 ELSE 0 END) AS shipped_orders,
        SUM(CASE WHEN order_status_desc = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
        SUM(CASE WHEN order_status_desc = 'In Process' THEN 1 ELSE 0 END) AS pending_orders,
        
        -- Contagem por canal de vendas
        SUM(CASE WHEN sales_channel = 'Online' THEN 1 ELSE 0 END) AS online_orders,
        SUM(CASE WHEN sales_channel = 'Sales Rep' THEN 1 ELSE 0 END) AS sales_rep_orders,
        
        -- Contagem por tier de valor
        SUM(CASE WHEN order_value_tier = 'High Value' THEN 1 ELSE 0 END) AS high_value_orders,
        SUM(CASE WHEN order_value_tier = 'Medium Value' THEN 1 ELSE 0 END) AS medium_value_orders,
        SUM(CASE WHEN order_value_tier = 'Low Value' THEN 1 ELSE 0 END) AS low_value_orders,
        SUM(CASE WHEN order_value_tier = 'Micro Value' THEN 1 ELSE 0 END) AS micro_value_orders
        
    FROM {{ ref('stg_sales_orderheader') }}
    GROUP BY customer_id
),

enriched_customers AS (
    SELECT
        c.*,
        COALESCE(os.total_orders, 0) AS total_orders,
        COALESCE(os.total_revenue, 0) AS total_revenue,
        COALESCE(os.avg_order_value, 0) AS avg_order_value,
        COALESCE(os.max_order_value, 0) AS max_order_value,
        COALESCE(os.min_order_value, 0) AS min_order_value,
        os.last_order_date,
        os.first_order_date,
        COALESCE(os.shipped_orders, 0) AS shipped_orders,
        COALESCE(os.cancelled_orders, 0) AS cancelled_orders,
        COALESCE(os.pending_orders, 0) AS pending_orders,
        COALESCE(os.online_orders, 0) AS online_orders,
        COALESCE(os.sales_rep_orders, 0) AS sales_rep_orders,
        COALESCE(os.high_value_orders, 0) AS high_value_orders,
        COALESCE(os.medium_value_orders, 0) AS medium_value_orders,
        COALESCE(os.low_value_orders, 0) AS low_value_orders,
        COALESCE(os.micro_value_orders, 0) AS micro_value_orders,
        
        -- Cálculo de dias desde a última compra
        CASE 
            WHEN os.last_order_date IS NOT NULL THEN DATEDIFF(DAY, os.last_order_date, CURRENT_DATE())
            ELSE NULL
        END AS days_since_last_order,
        
        -- Cálculo de período como cliente
        CASE 
            WHEN os.first_order_date IS NOT NULL AND os.last_order_date IS NOT NULL 
            THEN DATEDIFF(DAY, os.first_order_date, os.last_order_date)
            ELSE NULL
        END AS customer_lifetime_days,
        
        -- Categorização do cliente por valor
        CASE 
            WHEN COALESCE(os.total_revenue, 0) > 50000 THEN 'VIP'
            WHEN COALESCE(os.total_revenue, 0) > 10000 THEN 'Premium'
            WHEN COALESCE(os.total_revenue, 0) > 1000 THEN 'Standard'
            WHEN COALESCE(os.total_revenue, 0) > 0 THEN 'Low Value'
            ELSE 'No Purchase'
        END AS customer_value_segment,
        
        -- Categorização por frequência de compra
        CASE 
            WHEN COALESCE(os.total_orders, 0) > 10 THEN 'High Frequency'
            WHEN COALESCE(os.total_orders, 0) > 5 THEN 'Medium Frequency'
            WHEN COALESCE(os.total_orders, 0) > 1 THEN 'Low Frequency'
            WHEN COALESCE(os.total_orders, 0) = 1 THEN 'One Time'
            ELSE 'No Purchase'
        END AS purchase_frequency_segment,
        
        -- Categorização por recência
        CASE 
            WHEN DATEDIFF(DAY, os.last_order_date, CURRENT_DATE()) <= 90 THEN 'Recent'
            WHEN DATEDIFF(DAY, os.last_order_date, CURRENT_DATE()) <= 365 THEN 'Moderate'
            WHEN DATEDIFF(DAY, os.last_order_date, CURRENT_DATE()) > 365 THEN 'Dormant'
            ELSE 'No Purchase'
        END AS recency_segment,
        
        -- Canal preferido
        CASE 
            WHEN COALESCE(os.online_orders, 0) > COALESCE(os.sales_rep_orders, 0) THEN 'Online'
            WHEN COALESCE(os.sales_rep_orders, 0) > COALESCE(os.online_orders, 0) THEN 'Sales Rep'
            WHEN COALESCE(os.online_orders, 0) = COALESCE(os.sales_rep_orders, 0) AND COALESCE(os.total_orders, 0) > 0 THEN 'Mixed'
            ELSE 'Unknown'
        END AS preferred_channel
        
    FROM customers c
    LEFT JOIN order_summary os ON c.customer_id = os.customer_id
)

SELECT * FROM enriched_customers