-- models/staging/sales/stg_sales_orderheader_working.sql
{{ config(materialized='view') }}

WITH source AS (
    SELECT 
        explode(data) as order_header_data
    FROM {{ source('raw_adventureworks', 'raw_api_salesorderheader') }}
),

flattened AS (
    SELECT
        -- Extract fields from the JSON data structure
        order_header_data.SalesOrderID AS sales_order_id,
        order_header_data.CustomerID AS customer_id,
        order_header_data.OrderDate AS order_date,
        order_header_data.DueDate AS due_date,
        order_header_data.ShipDate AS ship_date,
        order_header_data.Status AS status,
        order_header_data.OnlineOrderFlag AS online_order_flag,
        order_header_data.SalesPersonID AS sales_person_id,
        order_header_data.TerritoryID AS territory_id,
        order_header_data.SubTotal AS subtotal,
        order_header_data.TaxAmt AS tax_amount,
        order_header_data.Freight AS freight_amount,
        order_header_data.TotalDue AS total_due,
        order_header_data.ModifiedDate AS modified_date,
        
        -- Business calculations
        DATEDIFF(order_header_data.DueDate, order_header_data.OrderDate) AS days_to_due,
        DATEDIFF(order_header_data.ShipDate, order_header_data.OrderDate) AS days_to_ship,
        
        -- Status categorization
        CASE 
            WHEN order_header_data.Status = 1 THEN 'In Process'
            WHEN order_header_data.Status = 2 THEN 'Approved'
            WHEN order_header_data.Status = 3 THEN 'Backordered'
            WHEN order_header_data.Status = 4 THEN 'Rejected'
            WHEN order_header_data.Status = 5 THEN 'Shipped'
            WHEN order_header_data.Status = 6 THEN 'Cancelled'
            ELSE 'Unknown'
        END AS order_status_desc,
        
        -- Value tier categorization
        CASE 
            WHEN order_header_data.TotalDue > 10000 THEN 'High Value'
            WHEN order_header_data.TotalDue > 1000 THEN 'Medium Value'
            WHEN order_header_data.TotalDue > 100 THEN 'Low Value'
            ELSE 'Micro Value'
        END AS order_value_tier,
        
        -- Sales channel
        CASE 
            WHEN order_header_data.OnlineOrderFlag = true THEN 'Online'
            ELSE 'Sales Rep'
        END AS sales_channel,
        
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['order_header_data.SalesOrderID']) }} AS sales_order_sk
        
    FROM source
    WHERE order_header_data.SalesOrderID IS NOT NULL
)

SELECT * FROM flattened