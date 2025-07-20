-- models/staging/sales/stg_sales_orderheader.sql
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw_adventureworks', 'raw_api_salesorderheader') }}
),

renamed_and_cleaned AS (
    SELECT
        -- IDs
        SalesOrderID AS sales_order_id,
        CustomerID AS customer_id,
        SalesPersonID AS sales_person_id,
        TerritoryID AS territory_id,
        
        -- Informações do pedido
        OrderDate AS order_date,
        DueDate AS due_date,
        ShipDate AS ship_date,
        Status AS order_status,
        
        -- Valores financeiros
        CAST(SubTotal AS DECIMAL(15,4)) AS subtotal,
        CAST(TaxAmt AS DECIMAL(15,4)) AS tax_amount,
        CAST(Freight AS DECIMAL(15,4)) AS freight_amount,
        CAST(TotalDue AS DECIMAL(15,4)) AS total_due,
        
        -- Dados temporais
        ModifiedDate AS modified_date,
        
        -- Chave surrogada
        {{ dbt_utils.generate_surrogate_key(['SalesOrderID']) }} AS order_sk
        
    FROM source
    WHERE SalesOrderID IS NOT NULL
),

calculated_metrics AS (
    SELECT
        *,
        -- Cálculos de negócio
        DATEDIFF(DAY, order_date, due_date) AS days_to_due,
        DATEDIFF(DAY, order_date, ship_date) AS days_to_ship,
        
        -- Categorização do valor do pedido
        CASE 
            WHEN total_due > 10000 THEN 'High Value'
            WHEN total_due > 1000 THEN 'Medium Value'
            WHEN total_due > 100 THEN 'Low Value'
            ELSE 'Micro Value'
        END AS order_value_tier,
        
        -- Status do pedido
        CASE 
            WHEN order_status = 1 THEN 'In Process'
            WHEN order_status = 2 THEN 'Approved'
            WHEN order_status = 3 THEN 'Backordered'
            WHEN order_status = 4 THEN 'Rejected'
            WHEN order_status = 5 THEN 'Shipped'
            WHEN order_status = 6 THEN 'Cancelled'
            ELSE 'Unknown'
        END AS order_status_desc,
        
        -- Canal de vendas (baseado na presença de SalesPersonID)
        CASE 
            WHEN sales_person_id IS NOT NULL THEN 'Sales Rep'
            ELSE 'Online'
        END AS sales_channel
        
    FROM renamed_and_cleaned
)

SELECT * FROM calculated_metrics