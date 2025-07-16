-- models/staging/sales/stg_sales_orderheader_corrected.sql
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw_adventureworks', 'raw_api_salesorderheader') }}
),

cleaned_and_enhanced AS (
    SELECT
        -- IDs principais
        SalesOrderID AS sales_order_id,
        CustomerID AS customer_id,
        SalesPersonID AS sales_person_id,
        TerritoryID AS territory_id,
        
        -- Datas
        CAST(OrderDate AS DATE) AS order_date,
        CAST(DueDate AS DATE) AS due_date,
        CAST(ShipDate AS DATE) AS ship_date,
        
        -- Status e flags
        Status AS status,
        OnlineOrderFlag AS online_order_flag,
        
        -- Valores financeiros
        CAST(SubTotal AS DECIMAL(15,4)) AS subtotal,
        CAST(TaxAmt AS DECIMAL(15,4)) AS tax_amount,
        CAST(Freight AS DECIMAL(15,4)) AS freight_amount,
        CAST(TotalDue AS DECIMAL(15,4)) AS total_due,
        
        -- Metadados
        ModifiedDate AS modified_date,
        
        -- Chave surrogada
        {{ dbt_utils.generate_surrogate_key(['SalesOrderID']) }} AS sales_order_sk
        
    FROM source
    WHERE SalesOrderID IS NOT NULL
      AND CustomerID IS NOT NULL
),

calculated_metrics AS (
    SELECT
        *,
        -- Cálculos de tempo
        DATEDIFF(due_date, order_date) AS days_to_due,
        DATEDIFF(ship_date, order_date) AS days_to_ship,
        
        -- Status descritivo
        CASE 
            WHEN status = 1 THEN 'In Process'
            WHEN status = 2 THEN 'Approved'
            WHEN status = 3 THEN 'Backordered'
            WHEN status = 4 THEN 'Rejected'
            WHEN status = 5 THEN 'Shipped'
            WHEN status = 6 THEN 'Cancelled'
            ELSE 'Unknown'
        END AS order_status_desc,
        
        -- Categorização de valor
        CASE 
            WHEN total_due > 10000 THEN 'High Value'
            WHEN total_due > 1000 THEN 'Medium Value'
            WHEN total_due > 100 THEN 'Low Value'
            ELSE 'Micro Value'
        END AS order_value_tier,
        
        -- Canal de vendas
        CASE 
            WHEN online_order_flag = true THEN 'Online'
            ELSE 'Sales Rep'
        END AS sales_channel,
        
        -- Indicadores de qualidade
        CASE 
            WHEN total_due <= 0 OR subtotal <= 0 THEN TRUE
            WHEN due_date < order_date THEN TRUE
            ELSE FALSE
        END AS has_data_quality_issues,
        
        -- Indicadores de performance
        CASE 
            WHEN ship_date IS NOT NULL AND ship_date <= due_date THEN TRUE
            ELSE FALSE
        END AS on_time_delivery,
        
        -- Categorização de complexidade fiscal
        CASE 
            WHEN tax_amount > 0 AND freight_amount > 0 THEN 'Full Tax & Freight'
            WHEN tax_amount > 0 THEN 'Tax Only'
            WHEN freight_amount > 0 THEN 'Freight Only'
            ELSE 'No Additional Charges'
        END AS tax_freight_category
        
    FROM cleaned_and_enhanced
)

SELECT * FROM calculated_metrics