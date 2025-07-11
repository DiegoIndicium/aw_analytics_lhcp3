-- Verificar se todos os valores de CLV (Customer Lifetime Value) são positivos
SELECT *
FROM {{ ref('dim_customers_enhanced') }}
WHERE lifetime_revenue < 0