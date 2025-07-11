-- Verificar se os estágios de ciclo de vida dos produtos são válidos
SELECT *
FROM {{ ref('dim_products_performance') }}
WHERE lifecycle_stage NOT IN ('Crescimento', 'Maturidade', 'Declínio', 'Descontinuado')