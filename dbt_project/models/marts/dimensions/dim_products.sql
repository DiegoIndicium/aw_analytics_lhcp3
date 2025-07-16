-- models/marts/dimensions/dim_products.sql
{{ config(materialized='table') }}

WITH products AS (
    SELECT * FROM {{ source('raw_adventureworks', 'raw_db_production_product') }}
),

product_categories AS (
    SELECT * FROM {{ source('raw_adventureworks', 'raw_db_production_productcategory') }}
),

product_subcategories AS (
    SELECT * FROM {{ source('raw_adventureworks', 'raw_db_production_productsubcategory') }}
),

products_with_hierarchy AS (
    SELECT
        p.ProductID AS product_id,
        p.Name AS product_name,
        p.ProductNumber AS product_number,
        p.Color AS product_color,
        p.Size AS product_size,
        p.Weight AS product_weight,
        p.ListPrice AS list_price,
        p.StandardCost AS standard_cost,
        p.SafetyStockLevel AS safety_stock_level,
        p.ReorderPoint AS reorder_point,
        p.DaysToManufacture AS days_to_manufacture,
        p.SellStartDate AS sell_start_date,
        p.SellEndDate AS sell_end_date,
        p.DiscontinuedDate AS discontinued_date,
        p.ModifiedDate AS modified_date,
        
        -- Hierarchy information
        pc.Name AS category_name,
        psc.Name AS subcategory_name,
        
        -- Chave surrogada
        {{ dbt_utils.generate_surrogate_key(['ProductID']) }} AS product_sk
        
    FROM products p
    LEFT JOIN product_subcategories psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
    LEFT JOIN product_categories pc ON psc.ProductCategoryID = pc.ProductCategoryID
)

SELECT
    -- Chaves
    product_sk,
    product_id,
    
    -- Informações básicas do produto
    product_name,
    product_number,
    COALESCE(product_color, 'Not Specified') AS product_color,
    COALESCE(product_size, 'Not Specified') AS product_size,
    COALESCE(product_weight, 0) AS product_weight,
    
    -- Hierarquia de produtos
    COALESCE(category_name, 'Unknown') AS category_name,
    COALESCE(subcategory_name, 'Unknown') AS subcategory_name,
    
    -- Preços e custos
    list_price,
    standard_cost,
    CASE 
        WHEN standard_cost > 0 THEN (list_price - standard_cost) / standard_cost
        ELSE 0
    END AS profit_margin,
    
    -- Informações de estoque
    safety_stock_level,
    reorder_point,
    days_to_manufacture,
    
    -- Datas importantes
    sell_start_date,
    sell_end_date,
    discontinued_date,
    
    -- Status do produto
    CASE 
        WHEN discontinued_date IS NOT NULL THEN 'Discontinued'
        WHEN sell_end_date IS NOT NULL AND sell_end_date < CURRENT_DATE() THEN 'End of Sales'
        WHEN sell_start_date IS NOT NULL AND sell_start_date <= CURRENT_DATE() THEN 'Active'
        ELSE 'Unknown'
    END AS product_status,
    
    -- Categorização por preço
    CASE 
        WHEN list_price > 1000 THEN 'Premium'
        WHEN list_price > 100 THEN 'Standard'
        WHEN list_price > 0 THEN 'Budget'
        ELSE 'Free'
    END AS price_tier,
    
    -- Tipo de produto (simplificado)
    CASE 
        WHEN days_to_manufacture > 0 THEN 'Manufactured'
        ELSE 'Purchased'
    END AS product_type,
    
    -- Metadados
    modified_date,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
    
FROM products_with_hierarchy