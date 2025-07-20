-- models/staging/humanresources/stg_employees.sql
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw_adventureworks', 'raw_db_humanresources_employee') }}
),

renamed_and_cleaned AS (
    SELECT
        -- IDs
        BusinessEntityID AS employee_id,
        
        -- Informações do funcionário
        NationalIDNumber AS national_id_number,
        LoginID AS login_id,
        JobTitle AS job_title,
        
        -- Dados demográficos
        BirthDate AS birth_date,
        MaritalStatus AS marital_status,
        Gender AS gender,
        
        -- Dados de contratação
        HireDate AS hire_date,
        SalariedFlag AS is_salaried,
        VacationHours AS vacation_hours,
        SickLeaveHours AS sick_leave_hours,
        CurrentFlag AS is_current,
        
        -- Dados temporais
        ModifiedDate AS modified_date,
        
        -- Chave surrogada
        {{ dbt_utils.generate_surrogate_key(['BusinessEntityID']) }} AS employee_sk
        
    FROM source
    WHERE BusinessEntityID IS NOT NULL
),

calculated_metrics AS (
    SELECT
        *,
        -- Cálculos de idade e tempo de serviço
        DATEDIFF(YEAR, birth_date, CURRENT_DATE()) AS age_years,
        DATEDIFF(YEAR, hire_date, CURRENT_DATE()) AS years_of_service,
        
        -- Categorização de idade
        CASE 
            WHEN DATEDIFF(YEAR, birth_date, CURRENT_DATE()) < 30 THEN 'Young'
            WHEN DATEDIFF(YEAR, birth_date, CURRENT_DATE()) < 50 THEN 'Middle Age'
            ELSE 'Senior'
        END AS age_category,
        
        -- Categorização de tempo de serviço
        CASE 
            WHEN DATEDIFF(YEAR, hire_date, CURRENT_DATE()) < 2 THEN 'New Employee'
            WHEN DATEDIFF(YEAR, hire_date, CURRENT_DATE()) < 5 THEN 'Experienced'
            ELSE 'Veteran'
        END AS tenure_category,
        
        -- Status do funcionário
        CASE 
            WHEN is_current = 1 THEN 'Active'
            ELSE 'Inactive'
        END AS employee_status
        
    FROM renamed_and_cleaned
)

SELECT * FROM calculated_metrics