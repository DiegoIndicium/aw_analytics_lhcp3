-- models/staging/humanresources/stg_employees_working.sql
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw_adventureworks', 'raw_db_humanresources_employee') }}
),

renamed_and_cleaned AS (
    SELECT
        -- IDs
        BusinessEntityID AS business_entity_id,
        NationalIDNumber AS national_id_number,
        LoginID AS login_id,
        
        -- Job information
        OrganizationLevel AS organization_level,
        JobTitle AS job_title,
        
        -- Personal information
        BirthDate AS birth_date,
        MaritalStatus AS marital_status,
        Gender AS gender,
        
        -- Employment information
        HireDate AS hire_date,
        SalariedFlag AS salaried_flag,
        VacationHours AS vacation_hours,
        SickLeaveHours AS sick_leave_hours,
        CurrentFlag AS current_flag,
        
        -- Metadata
        ModifiedDate AS modified_date,
        
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['BusinessEntityID']) }} AS employee_sk
        
    FROM source
    WHERE BusinessEntityID IS NOT NULL
),

categorized AS (
    SELECT
        *,
        -- Fix boolean comparison for current flag
        CASE 
            WHEN current_flag = true THEN true
            ELSE false
        END AS is_current,
        
        -- Employment type categorization
        CASE 
            WHEN salaried_flag = true THEN 'Salaried'
            ELSE 'Hourly'
        END AS employment_type,
        
        -- Seniority level
        CASE 
            WHEN DATEDIFF(CURRENT_DATE(), hire_date) > 3650 THEN 'Senior'
            WHEN DATEDIFF(CURRENT_DATE(), hire_date) > 1825 THEN 'Mid-level'
            ELSE 'Junior'
        END AS seniority_level,
        
        -- Age category
        CASE 
            WHEN DATEDIFF(CURRENT_DATE(), birth_date) / 365 >= 50 THEN 'Senior'
            WHEN DATEDIFF(CURRENT_DATE(), birth_date) / 365 >= 30 THEN 'Mid-career'
            ELSE 'Young Professional'
        END AS age_category
        
    FROM renamed_and_cleaned
)

SELECT * FROM categorized