{{
  config(
    materialized='table'
  )
}}

WITH t_data AS (
  SELECT DISTINCT 
    `ship-postal-code` AS ship_postal_code,
    `ship-city` AS ship_city,
    `ship-state` AS ship_state,
    `ship-country` AS ship_country
  FROM
    {{ source('bronze', 'amazon_sale_report') }}
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['ship_postal_code', 'ship_city', 'ship_state', 'ship_country']) }} AS shipment_id,
  *
FROM 
  t_data