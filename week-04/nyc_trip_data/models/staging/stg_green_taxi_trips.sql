WITH 
CTE_raw AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY "VendorID", lpep_pickup_datetime) AS row_num
    FROM 
        {{ source('green_taxi_trips', 'm2024_01') }}
    WHERE 
        "VendorID" IS NOT NULL
    UNION ALL
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY "VendorID", lpep_pickup_datetime) AS row_num
    FROM 
        {{ source('green_taxi_trips', 'm2024_02') }}
    WHERE 
        "VendorID" IS NOT NULL
    UNION ALL
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY "VendorID", lpep_pickup_datetime) AS row_num
    FROM 
        {{ source('green_taxi_trips', 'm2024_03') }}
    WHERE 
        "VendorID" IS NOT NULL
    UNION ALL
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY "VendorID", lpep_pickup_datetime) AS row_num
    FROM 
        {{ source('green_taxi_trips', 'm2024_04') }}
    WHERE 
        "VendorID" IS NOT NULL
    UNION ALL
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY "VendorID", lpep_pickup_datetime) AS row_num
    FROM 
        {{ source('green_taxi_trips', 'm2024_05') }}
    WHERE 
        "VendorID" IS NOT NULL
    UNION ALL
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY "VendorID", lpep_pickup_datetime) AS row_num
    FROM 
        {{ source('green_taxi_trips', 'm2024_06') }}
    WHERE 
        "VendorID" IS NOT NULL
    UNION ALL
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY "VendorID", lpep_pickup_datetime) AS row_num
    FROM 
        {{ source('green_taxi_trips', 'm2024_07') }}
    WHERE 
        "VendorID" IS NOT NULL
    UNION ALL
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY "VendorID", lpep_pickup_datetime) AS row_num
    FROM 
        {{ source('green_taxi_trips', 'm2024_08') }}
    WHERE 
        "VendorID" IS NOT NULL
    UNION ALL
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY "VendorID", lpep_pickup_datetime) AS row_num
    FROM 
        {{ source('green_taxi_trips', 'm2024_09') }}
    WHERE 
        "VendorID" IS NOT NULL
    UNION ALL
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY "VendorID", lpep_pickup_datetime) AS row_num
    FROM 
        {{ source('green_taxi_trips', 'm2024_10') }}
    WHERE 
        "VendorID" IS NOT NULL
    UNION ALL
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY "VendorID", lpep_pickup_datetime) AS row_num
    FROM 
        {{ source('green_taxi_trips', 'm2024_11') }}
    WHERE 
        "VendorID" IS NOT NULL
)

SELECT

    -- Identifiers
    {{ dbt_utils.generate_surrogate_key(['"VendorID"', 'lpep_pickup_datetime']) }} AS trip_id,
    {{ dbt.safe_cast('"VendorID"', api.Column.translate_type("integer")) }} as vendor_id,
    {{ dbt.safe_cast('"RatecodeID"', api.Column.translate_type("integer")) }} as rate_code_id,
    {{ dbt.safe_cast('"PULocationID"', api.Column.translate_type("integer")) }} as pickup_location_id,
    {{ dbt.safe_cast('"DOLocationID"', api.Column.translate_type("integer")) }} as dropoff_location_id,

    -- Timestamps
    CAST(lpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(lpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

    -- Trip Info
    store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passengers_count,
    CAST(trip_distance AS NUMERIC) AS trip_distance,

    {{ dbt.safe_cast("trip_type", api.Column.translate_type("integer")) }} as trip_type,

    -- Payment Info
    CAST(fare_amount AS NUMERIC) AS fare_amount,
    CAST(extra AS NUMERIC) AS extra,
    CAST(mta_tax AS NUMERIC) AS mta_tax,
    CAST(tip_amount AS NUMERIC) AS tip_amount,
    CAST(tolls_amount AS NUMERIC) AS tolls_amount,
    CAST(0 AS NUMERIC) AS ehail_fee,
    CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC) AS total_amount,
    COALESCE(
        {{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }},
        0
    ) AS payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description

FROM 
    CTE_raw
WHERE
    row_num = 1
{% if var('is_test_run') %}
LIMIT
    100
{% endif %}