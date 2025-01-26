WITH

CTE_taxi_trips AS (
    SELECT 
        *,
        'Yellow' AS service_type
    FROM
        {{ ref('stg_yellow_taxi_trips') }}
    UNION ALL
    SELECT 
        *,
        'Green' AS service_type
    FROM
        {{ ref('stg_green_taxi_trips') }}
)

SELECT
    FCT.trip_id,
    FCT.vendor_id,
    FCT.service_type,
    FCT.rate_code_id,

    FCT.pickup_location_id,
    DIM_PICKUP_ZONE.borough AS pickup_borough,
    DIM_PICKUP_ZONE.zone AS pickup_zone,

    FCT.dropoff_location_id,
    DIM_DROPOFF_ZONE.borough AS dropoff_borough,
    DIM_DROPOFF_ZONE.zone AS dropoff_zone,

    FCT.pickup_datetime,
    FCT.dropoff_datetime,

    FCT.store_and_fwd_flag,
    FCT.passengers_count,
    FCT.trip_distance,
    FCT.trip_type,
    FCT.fare_amount,
    FCT.extra,
    FCT.mta_tax,
    FCT.tip_amount,
    FCT.tolls_amount,
    FCT.ehail_fee,
    FCT.improvement_surcharge,
    FCT.total_amount,
    FCT.payment_type,
    FCT.payment_type_description
FROM
    CTE_taxi_trips FCT
INNER JOIN 
    {{ ref('dim_zones') }} DIM_PICKUP_ZONE
    ON FCT.pickup_location_id = DIM_PICKUP_ZONE.location_id
INNER JOIN 
    {{ ref('dim_zones') }} DIM_DROPOFF_ZONE
    ON FCT.dropoff_location_id = DIM_DROPOFF_ZONE.location_id
WHERE 
    DIM_PICKUP_ZONE.borough != 'Unknown'
    AND DIM_DROPOFF_ZONE.borough != 'Unknown'