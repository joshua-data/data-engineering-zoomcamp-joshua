SELECT
    "LocationID" AS location_id,
    "Borough" AS borough,
    "Zone" AS zone,
    REPLACE(service_zone, 'Boro', 'Green') AS service_zone
FROM 
    {{ source('dimensions', 'taxi_zone_lookup') }}