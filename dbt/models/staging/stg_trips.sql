with source as (
    select *
    from {{ source('raw', 'trips') }}
),

typed as (
    select
        cast(trip_id as string) as trip_id,
        cast(pickup_ts as timestamp_ntz) as pickup_ts,
        cast(dropoff_ts as timestamp_ntz) as dropoff_ts,
        cast(vendor_id as string) as vendor_id,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as float) as trip_distance,
        cast(fare_amount as float) as fare_amount,
        cast(tip_amount as float) as tip_amount,
        cast(total_amount as float) as total_amount,
        cast(pickup_borough as string) as pickup_borough,
        cast(dropoff_borough as string) as dropoff_borough,
        cast(payment_type as string) as payment_type,
        cast(load_ts as timestamp_ntz) as load_ts
    from source
),

cleaned as (
    select
        trip_id,
        pickup_ts,
        dropoff_ts,
        load_ts,
        coalesce(nullif(trim(vendor_id), ''), 'UNKNOWN') as vendor_id,
        greatest(passenger_count, 0) as passenger_count,
        greatest(trip_distance, 0) as trip_distance,
        greatest(fare_amount, 0) as fare_amount,
        greatest(tip_amount, 0) as tip_amount,
        greatest(total_amount, 0) as total_amount,
        coalesce(nullif(trim(pickup_borough), ''), 'Unknown') as pickup_borough,
        coalesce(nullif(trim(dropoff_borough), ''), 'Unknown') as dropoff_borough,
        case
            when upper(payment_type) in ('CARD', 'CREDIT', 'CREDIT_CARD') then 'Card'
            when upper(payment_type) = 'CASH' then 'Cash'
            else 'Other'
        end as payment_type
    from typed
    where
        trip_id is not null
        and pickup_ts is not null
        and dropoff_ts is not null
),

final as (
    select
        trip_id,
        pickup_ts,
        dropoff_ts,
        vendor_id,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        pickup_borough,
        dropoff_borough,
        payment_type,
        load_ts,
        cast(pickup_ts as date) as pickup_date,
        greatest(datediff('minute', pickup_ts, dropoff_ts), 0) as trip_minutes
    from cleaned
)

select
    trip_id,
    pickup_ts,
    dropoff_ts,
    vendor_id,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    pickup_borough,
    dropoff_borough,
    payment_type,
    load_ts,
    trip_minutes,
    pickup_date,
    case
        when trip_minutes = 0 then 0
        else round(trip_distance / (trip_minutes / 60.0), 2)
    end as avg_speed_mph
from final
