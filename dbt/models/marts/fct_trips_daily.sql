{{
    config(
        materialized='incremental',
        unique_key=['pickup_date', 'vendor_id', 'pickup_borough'],
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns'
    )
}}

with trips as (
    select
        pickup_date,
        vendor_id,
        pickup_borough,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        trip_minutes
    from {{ ref('stg_trips') }}
),

aggregated as (
    select
        pickup_date,
        vendor_id,
        pickup_borough,
        count(*) as trips,
        sum(fare_amount) as total_fare,
        sum(tip_amount) as total_tip,
        sum(total_amount) as total_amount,
        avg(trip_distance) as avg_distance,
        avg(trip_minutes) as avg_trip_minutes
    from trips
    group by 1, 2, 3
),

final as (
    select
        a.pickup_date,
        a.vendor_id,
        v.vendor_key,
        a.pickup_borough,
        b.borough_key,
        a.trips,
        round(a.total_fare, 2) as total_fare,
        round(a.total_tip, 2) as total_tip,
        round(a.total_amount, 2) as total_amount,
        round(a.avg_distance, 2) as avg_distance,
        round(a.avg_trip_minutes, 2) as avg_trip_minutes
    from aggregated as a
    left join {{ ref('dim_vendor') }} as v
        on a.vendor_id = v.vendor_id
    left join {{ ref('dim_borough') }} as b
        on a.pickup_borough = b.borough_name
)

select *
from final
