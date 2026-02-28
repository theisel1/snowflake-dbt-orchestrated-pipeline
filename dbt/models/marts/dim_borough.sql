with boroughs as (
    select pickup_borough as borough_name
    from {{ ref('stg_trips') }}

    union distinct

    select dropoff_borough as borough_name
    from {{ ref('stg_trips') }}
),

distinct_boroughs as (
    select distinct borough_name
    from boroughs
    where borough_name is not null
)

select
    borough_name,
    md5(coalesce(cast(borough_name as string), '')) as borough_key
from distinct_boroughs
