with vendors as (
    select distinct vendor_id
    from {{ ref('stg_trips') }}
    where vendor_id is not null
)

select
    vendor_id,
    md5(coalesce(cast(vendor_id as string), '')) as vendor_key
from vendors
