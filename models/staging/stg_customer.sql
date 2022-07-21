with source_data as (
    select
        customerid
        , personid
        , storeid
        , territoryid
    from {{ source('adventureworks-erp', 'customer') }}
)
select *
from source_data	