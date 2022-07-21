with source_data as (
    select
        businessentityid
        , name as storename
        , salespersonid
        , modifieddate
    from {{ source('adventureworks-erp', 'store') }}
)
select *
from source_data	