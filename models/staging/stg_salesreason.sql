with source_data as (
    select
        salesreasonid
        , name as reason_name
        , reasontype
        , modifieddate
    from {{ source('adventureworks-erp', 'salesreason') }}
)
select *
from source_data