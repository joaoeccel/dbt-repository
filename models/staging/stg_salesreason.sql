with source_data as (
    select
        reasontype
        , modifieddate
        , 'name'
        , salesreasonid
    from {{ source('adventureworks-erp', 'salesreason') }}
)
select *
from source_data