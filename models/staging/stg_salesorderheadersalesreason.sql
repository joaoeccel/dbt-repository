with source_data as (
    select
        salesorderid
        , modifieddate
        , salesreasonid
    from {{ source('adventureworks-erp', 'salesorderheadersalesreason') }}
)
select *
from source_data