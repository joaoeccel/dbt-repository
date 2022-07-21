with source_data as (
    select
        businessentityid
        , addressid
        , addresstypeid
        , rowguid
        , modifieddate
    from {{ source('adventureworks-erp', 'businessentityaddress') }}
)
select *
from source_data