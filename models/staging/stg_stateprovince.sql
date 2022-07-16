with source_data as (
    select
        stateprovinceid
        , countryregioncode
        , modifieddate
        , rowguid
        , 'name'
        , territoryid
        , isonlystateprovinceflag
        , stateprovincecode
    from {{ source('adventureworks-erp', 'stateprovince') }}
)
select *
from source_data