with source_data as (
    select
        countryregioncode
        , modifieddate
        , name as country_name
    from {{ source('adventureworks-erp', 'countryregion') }}
)
select *
from source_data