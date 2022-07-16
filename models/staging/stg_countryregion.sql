with source_data as (
    select
        countryregioncode
        , modifieddate
        , 'name'
    from {{ source('adventureworks-erp', 'creditcard') }}
)
select *
from source_data