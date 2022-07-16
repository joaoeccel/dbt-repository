with source_data as (
    select
        creditcardid
        , cardtype
        , expyear
        , modifieddate
        , expmonth
        , cardnumber
    from {{ source('adventureworks-erp', 'creditcard') }}
)
select *
from source_data