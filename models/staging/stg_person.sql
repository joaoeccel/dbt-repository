with source_data as (
    select
        businessentityid
        , title
        , firstname
        , middlename
        , lastname
        , persontype
        , namestyle
        , suffix
        , modifieddate
        , rowguid
        , emailpromotion
    from {{ source('adventureworks-erp', 'person') }}
)
select *
from source_data	