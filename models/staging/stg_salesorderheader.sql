with source_data as (
    select
        salesorderid
        , shipmethodid
        , billtoaddressid
        , modifieddate
        , rowguid
        , taxamt
        , shiptoaddressid
        , onlineorderflag
        , territoryid
        , 'status'
        , orderdate
        , creditcardapprovalcode
        , subtotal
        , creditcardid
        , revisionnumber
        , freight
        , duedate
        , totaldue
        , customerid
        , shipdate
        , accountnumber
    from {{ source('adventureworks-erp', 'salesorderheader') }}
)
select *
from source_data