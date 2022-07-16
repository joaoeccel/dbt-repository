with staging as (
    select *
    from {{ref('stg_salesorderheader')}}
)
    
, transformed as (
    select
        salesorderid
        , customerid
    from staging
)

select *
from transformed