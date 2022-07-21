with stg_salesorderheader as (
    select *
    from {{ref('stg_salesorderheader')}}
)

, stg_salesorderheadersalesreason as (
    select *
    from {{ref('stg_salesorderheadersalesreason')}}
)

, stg_salesreason as (
    select *
    from {{ref('stg_salesreason')}}
)

, transformed as (
    select 
        -- Here, no surrogate key is created, as one salesorderid can have more than one salesreason.
        stg_salesorderheadersalesreason.salesorderid
        , IFNULL(stg_salesreason.reason_name,'Not indicated') as reason_name
        , stg_salesorderheadersalesreason.salesreasonid 
        , stg_salesreason.salesreasonid as salesreasonid_from_salesreason
    from stg_salesorderheadersalesreason
    left join stg_salesreason on stg_salesorderheadersalesreason.salesreasonid = stg_salesreason.salesreasonid 
)

select *
from transformed