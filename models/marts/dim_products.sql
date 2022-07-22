with stg_salesorderheader as (
    select *
    from {{ref('stg_salesorderheader')}}
)

, stg_salesorderdetail as (
    select 
        distinct(productid)
    from {{ref('stg_salesorderdetail')}}
)

, stg_product as (
    select *
    from {{ref('stg_product')}}
)

, transformed as (
    select 
        row_number() over (order by stg_salesorderdetail.productid) as product_sk -- auto-incremental surrogate key
        , stg_salesorderdetail.productid
        , stg_product.product_name 
    from stg_salesorderdetail
    left join stg_product on stg_salesorderdetail.productid = stg_product.productid
)

select *
from transformed