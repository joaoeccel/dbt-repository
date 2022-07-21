with customers as (
    select
        customer_sk
        , customerid
    from {{ref('dim_customers')}} 
)

, creditcards as (
    select
        creditcard_sk
        , creditcardid
    from {{ref('dim_creditcards')}}
)

, locations as (
    select
        shiptoaddress_sk
        , shiptoaddressid
    from {{ref('dim_locations')}}
)

, reasons as (
    select
        salesorderid
        , salesreasonid
    from {{ref('dim_reasons')}}
)

, products as (
    select
        product_sk
        , productid
    from {{ref('dim_products')}}
)


, salesorderdetail as (
    select
        stg_salesorderdetail.salesorderid
        , stg_salesorderdetail.orderqty
        , stg_salesorderdetail.productid
        , products.product_sk as product_fk
        , stg_salesorderdetail.specialofferid
        , stg_salesorderdetail.unitprice
        , stg_salesorderdetail.unitpricediscount 
        , stg_salesorderdetail.unitprice * stg_salesorderdetail.orderqty  AS  revenue_wo_taxandfreight
    from {{ref('stg_salesorderdetail')}} stg_salesorderdetail
    left join products on stg_salesorderdetail.productid = products.productid
)

, salesorderheader as (
    select
        salesorderid
        , customers.customer_sk as customer_fk
        , creditcards.creditcard_sk as creditcard_fk
        , locations.shiptoaddress_sk as shiptoadress_fk
        , shipmethodid
        , billtoaddressid
        , modifieddate
        , rowguid
        , taxamt
        -- , shiptoaddressid
        , onlineorderflag
        , territoryid
        , order_status
        , CASE 
            WHEN order_status = 1 THEN 'In_process'
            WHEN order_status = 2 THEN 'Approved'
            WHEN order_status = 3 THEN 'Backordered' 
            WHEN order_status = 4 THEN 'Rejected' 
            WHEN order_status = 5 THEN 'Shipped'
            WHEN order_status = 6 THEN 'Cancelled' 
            ELSE 'no_status'
        end as order_status_name
        , orderdate
        , creditcardapprovalcode
        , subtotal
        -- , creditcardid
        , currencyrateid
        , revisionnumber
        , freight
        , duedate
        , totaldue
        -- , customerid
        , shipdate
        , accountnumber
    from {{ref('stg_salesorderheader')}} 
    left join customers on stg_salesorderheader.customerid = customers.customerid
    left join creditcards on stg_salesorderheader.creditcardid = creditcards.creditcardid
    left join locations on stg_salesorderheader.shiptoaddressid = locations.shiptoaddressid
    where creditcardapprovalcode is not null
)

/* We then join salesorderdetail and salesorderheader to get the final fact table*/
, final as (
    select
        salesorderdetail.salesorderid
        , salesorderdetail.product_fk
        , salesorderheader.customer_fk
        , salesorderheader.shiptoadress_fk
        , salesorderheader.creditcard_fk
        , salesorderdetail.unitprice
        , salesorderdetail.orderqty
        , salesorderdetail.revenue_wo_taxandfreight
        , salesorderheader.taxamt as tax_per_salesorderid
        , salesorderheader.freight as freight_per_salesorderid
        , salesorderheader.totaldue
        , salesorderheader.orderdate
        , salesorderheader.order_status_name 
    from salesorderdetail
    left join salesorderheader on salesorderdetail.salesorderid = salesorderheader.salesorderid
    where creditcard_fk is not null
)

select *
from final
