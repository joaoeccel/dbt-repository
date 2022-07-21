with stg_salesorderheader as (
    select 
    distinct(customerid)
    , shiptoaddressid
    from {{ref('stg_salesorderheader')}}
)

, stg_businessentityaddress as (
    select *
    from {{ref('stg_businessentityaddress')}}
)

, stg_person as (
    select *
    from {{ref('stg_person')}}
)

, transformed as (
SELECT
    row_number() over (order by stg_salesorderheader.customerid) as customer_sk -- auto-incremental surrogate key
    , stg_salesorderheader.customerid
    , stg_salesorderheader.shiptoaddressid
    , stg_businessentityaddress.addressid as addressid_person
    , stg_businessentityaddress.businessentityid as businessentitytidbeaddress
    , stg_person.businessentityid as businessentitytid_person
    , stg_person.firstname
    , stg_person.middlename
    , stg_person.lastname
    , CONCAT(
        IFNULL(stg_person.firstname,' ')
        ,' '
        ,IFNULL(stg_person.middlename,' ')
        ,' '
        ,IFNULL(stg_person.lastname,' ')) as customer_fullname
    FROM stg_salesorderheader
    left join stg_businessentityaddress on stg_salesorderheader.shiptoaddressid = stg_businessentityaddress.addressid
    left join stg_person on stg_businessentityaddress.businessentityid = stg_person.businessentityid
)

select *
from transformed