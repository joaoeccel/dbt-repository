with source_data as (
    select
        productid
        , safetystocklevel
        , finishedgoodsflag
        , class
        , makeflag
        , productnumber
        , reorderpoint
        , modifieddate
        , rowguid
        , productmodelid
        , weightunitmeasurecode
        , standardcost
        , 'name'
        , productsubcategoryid
        , listprice
        , daystomanufacture
        , productline
        , color
        , sellstartdate
        , 'weight'
    from {{ source('adventureworks-erp', 'product') }}
)
select *
from source_data