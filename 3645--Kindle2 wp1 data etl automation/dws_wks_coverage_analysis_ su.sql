CREATE TABLE if not exists dws_wks_coverage_analysis_temp_sum120230930 stored as parquet LOCATION 'boschfs://boschfs/warehouse/Yanfei/dws_wks_coverage_analysis_temp_sum120230930' as
select
    store_id
,   store_name
,   category_name_cle
,   product_line
,   Focus_cat
,   brand_new
,   manufactor
,   car_brand
,   recpt_mon
,   lid as level_id
,
    case
    when
        category_name_cle in ('Brake fluid'
                             , 'CG-PAB Other')
    then
        1
    else
        max(cov_flag)
    end as cov_flag
,------------刹车油全覆盖
    case
    when
        category_name_cle in ('Brake fluid'
                             , 'CG-PAB Other')
    then
        1
    else
        max(PCD_FLAG)
    end as pcd_flag
,
    --case when brand_name_cle=='博世' then sum(out_qty) else 0 end as BOSCH_QTY,
    --case when brand_name_cle<>'博世' then sum(out_qty) else 0 end as NON_BOSCH_QTY,
    SUM(cast(out_qty as double)) as total_qty
from
    (
        select
            store_id
        ,   store_name
        ,   category_name_cle
        ,   brand_name_cle
        ,
            case
            when
                category_name_cle in ('Engine Oil'
                                     , 'Battery'
                                     , 'Transmission oil'
                                     , 'Oil filter'
                                     , 'Spark plug'
                                     , 'Cabin filter'
                                     , 'Air filter'
                                     , 'Brake disk Rr'
                                     , 'Brake pad fr'
                                     , 'Brake pad rr'
                                     , 'Wiper'
                                     , 'Brake fluid'
                                     , 'Brake disk fr'
                                     , 'Fuel filter'
                                     , 'Steering Gear'
                                     , 'Ignition Coil'
                                     , 'Injector'
                                     , 'Lambda sensor'
                                     , 'Steering Pump'
                                     , 'High-pressure pump GDI'
                                     , 'CG-PAB Other')
            then
                1
            else
                0
            end as Focus_cat
        ,
            case
            when
                brand_name_cle='博世'
            then
                'BOSCH'
            else
                'NON_BOSCH'
            end as brand_new
        ,
            case
            when
                out_qty is null
            then
                0
            else
                out_qty
            end as out_qty
        ,   lid
        ,   product_line
        ,   manufactor
        ,   car_brand
        ,   recpt_mon
        ,   cov_flag
        ,   pcd_flag
        from
            kindle_coverage_ana_with_pcd_status_temp)t-----------------------align with PCD status---------------------
where
    Focus_cat=1
group by
    store_id
,   store_name
,   category_name_cle
,   Focus_cat
,   brand_new
,   manufactor
,   car_brand
,   recpt_mon
,   lid
,   product_line