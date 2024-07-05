CREATE TABLE if not exists dws_wks_coverage_analysis_uncovered_levelid_temp stored as parquet LOCATION 'boschfs://boschfs/warehouse/Yanfei/dws_wks_coverage_analysis_uncovered_levelid_temp' as
select
    a.*
,   b.product_line
from
    (
        select
            level_id
        ,   category_name_cle
        ,   manufactor
        ,   car_brand
        ,   sum(total_qty) as qty
        from
            dws_wks_coverage_analysis_temp_sum120230930
        where
            cov_flag=0
        and level_id is not null
        group by
            level_id
        ,   category_name_cle
        ,   product_line
        ,   manufactor
        ,   car_brand
        ORDER BY
            QTY desc) a
left join
    (
        select
            product_line
        ,
            case
            when
                name='Ignition coil'
            then
                'Ignition Coil'
            else
                name
            end as name
        from
            ods_spiderb_sys_productline_df
        where
            ds='20231007'
        group by
            product_line
        ,   name) b
on
    a.category_name_cle=b.name
order by
    qty desc