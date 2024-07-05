create table if not exists kindle_covered_product_line_pcd_na as
select
    level_id
,   category_name_cle
,   product_line
,   manufactor
,   car_brand
,   sum(total_qty) as qty
from
    dws_wks_coverage_analysis_temp_sum120230630
where
    cov_flag=1
and pcd_flag=0
group by
    level_id
,   category_name_cle
,   product_line
,   manufactor
,   car_brand
ORDER BY
    QTY desc