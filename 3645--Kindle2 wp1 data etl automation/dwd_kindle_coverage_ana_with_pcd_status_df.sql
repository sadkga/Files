-- drop table kindle_pcd_status_temp
create table if not exists kindle_pcd_status_temp as
select
          a.product_number
,         a.pcd_status_name
,         a.product_line
,         a.pcd_status
,         b.source_id
,
          case
          when
                  a.pcd_status='01'
          then
                  1
          else
                  0
          end as PCD_FLAG
from
          ods_spiderb_calc_product_res_df a
left join
          DIM_BOSCH_COVERAGE_LID_TEMP b
on
          a.product_number=b.product_number
where
          a.ds='${yyyyMMdd,-1d}'
--select * from kindle_pcd_status_temp where product_line in ('PL018','PL019') and source_id='ACC0836A0003'
-- drop table kindle_pcd_status_temp_agg
create table if not exists kindle_pcd_status_temp_agg as
select
         source_id
,        product_line
,        max(PCD_FLAG) as PCD_FLAG
from
         kindle_pcd_status_temp
group by
         source_id
,        product_line
-- drop table < dwd_kindle_coverage_ana_with_pcd_status status_temp
create table if not exists kindle_coverage_ana_with_pcd_status_temp as
select
          a.*
,         b.pcd_flag
from
          dws_wks_coverage_analysis_temp20230930 a
left join
          kindle_pcd_status_temp_agg b
on
          a.lid         =b.source_id
and       a.product_line=b.product_line