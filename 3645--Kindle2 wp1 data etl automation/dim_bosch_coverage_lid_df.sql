CREATE TABLE if not exists DIM_BOSCH_COVERAGE_LID_TEMP as
select
          a.product_number
,         a.product_key
,         b.source_id as source_id
from
          (
                 select
                        product_number
                 ,      product_key
                 ,      source_id
                 ,      bosch_id
                 from
                        ods_spiderb_sys_part_application_df
                 where
                        ds        ='${yyyyMMdd,-1d}'
                 and    is_deleted=0) a
left join
          (
                   select
                            bosch_id
                   ,        source_id
                   from
                            ods_spiderb_sys_main_vehicle_basic_df
                   where
                            ds='${yyyyMMdd,-1d}'
                   group by
                            bosch_id
                   ,        source_id) b
on
          a.bosch_id=b.bosch_id