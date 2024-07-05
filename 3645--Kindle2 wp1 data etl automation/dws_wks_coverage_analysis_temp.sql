CREATE TABLE if not exists dws_wks_coverage_analysis_temp20230930 stored as parquet LOCATION 'boschfs://boschfs/warehouse/Yanfei/dws_wks_coverage_analysis_temp20230930' as
SELECT
          store_id
,         store_name
,         wo_num
,         start_time
,         wo_type
,         rpr_type_id
,         wo_status_id
,         svcre_advisor
,         is_rework
,         is_adnl
,         svcre_type_name
,         lbr_project_name
,         lbr_total
,         parts_total
,         stk_num
,         parts_id
,         parts_name
,         parts_name2
,         a.brand as brand
,         stm_brand
,         parts_category_name
,         ticket_num
,         out_qty
,         cost
,         tax_cost
,         kld
,         dsct
,         act_total
,         grs_profit_amt
,         recpt_time
,         substring(recpt_time,1,7) as recpt_mon
,         before_dsct_total
,         ar_total
,         setl_total
,         receipts_total
,         credit_nopay
,         reduce_money
,         coupon_money
,         point_score_money
,         pay_type
,         coupon_name
,         empe_name
,         worker_group_name
,         sm_adnl
,         sm_not_accepted
,         is_not_accepted
,         book_time
,         plan_fnsh_time
,         cst_name
,         cst_mobile
,         car_num
,         a.vin as vin
,         mileage
,         doc_crt_time
,         custom_brand
,         custom_line
,         custom_modl
,         repair_man
,         repair_man_mobile
,         insure_comp
,         cst_source_name
,         insurance_expires
,         bsn_insurance_expires
,         force_expires_date
,         svc_rcrd_id
,         bosch_brand_id
,         brand_name
,         gnrc_artcl_id
,         desc_en
,
          case
          when
                  category_name_cle='CG-PAB Other'
          then
                  'Brake fluid'
          else
                  category_name_cle
          end as category_name_cle
,         brand_name_cle
,         b.product_line
,
          case
          when
                  b.name is not null
          then
                  1
          else
                  0
          end          as cov_flag
,         a.level_id   as level_id
,         c.remark_id  as LID
,         c.manufactor as manufactor
,         c.brand      as car_brand
from
          (
                 select
                        *
                 from
                        dws_wks_service_record_smy_df
                 where
                        ds         ='${yyyyMMdd,-1d}'
                 and    recpt_time >='2021-01-01'
                 and    recpt_time <='2023-09-30') a
left join
          (
                   select
                            vin
                   ,        product_line
                   ,        name
                   from
                            (
                                      select
                                                n.*
                                      ,         m.name
                                      from
                                                (
                                                          select
                                                                    a.*
                                                          ,         b. product_line
                                                          from
                                                                    (
                                                                              select
                                                                                        a.remark_id
                                                                              ,         a.vin
                                                                              ,         b.product_key
                                                                              from
                                                                                        (
                                                                                                 select
                                                                                                          max(level_id) as remark_id
                                                                                                 ,        vin
                                                                                                 from
                                                                                                          dim_wks_vin_level_id
                                                                                                 where
                                                                                                          ds='${yyyyMMdd,-1d}'
                                                                                                 group by
                                                                                                          vin ) a
                                                                              left join
                                                                                        (
                                                                                                  select
                                                                                                            a.product_key
                                                                                                  ,         b.source_id as source_id
                                                                                                  from
                                                                                                            (
                                                                                                                   select
                                                                                                                          product_key
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
                                                                                                            a.bosch_id=b.bosch_id) b
                                                                              on
                                                                                        a.remark_id=b.source_id) a
                                                          left join
                                                                    (
                                                                           SELECT
                                                                                  uuid
                                                                           ,      product_key
                                                                           ,      product_line
                                                                           FROM
                                                                                  ods_spiderb_origin_productkey_rel_productline_df
                                                                           where
                                                                                  ds='${yyyyMMdd,-1d}' ) b
                                                          on
                                                                    a.product_key=b.product_key) n
                                      left join
                                                (
                                                       SELECT
                                                              uuid
                                                       ,      product_line
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
                                                              ds='${yyyyMMdd,-1d}' ) m
                                      on
                                                n.product_line=m.product_line) t
                   group by
                            vin
                   ,        product_line
                   ,        name) b
on
          a.VIN              =b.vin
and       a.category_name_cle=b.name
left join
          (
                    select
                              a.remark_id
                    ,         a.vin
                    ,         b.manufactor
                    ,         b.brand
                    from
                              (
                                       select
                                                max(level_id) as remark_id
                                       ,        vin
                                       from
                                                dim_wks_vin_level_id
                                       where
                                                ds='${yyyyMMdd,-1d}'
                                       group by
                                                vin ) a
                    left join
                              (
                                       select
                                                source_id
                                       ,        max(manufactor) as manufactor
                                       ,        max(brand)      as brand
                                       from
                                                ods_spiderb_sys_main_vehicle_basic_df
                                       where
                                                ds='${yyyyMMdd,-1d}'
                                       group by
                                                source_id) b
                    on
                              a.remark_id=b.source_id) c
on
          a.vin=c.vin