CREATE TABLE if not exists dws_uncovered_OE_list12023q3 as
select
       source_id
,      product_line
,      OE
,      demands
from
       (
                 select
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty/b.oe_count as demands
                 from
                           (
                                  select
                                         source_id
                                  ,      product_line
                                  ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                  from
                                         (
                                                select
                                                       source_id
                                                ,      'PL028' as product_line
                                                ,      ofo
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                where
                                                       ds='${yyyyMMdd,-1d}'
                                                and    source_id in
                                                       (
                                                              select
                                                                     level_id
                                                              from
                                                                     dws_wks_coverage_analysis_uncovered_levelid_temp
                                                              where
                                                                     product_line='PL028' ))t lateral view explode(split(ofo,'\\|')) temp as temp_col) a
                 left join
                           (
                                    select
                                             source_id
                                    ,        product_line
                                    ,        count(distinct oe) as oe_count
                                    from
                                             (
                                                    select
                                                           source_id
                                                    ,      product_line
                                                    ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                                    from
                                                           (
                                                                  select
                                                                         source_id
                                                                  ,      'PL028' as product_line
                                                                  ,      ofo
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  where
                                                                         ds='${yyyyMMdd,-1d}'
                                                                  and    source_id in
                                                                         (
                                                                                select
                                                                                       level_id
                                                                                from
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_temp
                                                                                where
                                                                                       product_line='PL028' ))t lateral view explode(split(ofo,'\\|')) temp as temp_col) tt
                                    group by
                                             source_id
                                    ,        product_line) b
                 on
                           a.source_id   =b.source_id
                 and       a.product_line=b.product_line
                 left join
                           dws_wks_coverage_analysis_uncovered_levelid_temp c
                 on
                           a.source_id   =c.level_id
                 and       a.product_line=c.product_line
                 order by
                           demands desc) t

union all
---PL026--ico----ignition coil select * from ods_spiderb_sys_vehicle_other_rel_pl_df where ds='20221030'
select
       source_id
,      product_line
,      OE
,      demands
from
       (
                 select
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty/b.oe_count as demands
                 from
                           (
                                  select
                                         source_id
                                  ,      product_line
                                  ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                  from
                                         (
                                                select
                                                       source_id
                                                ,      'PL026' as product_line
                                                ,      ico
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                where
                                                       ds='${yyyyMMdd,-1d}'
                                                and    source_id in
                                                       (
                                                              select
                                                                     level_id
                                                              from
                                                                     dws_wks_coverage_analysis_uncovered_levelid_temp
                                                              where
                                                                     product_line='PL026' ))t lateral view explode(split(ico,'\\|')) temp as temp_col) a
                 left join
                           (
                                    select
                                             source_id
                                    ,        product_line
                                    ,        count(distinct oe) as oe_count
                                    from
                                             (
                                                    select
                                                           source_id
                                                    ,      product_line
                                                    ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                                    from
                                                           (
                                                                  select
                                                                         source_id
                                                                  ,      'PL026' as product_line
                                                                  ,      ico
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  where
                                                                         ds='${yyyyMMdd,-1d}'
                                                                  and    source_id in
                                                                         (
                                                                                select
                                                                                       level_id
                                                                                from
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_temp
                                                                                where
                                                                                       product_line='PL026' ))t lateral view explode(split(ico,'\\|')) temp as temp_col) tt
                                    group by
                                             source_id
                                    ,        product_line) b
                 on
                           a.source_id   =b.source_id
                 and       a.product_line=b.product_line
                 left join
                           dws_wks_coverage_analysis_uncovered_levelid_temp c
                 on
                           a.source_id   =c.level_id
                 and       a.product_line=c.product_line
                 order by
                           demands desc) t

union all
---PL014---afo--air filter
select
       source_id
,      product_line
,      OE
,      demands
from
       (
                 select
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty/b.oe_count as demands
                 from
                           (
                                  select
                                         source_id
                                  ,      product_line
                                  ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                  from
                                         (
                                                select
                                                       source_id
                                                ,      'PL014' as product_line
                                                ,      afo
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                where
                                                       ds='${yyyyMMdd,-1d}'
                                                and    source_id in
                                                       (
                                                              select
                                                                     level_id
                                                              from
                                                                     dws_wks_coverage_analysis_uncovered_levelid_temp
                                                              where
                                                                     product_line='PL014' ))t lateral view explode(split(afo,'\\|')) temp as temp_col) a
                 left join
                           (
                                    select
                                             source_id
                                    ,        product_line
                                    ,        count(distinct oe) as oe_count
                                    from
                                             (
                                                    select
                                                           source_id
                                                    ,      product_line
                                                    ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                                    from
                                                           (
                                                                  select
                                                                         source_id
                                                                  ,      'PL014' as product_line
                                                                  ,      afo
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  where
                                                                         ds='${yyyyMMdd,-1d}'
                                                                  and    source_id in
                                                                         (
                                                                                select
                                                                                       level_id
                                                                                from
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_temp
                                                                                where
                                                                                       product_line='PL014' ))t lateral view explode(split(afo,'\\|')) temp as temp_col) tt
                                    group by
                                             source_id
                                    ,        product_line) b
                 on
                           a.source_id   =b.source_id
                 and       a.product_line=b.product_line
                 left join
                           dws_wks_coverage_analysis_uncovered_levelid_temp c
                 on
                           a.source_id   =c.level_id
                 and       a.product_line=c.product_line
                 order by
                           demands desc)t

union all
---PL015--bon---Battery
select
       source_id
,      product_line
,      OE
,      demands
from
       (
                 select
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty/b.oe_count as demands
                 from
                           (
                                  select
                                         source_id
                                  ,      product_line
                                  ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                  from
                                         (
                                                select
                                                       source_id
                                                ,      'PL015' as product_line
                                                ,      bon
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                where
                                                       ds='${yyyyMMdd,-1d}'
                                                and    source_id in
                                                       (
                                                              select
                                                                     level_id
                                                              from
                                                                     dws_wks_coverage_analysis_uncovered_levelid_temp
                                                              where
                                                                     product_line='PL015' ))t lateral view explode(split(bon,'\\|')) temp as temp_col) a
                 left join
                           (
                                    select
                                             source_id
                                    ,        product_line
                                    ,        count(distinct oe) as oe_count
                                    from
                                             (
                                                    select
                                                           source_id
                                                    ,      product_line
                                                    ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                                    from
                                                           (
                                                                  select
                                                                         source_id
                                                                  ,      'PL015' as product_line
                                                                  ,      bon
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  where
                                                                         ds='${yyyyMMdd,-1d}'
                                                                  and    source_id in
                                                                         (
                                                                                select
                                                                                       level_id
                                                                                from
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_temp
                                                                                where
                                                                                       product_line='PL015' ))t lateral view explode(split(bon,'\\|')) temp as temp_col) tt
                                    group by
                                             source_id
                                    ,        product_line) b
                 on
                           a.source_id   =b.source_id
                 and       a.product_line=b.product_line
                 left join
                           dws_wks_coverage_analysis_uncovered_levelid_temp c
                 on
                           a.source_id   =c.level_id
                 and       a.product_line=c.product_line
                 order by
                           demands desc)t

union all
---PL016---bfdon--Brake disk fr
select
       source_id
,      product_line
,      OE
,      demands
from
       (
                 select
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty/b.oe_count as demands
                 from
                           (
                                  select
                                         source_id
                                  ,      product_line
                                  ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                  from
                                         (
                                                select
                                                       source_id
                                                ,      'PL016' as product_line
                                                ,      bfdon
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                where
                                                       ds='${yyyyMMdd,-1d}'
                                                and    source_id in
                                                       (
                                                              select
                                                                     level_id
                                                              from
                                                                     dws_wks_coverage_analysis_uncovered_levelid_temp
                                                              where
                                                                     product_line='PL016' ))t lateral view explode(split(bfdon,'\\|')) temp as temp_col) a
                 left join
                           (
                                    select
                                             source_id
                                    ,        product_line
                                    ,        count(distinct oe) as oe_count
                                    from
                                             (
                                                    select
                                                           source_id
                                                    ,      product_line
                                                    ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                                    from
                                                           (
                                                                  select
                                                                         source_id
                                                                  ,      'PL016' as product_line
                                                                  ,      bfdon
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  where
                                                                         ds='${yyyyMMdd,-1d}'
                                                                  and    source_id in
                                                                         (
                                                                                select
                                                                                       level_id
                                                                                from
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_temp
                                                                                where
                                                                                       product_line='PL016' ))t lateral view explode(split(bfdon,'\\|')) temp as temp_col) tt
                                    group by
                                             source_id
                                    ,        product_line) b
                 on
                           a.source_id   =b.source_id
                 and       a.product_line=b.product_line
                 left join
                           dws_wks_coverage_analysis_uncovered_levelid_temp c
                 on
                           a.source_id   =c.level_id
                 and       a.product_line=c.product_line
                 order by
                           demands desc)t

union all
---PL017--brdon---Brake disk rr
select
       source_id
,      product_line
,      OE
,      demands
from
       (
                 select
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty/b.oe_count as demands
                 from
                           (
                                  select
                                         source_id
                                  ,      product_line
                                  ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                  from
                                         (
                                                select
                                                       source_id
                                                ,      'PL017' as product_line
                                                ,      brdon
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                where
                                                       ds='${yyyyMMdd,-1d}'
                                                and    source_id in
                                                       (
                                                              select
                                                                     level_id
                                                              from
                                                                     dws_wks_coverage_analysis_uncovered_levelid_temp
                                                              where
                                                                     product_line='PL017' ))t lateral view explode(split(brdon,'\\|')) temp as temp_col) a
                 left join
                           (
                                    select
                                             source_id
                                    ,        product_line
                                    ,        count(distinct oe) as oe_count
                                    from
                                             (
                                                    select
                                                           source_id
                                                    ,      product_line
                                                    ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                                    from
                                                           (
                                                                  select
                                                                         source_id
                                                                  ,      'PL017' as product_line
                                                                  ,      brdon
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  where
                                                                         ds='${yyyyMMdd,-1d}'
                                                                  and    source_id in
                                                                         (
                                                                                select
                                                                                       level_id
                                                                                from
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_temp
                                                                                where
                                                                                       product_line='PL017' ))t lateral view explode(split(brdon,'\\|')) temp as temp_col) tt
                                    group by
                                             source_id
                                    ,        product_line) b
                 on
                           a.source_id   =b.source_id
                 and       a.product_line=b.product_line
                 left join
                           dws_wks_coverage_analysis_uncovered_levelid_temp c
                 on
                           a.source_id   =c.level_id
                 and       a.product_line=c.product_line
                 order by
                           demands desc)t
--drop table dws_uncovered_OE_list22023q3
---PL029--spon---Spark plug
CREATE TABLE if not exists dws_uncovered_OE_list22023q3 as
select
       source_id
,      product_line
,      OE
,      demands
from
       (
                 select
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty/b.oe_count as demands
                 from
                           (
                                  select
                                         source_id
                                  ,      product_line
                                  ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                  from
                                         (
                                                select
                                                       source_id
                                                ,      'PL029' as product_line
                                                ,      spon
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                where
                                                       ds='${yyyyMMdd,-1d}'
                                                and    source_id in
                                                       (
                                                              select
                                                                     level_id
                                                              from
                                                                     dws_wks_coverage_analysis_uncovered_levelid_temp
                                                              where
                                                                     product_line='PL029' ))t lateral view explode(split(spon,'\\|')) temp as temp_col) a
                 left join
                           (
                                    select
                                             source_id
                                    ,        product_line
                                    ,        count(distinct oe) as oe_count
                                    from
                                             (
                                                    select
                                                           source_id
                                                    ,      product_line
                                                    ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                                    from
                                                           (
                                                                  select
                                                                         source_id
                                                                  ,      'PL029' as product_line
                                                                  ,      spon
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  where
                                                                         ds='${yyyyMMdd,-1d}'
                                                                  and    source_id in
                                                                         (
                                                                                select
                                                                                       level_id
                                                                                from
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_temp
                                                                                where
                                                                                       product_line='PL029' ))t lateral view explode(split(spon,'\\|')) temp as temp_col) tt
                                    group by
                                             source_id
                                    ,        product_line) b
                 on
                           a.source_id   =b.source_id
                 and       a.product_line=b.product_line
                 left join
                           dws_wks_coverage_analysis_uncovered_levelid_temp c
                 on
                           a.source_id   =c.level_id
                 and       a.product_line=c.product_line
                 order by
                           demands desc) t

union all
---PL022--acfo---Cabin filter
select
       source_id
,      product_line
,      OE
,      demands
from
       (
                 select
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty/b.oe_count as demands
                 from
                           (
                                  select
                                         source_id
                                  ,      product_line
                                  ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                  from
                                         (
                                                select
                                                       source_id
                                                ,      'PL022' as product_line
                                                ,      acfo
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                where
                                                       ds='${yyyyMMdd,-1d}'
                                                and    source_id in
                                                       (
                                                              select
                                                                     level_id
                                                              from
                                                                     dws_wks_coverage_analysis_uncovered_levelid_temp
                                                              where
                                                                     product_line='PL022' ))t lateral view explode(split(acfo,'\\|')) temp as temp_col) a
                 left join
                           (
                                    select
                                             source_id
                                    ,        product_line
                                    ,        count(distinct oe) as oe_count
                                    from
                                             (
                                                    select
                                                           source_id
                                                    ,      product_line
                                                    ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                                    from
                                                           (
                                                                  select
                                                                         source_id
                                                                  ,      'PL022' as product_line
                                                                  ,      acfo
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  where
                                                                         ds='${yyyyMMdd,-1d}'
                                                                  and    source_id in
                                                                         (
                                                                                select
                                                                                       level_id
                                                                                from
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_temp
                                                                                where
                                                                                       product_line='PL022' ))t lateral view explode(split(acfo,'\\|')) temp as temp_col) tt
                                    group by
                                             source_id
                                    ,        product_line) b
                 on
                           a.source_id   =b.source_id
                 and       a.product_line=b.product_line
                 left join
                           dws_wks_coverage_analysis_uncovered_levelid_temp c
                 on
                           a.source_id   =c.level_id
                 and       a.product_line=c.product_line
                 order by
                           demands desc)t

union all
---PL024--ffo---fule filter
select
       source_id
,      product_line
,      OE
,      demands
from
       (
                 select
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty/b.oe_count as demands
                 from
                           (
                                  select
                                         source_id
                                  ,      product_line
                                  ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                  from
                                         (
                                                select
                                                       source_id
                                                ,      'PL024' as product_line
                                                ,      ffo
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                where
                                                       ds='${yyyyMMdd,-1d}'
                                                and    source_id in
                                                       (
                                                              select
                                                                     level_id
                                                              from
                                                                     dws_wks_coverage_analysis_uncovered_levelid_temp
                                                              where
                                                                     product_line='PL024' ))t lateral view explode(split(ffo,'\\|')) temp as temp_col) a
                 left join
                           (
                                    select
                                             source_id
                                    ,        product_line
                                    ,        count(distinct oe) as oe_count
                                    from
                                             (
                                                    select
                                                           source_id
                                                    ,      product_line
                                                    ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                                    from
                                                           (
                                                                  select
                                                                         source_id
                                                                  ,      'PL024' as product_line
                                                                  ,      ffo
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  where
                                                                         ds='${yyyyMMdd,-1d}'
                                                                  and    source_id in
                                                                         (
                                                                                select
                                                                                       level_id
                                                                                from
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_temp
                                                                                where
                                                                                       product_line='PL024' ))t lateral view explode(split(ffo,'\\|')) temp as temp_col) tt
                                    group by
                                             source_id
                                    ,        product_line) b
                 on
                           a.source_id   =b.source_id
                 and       a.product_line=b.product_line
                 left join
                           dws_wks_coverage_analysis_uncovered_levelid_temp c
                 on
                           a.source_id   =c.level_id
                 and       a.product_line=c.product_line
                 order by
                           demands desc)t

union all
---PL018-bfpon--Brake pad fr
select
       source_id
,      product_line
,      OE
,      demands
from
       (
                 select
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty/b.oe_count as demands
                 from
                           (
                                  select
                                         source_id
                                  ,      product_line
                                  ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                  from
                                         (
                                                select
                                                       source_id
                                                ,      'PL018' as product_line
                                                ,      bfpon
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                where
                                                       ds='${yyyyMMdd,-1d}'
                                                and    source_id in
                                                       (
                                                              select
                                                                     level_id
                                                              from
                                                                     dws_wks_coverage_analysis_uncovered_levelid_temp
                                                              where
                                                                     product_line='PL018' ))t lateral view explode(split(bfpon,'\\|')) temp as temp_col) a
                 left join
                           (
                                    select
                                             source_id
                                    ,        product_line
                                    ,        count(distinct oe) as oe_count
                                    from
                                             (
                                                    select
                                                           source_id
                                                    ,      product_line
                                                    ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                                    from
                                                           (
                                                                  select
                                                                         source_id
                                                                  ,      'PL018' as product_line
                                                                  ,      bfpon
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  where
                                                                         ds='${yyyyMMdd,-1d}'
                                                                  and    source_id in
                                                                         (
                                                                                select
                                                                                       level_id
                                                                                from
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_temp
                                                                                where
                                                                                       product_line='PL018' ))t lateral view explode(split(bfpon,'\\|')) temp as temp_col) tt
                                    group by
                                             source_id
                                    ,        product_line) b
                 on
                           a.source_id   =b.source_id
                 and       a.product_line=b.product_line
                 left join
                           dws_wks_coverage_analysis_uncovered_levelid_temp c
                 on
                           a.source_id   =c.level_id
                 and       a.product_line=c.product_line
                 order by
                           demands desc)t

union all
---PL019--brpon--Brake pad rr
select
       source_id
,      product_line
,      OE
,      demands
from
       (
                 select
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty/b.oe_count as demands
                 from
                           (
                                  select
                                         source_id
                                  ,      product_line
                                  ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                  from
                                         (
                                                select
                                                       source_id
                                                ,      'PL019' as product_line
                                                ,      brpon
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                where
                                                       ds='${yyyyMMdd,-1d}'
                                                and    source_id in
                                                       (
                                                              select
                                                                     level_id
                                                              from
                                                                     dws_wks_coverage_analysis_uncovered_levelid_temp
                                                              where
                                                                     product_line='PL019' ))t lateral view explode(split(brpon,'\\|')) temp as temp_col) a
                 left join
                           (
                                    select
                                             source_id
                                    ,        product_line
                                    ,        count(distinct oe) as oe_count
                                    from
                                             (
                                                    select
                                                           source_id
                                                    ,      product_line
                                                    ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                                    from
                                                           (
                                                                  select
                                                                         source_id
                                                                  ,      'PL019' as product_line
                                                                  ,      brpon
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  where
                                                                         ds='${yyyyMMdd,-1d}'
                                                                  and    source_id in
                                                                         (
                                                                                select
                                                                                       level_id
                                                                                from
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_temp
                                                                                where
                                                                                       product_line='PL019' ))t lateral view explode(split(brpon,'\\|')) temp as temp_col) tt
                                    group by
                                             source_id
                                    ,        product_line) b
                 on
                           a.source_id   =b.source_id
                 and       a.product_line=b.product_line
                 left join
                           dws_wks_coverage_analysis_uncovered_levelid_temp c
                 on
                           a.source_id   =c.level_id
                 and       a.product_line=c.product_line
                 order by
                           demands desc)t

union all
---PL027--foso--LS
select
       source_id
,      product_line
,      OE
,      demands
from
       (
                 select
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty/b.oe_count as demands
                 from
                           (
                                  select
                                         source_id
                                  ,      product_line
                                  ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                  from
                                         (
                                                select
                                                       source_id
                                                ,      'PL027' as product_line
                                                ,      foso
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                where
                                                       ds='${yyyyMMdd,-1d}'
                                                and    source_id in
                                                       (
                                                              select
                                                                     level_id
                                                              from
                                                                     dws_wks_coverage_analysis_uncovered_levelid_temp
                                                              where
                                                                     product_line='PL027' ))t lateral view explode(split(foso,'\\|')) temp as temp_col) a
                 left join
                           (
                                    select
                                             source_id
                                    ,        product_line
                                    ,        count(distinct oe) as oe_count
                                    from
                                             (
                                                    select
                                                           source_id
                                                    ,      product_line
                                                    ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                                    from
                                                           (
                                                                  select
                                                                         source_id
                                                                  ,      'PL027' as product_line
                                                                  ,      foso
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  where
                                                                         ds='${yyyyMMdd,-1d}'
                                                                  and    source_id in
                                                                         (
                                                                                select
                                                                                       level_id
                                                                                from
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_temp
                                                                                where
                                                                                       product_line='PL027' ))t lateral view explode(split(foso,'\\|')) temp as temp_col) tt
                                    group by
                                             source_id
                                    ,        product_line) b
                 on
                           a.source_id   =b.source_id
                 and       a.product_line=b.product_line
                 left join
                           dws_wks_coverage_analysis_uncovered_levelid_temp c
                 on
                           a.source_id   =c.level_id
                 and       a.product_line=c.product_line
                 order by
                           demands desc) t

union all
---PL027--roso--LS
select
       source_id
,      product_line
,      OE
,      demands
from
       (
                 select
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty/b.oe_count as demands
                 from
                           (
                                  select
                                         source_id
                                  ,      product_line
                                  ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                  from
                                         (
                                                select
                                                       source_id
                                                ,      'PL027' as product_line
                                                ,      roso
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                where
                                                       ds='${yyyyMMdd,-1d}'
                                                and    source_id in
                                                       (
                                                              select
                                                                     level_id
                                                              from
                                                                     dws_wks_coverage_analysis_uncovered_levelid_temp
                                                              where
                                                                     product_line='PL027' ))t lateral view explode(split(roso,'\\|')) temp as temp_col) a
                 left join
                           (
                                    select
                                             source_id
                                    ,        product_line
                                    ,        count(distinct oe) as oe_count
                                    from
                                             (
                                                    select
                                                           source_id
                                                    ,      product_line
                                                    ,      translate(split(split(split(temp_col,'\\(')[0],'\\)')[0],'\\.')[0],'\\ ','\\') as OE
                                                    from
                                                           (
                                                                  select
                                                                         source_id
                                                                  ,      'PL027' as product_line
                                                                  ,      roso
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  where
                                                                         ds='${yyyyMMdd,-1d}'
                                                                  and    source_id in
                                                                         (
                                                                                select
                                                                                       s level_id
                                                                                from
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_temp
                                                                                where
                                                                                       product_line='PL027' ))t lateral view explode(split(roso,'\\|')) temp as temp_col) tt
                                    group by
                                             source_id
                                    ,        product_line) b
                 on
                           a.source_id   =b.source_id
                 and       a.product_line=b.product_line
                 left join
                           dws_wks_coverage_analysis_uncovered_levelid_temp c
                 on
                           a.source_id   =c.level_id
                 and       a.product_line=c.product_line
                 order by
                           demands desc) t
-- where OE not in (select OE from dws_uncovered_OE_list1)
--select OE from dws_wks_kindle2_oelist2023q2
select
       *
from
       dws_wks_kindle2_oelist2023q3_v2
--drop table dws_wks_kindle2_oelist2023q3
CREATE TABLE if not exists dws_wks_kindle2_oelist2023q3_v2 stored as parquet LOCATION 'boschfs://boschfs/warehouse/Yanfei/dws_wks_kindle2_oelist2023q3_v2' as
select
       *
from
       (
              select
                     oe
              ,      pd_code
              ,      frc_demands
              ,      product_line
              ,      vehicle_info
              ,      row_number() over
                                       (
                                           partition by pd_code
                                           order by frc_demands desc
                                       )
                     rn1
              from
                     dws_wks_kindle2_oelist2023q3) tt
where
       rn1<=10