SELECT
       source_id
,      product_line
,      OE
,      demands
FROM
       (
                 SELECT
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty / b.oe_count AS demands
                 FROM
                           (
                                  SELECT
                                         source_id
                                  ,      product_line
                                  ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                  FROM
                                         (
                                                SELECT
                                                       source_id
                                                ,      'PL028' AS product_line
                                                ,      ofo
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                WHERE
                                                       ds = '${yyyyMMdd,-1d}'
                                                AND    source_id IN
                                                       (
                                                              SELECT
                                                                     level_id
                                                              FROM
                                                                     dws_wks_coverage_analysis_uncovered_levelid_df
                                                              WHERE
                                                                     pday         = ${bdp.system.bizdate}
                                                              and    product_line = 'PL028' ) ) t LATERAL VIEW explode(split(ofo, '\\|')) temp AS temp_col ) a
                 LEFT JOIN
                           (
                                    SELECT
                                             source_id
                                    ,        product_line
                                    ,        count(DISTINCT oe) AS oe_count
                                    FROM
                                             (
                                                    SELECT
                                                           source_id
                                                    ,      product_line
                                                    ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                                    FROM
                                                           (
                                                                  SELECT
                                                                         source_id
                                                                  ,      'PL028' AS product_line
                                                                  ,      ofo
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  WHERE
                                                                         ds = '${yyyyMMdd,-1d}'
                                                                  AND    source_id IN
                                                                         (
                                                                                SELECT
                                                                                       level_id
                                                                                FROM
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_df
                                                                                WHERE
                                                                                       pday         = ${bdp.system.bizdate}
                                                                                and    product_line = 'PL028' ) ) t LATERAL VIEW explode(split(ofo, '\\|')) temp AS temp_col ) tt
                                    GROUP BY
                                             source_id
                                    ,        product_line ) b
                 ON
                           a.source_id    = b.source_id
                 AND       a.product_line = b.product_line
                 LEFT JOIN
                           (
                                  select
                                         *
                                  from
                                         dws_wks_coverage_analysis_uncovered_levelid_df
                                  where
                                         pday = ${bdp.system.bizdate}) c
                 ON
                           a.source_id    = c.level_id
                 AND       a.product_line = c.product_line
                 ORDER BY
                           demands DESC ) t

UNION ALL
---PL026--ico----ignition coil select * from ods_spiderb_sys_vehicle_other_rel_pl_df where ds='20221030'
SELECT
       source_id
,      product_line
,      OE
,      demands
FROM
       (
                 SELECT
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty / b.oe_count AS demands
                 FROM
                           (
                                  SELECT
                                         source_id
                                  ,      product_line
                                  ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                  FROM
                                         (
                                                SELECT
                                                       source_id
                                                ,      'PL026' AS product_line
                                                ,      ico
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                WHERE
                                                       ds = '${yyyyMMdd,-1d}'
                                                AND    source_id IN
                                                       (
                                                              SELECT
                                                                     level_id
                                                              FROM
                                                                     dws_wks_coverage_analysis_uncovered_levelid_df
                                                              WHERE
                                                                     pday         = ${bdp.system.bizdate}
                                                              and    product_line = 'PL026' ) ) t LATERAL VIEW explode(split(ico, '\\|')) temp AS temp_col ) a
                 LEFT JOIN
                           (
                                    SELECT
                                             source_id
                                    ,        product_line
                                    ,        count(DISTINCT oe) AS oe_count
                                    FROM
                                             (
                                                    SELECT
                                                           source_id
                                                    ,      product_line
                                                    ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                                    FROM
                                                           (
                                                                  SELECT
                                                                         source_id
                                                                  ,      'PL026' AS product_line
                                                                  ,      ico
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  WHERE
                                                                         ds = '${yyyyMMdd,-1d}'
                                                                  AND    source_id IN
                                                                         (
                                                                                SELECT
                                                                                       level_id
                                                                                FROM
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_df
                                                                                WHERE
                                                                                       pday         = ${bdp.system.bizdate}
                                                                                and    product_line = 'PL026' ) ) t LATERAL VIEW explode(split(ico, '\\|')) temp AS temp_col ) tt
                                    GROUP BY
                                             source_id
                                    ,        product_line ) b
                 ON
                           a.source_id    = b.source_id
                 AND       a.product_line = b.product_line
                 LEFT JOIN
                           (
                                  select
                                         *
                                  from
                                         dws_wks_coverage_analysis_uncovered_levelid_df
                                  where
                                         pday = ${bdp.system.bizdate}) c
                 ON
                           a.source_id    = c.level_id
                 AND       a.product_line = c.product_line
                 ORDER BY
                           demands DESC ) t

UNION ALL
---PL014---afo--air filter
SELECT
       source_id
,      product_line
,      OE
,      demands
FROM
       (
                 SELECT
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty / b.oe_count AS demands
                 FROM
                           (
                                  SELECT
                                         source_id
                                  ,      product_line
                                  ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                  FROM
                                         (
                                                SELECT
                                                       source_id
                                                ,      'PL014' AS product_line
                                                ,      afo
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                WHERE
                                                       ds = '${yyyyMMdd,-1d}'
                                                AND    source_id IN
                                                       (
                                                              SELECT
                                                                     level_id
                                                              FROM
                                                                     dws_wks_coverage_analysis_uncovered_levelid_df
                                                              WHERE
                                                                     pday         = ${bdp.system.bizdate}
                                                              and    product_line = 'PL014' ) ) t LATERAL VIEW explode(split(afo, '\\|')) temp AS temp_col ) a
                 LEFT JOIN
                           (
                                    SELECT
                                             source_id
                                    ,        product_line
                                    ,        count(DISTINCT oe) AS oe_count
                                    FROM
                                             (
                                                    SELECT
                                                           source_id
                                                    ,      product_line
                                                    ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                                    FROM
                                                           (
                                                                  SELECT
                                                                         source_id
                                                                  ,      'PL014' AS product_line
                                                                  ,      afo
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  WHERE
                                                                         ds = '${yyyyMMdd,-1d}'
                                                                  AND    source_id IN
                                                                         (
                                                                                SELECT
                                                                                       level_id
                                                                                FROM
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_df
                                                                                WHERE
                                                                                       pday         = ${bdp.system.bizdate}
                                                                                and    product_line = 'PL014' ) ) t LATERAL VIEW explode(split(afo, '\\|')) temp AS temp_col ) tt
                                    GROUP BY
                                             source_id
                                    ,        product_line ) b
                 ON
                           a.source_id    = b.source_id
                 AND       a.product_line = b.product_line
                 LEFT JOIN
                           (
                                  select
                                         *
                                  from
                                         dws_wks_coverage_analysis_uncovered_levelid_df
                                  where
                                         pday = ${bdp.system.bizdate}) c
                 ON
                           a.source_id    = c.level_id
                 AND       a.product_line = c.product_line
                 ORDER BY
                           demands DESC ) t

UNION ALL
---PL015--bon---Battery
SELECT
       source_id
,      product_line
,      OE
,      demands
FROM
       (
                 SELECT
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty / b.oe_count AS demands
                 FROM
                           (
                                  SELECT
                                         source_id
                                  ,      product_line
                                  ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                  FROM
                                         (
                                                SELECT
                                                       source_id
                                                ,      'PL015' AS product_line
                                                ,      bon
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                WHERE
                                                       ds = '${yyyyMMdd,-1d}'
                                                AND    source_id IN
                                                       (
                                                              SELECT
                                                                     level_id
                                                              FROM
                                                                     dws_wks_coverage_analysis_uncovered_levelid_df
                                                              WHERE
                                                                     pday         = ${bdp.system.bizdate}
                                                              and    product_line = 'PL015' ) ) t LATERAL VIEW explode(split(bon, '\\|')) temp AS temp_col ) a
                 LEFT JOIN
                           (
                                    SELECT
                                             source_id
                                    ,        product_line
                                    ,        count(DISTINCT oe) AS oe_count
                                    FROM
                                             (
                                                    SELECT
                                                           source_id
                                                    ,      product_line
                                                    ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                                    FROM
                                                           (
                                                                  SELECT
                                                                         source_id
                                                                  ,      'PL015' AS product_line
                                                                  ,      bon
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  WHERE
                                                                         ds = '${yyyyMMdd,-1d}'
                                                                  AND    source_id IN
                                                                         (
                                                                                SELECT
                                                                                       level_id
                                                                                FROM
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_df
                                                                                WHERE
                                                                                       pday         = ${bdp.system.bizdate}
                                                                                and    product_line = 'PL015' ) ) t LATERAL VIEW explode(split(bon, '\\|')) temp AS temp_col ) tt
                                    GROUP BY
                                             source_id
                                    ,        product_line ) b
                 ON
                           a.source_id    = b.source_id
                 AND       a.product_line = b.product_line
                 LEFT JOIN
                           (
                                  select
                                         *
                                  from
                                         dws_wks_coverage_analysis_uncovered_levelid_df
                                  where
                                         pday = ${bdp.system.bizdate}) c
                 ON
                           a.source_id    = c.level_id
                 AND       a.product_line = c.product_line
                 ORDER BY
                           demands DESC ) t

UNION ALL
---PL016---bfdon--Brake disk fr
SELECT
       source_id
,      product_line
,      OE
,      demands
FROM
       (
                 SELECT
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty / b.oe_count AS demands
                 FROM
                           (
                                  SELECT
                                         source_id
                                  ,      product_line
                                  ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                  FROM
                                         (
                                                SELECT
                                                       source_id
                                                ,      'PL016' AS product_line
                                                ,      bfdon
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                WHERE
                                                       ds = '${yyyyMMdd,-1d}'
                                                AND    source_id IN
                                                       (
                                                              SELECT
                                                                     level_id
                                                              FROM
                                                                     dws_wks_coverage_analysis_uncovered_levelid_df
                                                              WHERE
                                                                     pday         = ${bdp.system.bizdate}
                                                              and    product_line = 'PL016' ) ) t LATERAL VIEW explode(split(bfdon, '\\|')) temp AS temp_col ) a
                 LEFT JOIN
                           (
                                    SELECT
                                             source_id
                                    ,        product_line
                                    ,        count(DISTINCT oe) AS oe_count
                                    FROM
                                             (
                                                    SELECT
                                                           source_id
                                                    ,      product_line
                                                    ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                                    FROM
                                                           (
                                                                  SELECT
                                                                         source_id
                                                                  ,      'PL016' AS product_line
                                                                  ,      bfdon
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  WHERE
                                                                         ds = '${yyyyMMdd,-1d}'
                                                                  AND    source_id IN
                                                                         (
                                                                                SELECT
                                                                                       level_id
                                                                                FROM
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_df
                                                                                WHERE
                                                                                       pday         = ${bdp.system.bizdate}
                                                                                and    product_line = 'PL016' ) ) t LATERAL VIEW explode(split(bfdon, '\\|')) temp AS temp_col ) tt
                                    GROUP BY
                                             source_id
                                    ,        product_line ) b
                 ON
                           a.source_id    = b.source_id
                 AND       a.product_line = b.product_line
                 LEFT JOIN
                           (
                                  select
                                         *
                                  from
                                         dws_wks_coverage_analysis_uncovered_levelid_df
                                  where
                                         pday = ${bdp.system.bizdate}) c
                 ON
                           a.source_id    = c.level_id
                 AND       a.product_line = c.product_line
                 ORDER BY
                           demands DESC ) t

UNION ALL
---PL017--brdon---Brake disk rr
SELECT
       source_id
,      product_line
,      OE
,      demands
FROM
       (
                 SELECT
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty / b.oe_count AS demands
                 FROM
                           (
                                  SELECT
                                         source_id
                                  ,      product_line
                                  ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                  FROM
                                         (
                                                SELECT
                                                       source_id
                                                ,      'PL017' AS product_line
                                                ,      brdon
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                WHERE
                                                       ds = '${yyyyMMdd,-1d}'
                                                AND    source_id IN
                                                       (
                                                              SELECT
                                                                     level_id
                                                              FROM
                                                                     dws_wks_coverage_analysis_uncovered_levelid_df
                                                              WHERE
                                                                     pday         = ${bdp.system.bizdate}
                                                              and    product_line = 'PL017' ) ) t LATERAL VIEW explode(split(brdon, '\\|')) temp AS temp_col ) a
                 LEFT JOIN
                           (
                                    SELECT
                                             source_id
                                    ,        product_line
                                    ,        count(DISTINCT oe) AS oe_count
                                    FROM
                                             (
                                                    SELECT
                                                           source_id
                                                    ,      product_line
                                                    ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                                    FROM
                                                           (
                                                                  SELECT
                                                                         source_id
                                                                  ,      'PL017' AS product_line
                                                                  ,      brdon
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  WHERE
                                                                         ds = '${yyyyMMdd,-1d}'
                                                                  AND    source_id IN
                                                                         (
                                                                                SELECT
                                                                                       level_id
                                                                                FROM
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_df
                                                                                WHERE
                                                                                       pday         = ${bdp.system.bizdate}
                                                                                and    product_line = 'PL017' ) ) t LATERAL VIEW explode(split(brdon, '\\|')) temp AS temp_col ) tt
                                    GROUP BY
                                             source_id
                                    ,        product_line ) b
                 ON
                           a.source_id    = b.source_id
                 AND       a.product_line = b.product_line
                 LEFT JOIN
                           (
                                  select
                                         *
                                  from
                                         dws_wks_coverage_analysis_uncovered_levelid_df
                                  where
                                         pday = ${bdp.system.bizdate}) c
                 ON
                           a.source_id    = c.level_id
                 AND       a.product_line = c.product_line
                 ORDER BY
                           demands DESC ) t --drop table dws_uncovered_OE_list22023q3
---PL029--spon---Spark plug
-- ------------------------分割线------------------------------

union ALL

SELECT
       source_id
,      product_line
,      OE
,      demands
FROM
       (
                 SELECT
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty / b.oe_count AS demands
                 FROM
                           (
                                  SELECT
                                         source_id
                                  ,      product_line
                                  ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                  FROM
                                         (
                                                SELECT
                                                       source_id
                                                ,      'PL029' AS product_line
                                                ,      spon
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                WHERE
                                                       ds = '${yyyyMMdd,-1d}'
                                                AND    source_id IN
                                                       (
                                                              SELECT
                                                                     level_id
                                                              FROM
                                                                     dws_wks_coverage_analysis_uncovered_levelid_df
                                                              WHERE
                                                                     pday         = ${bdp.system.bizdate}
                                                              and    product_line = 'PL029' ) ) t LATERAL VIEW explode(split(spon, '\\|')) temp AS temp_col ) a
                 LEFT JOIN
                           (
                                    SELECT
                                             source_id
                                    ,        product_line
                                    ,        count(DISTINCT oe) AS oe_count
                                    FROM
                                             (
                                                    SELECT
                                                           source_id
                                                    ,      product_line
                                                    ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                                    FROM
                                                           (
                                                                  SELECT
                                                                         source_id
                                                                  ,      'PL029' AS product_line
                                                                  ,      spon
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  WHERE
                                                                         ds = '${yyyyMMdd,-1d}'
                                                                  AND    source_id IN
                                                                         (
                                                                                SELECT
                                                                                       level_id
                                                                                FROM
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_df
                                                                                WHERE
                                                                                       pday         = ${bdp.system.bizdate}
                                                                                and    product_line = 'PL029' ) ) t LATERAL VIEW explode(split(spon, '\\|')) temp AS temp_col ) tt
                                    GROUP BY
                                             source_id
                                    ,        product_line ) b
                 ON
                           a.source_id    = b.source_id
                 AND       a.product_line = b.product_line
                 LEFT JOIN
                           (
                                  select
                                         *
                                  from
                                         dws_wks_coverage_analysis_uncovered_levelid_df
                                  where
                                         pday = ${bdp.system.bizdate}) c
                 ON
                           a.source_id    = c.level_id
                 AND       a.product_line = c.product_line
                 ORDER BY
                           demands DESC ) t

UNION ALL
---PL022--acfo---Cabin filter
SELECT
       source_id
,      product_line
,      OE
,      demands
FROM
       (
                 SELECT
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty / b.oe_count AS demands
                 FROM
                           (
                                  SELECT
                                         source_id
                                  ,      product_line
                                  ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                  FROM
                                         (
                                                SELECT
                                                       source_id
                                                ,      'PL022' AS product_line
                                                ,      acfo
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                WHERE
                                                       ds = '${yyyyMMdd,-1d}'
                                                AND    source_id IN
                                                       (
                                                              SELECT
                                                                     level_id
                                                              FROM
                                                                     dws_wks_coverage_analysis_uncovered_levelid_df
                                                              WHERE
                                                                     pday         = ${bdp.system.bizdate}
                                                              and    product_line = 'PL022' ) ) t LATERAL VIEW explode(split(acfo, '\\|')) temp AS temp_col ) a
                 LEFT JOIN
                           (
                                    SELECT
                                             source_id
                                    ,        product_line
                                    ,        count(DISTINCT oe) AS oe_count
                                    FROM
                                             (
                                                    SELECT
                                                           source_id
                                                    ,      product_line
                                                    ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                                    FROM
                                                           (
                                                                  SELECT
                                                                         source_id
                                                                  ,      'PL022' AS product_line
                                                                  ,      acfo
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  WHERE
                                                                         ds = '${yyyyMMdd,-1d}'
                                                                  AND    source_id IN
                                                                         (
                                                                                SELECT
                                                                                       level_id
                                                                                FROM
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_df
                                                                                WHERE
                                                                                       pday         = ${bdp.system.bizdate}
                                                                                and    product_line = 'PL022' ) ) t LATERAL VIEW explode(split(acfo, '\\|')) temp AS temp_col ) tt
                                    GROUP BY
                                             source_id
                                    ,        product_line ) b
                 ON
                           a.source_id    = b.source_id
                 AND       a.product_line = b.product_line
                 LEFT JOIN
                           (
                                  select
                                         *
                                  from
                                         dws_wks_coverage_analysis_uncovered_levelid_df
                                  where
                                         pday = ${bdp.system.bizdate}) c
                 ON
                           a.source_id    = c.level_id
                 AND       a.product_line = c.product_line
                 ORDER BY
                           demands DESC ) t

UNION ALL
---PL024--ffo---fule filter
SELECT
       source_id
,      product_line
,      OE
,      demands
FROM
       (
                 SELECT
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty / b.oe_count AS demands
                 FROM
                           (
                                  SELECT
                                         source_id
                                  ,      product_line
                                  ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                  FROM
                                         (
                                                SELECT
                                                       source_id
                                                ,      'PL024' AS product_line
                                                ,      ffo
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                WHERE
                                                       ds = '${yyyyMMdd,-1d}'
                                                AND    source_id IN
                                                       (
                                                              SELECT
                                                                     level_id
                                                              FROM
                                                                     dws_wks_coverage_analysis_uncovered_levelid_df
                                                              WHERE
                                                                     pday         = ${bdp.system.bizdate}
                                                              and    product_line = 'PL024' ) ) t LATERAL VIEW explode(split(ffo, '\\|')) temp AS temp_col ) a
                 LEFT JOIN
                           (
                                    SELECT
                                             source_id
                                    ,        product_line
                                    ,        count(DISTINCT oe) AS oe_count
                                    FROM
                                             (
                                                    SELECT
                                                           source_id
                                                    ,      product_line
                                                    ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                                    FROM
                                                           (
                                                                  SELECT
                                                                         source_id
                                                                  ,      'PL024' AS product_line
                                                                  ,      ffo
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  WHERE
                                                                         ds = '${yyyyMMdd,-1d}'
                                                                  AND    source_id IN
                                                                         (
                                                                                SELECT
                                                                                       level_id
                                                                                FROM
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_df
                                                                                WHERE
                                                                                       pday         = ${bdp.system.bizdate}
                                                                                and    product_line = 'PL024' ) ) t LATERAL VIEW explode(split(ffo, '\\|')) temp AS temp_col ) tt
                                    GROUP BY
                                             source_id
                                    ,        product_line ) b
                 ON
                           a.source_id    = b.source_id
                 AND       a.product_line = b.product_line
                 LEFT JOIN
                           (
                                  select
                                         *
                                  from
                                         dws_wks_coverage_analysis_uncovered_levelid_df
                                  where
                                         pday = ${bdp.system.bizdate}) c
                 ON
                           a.source_id    = c.level_id
                 AND       a.product_line = c.product_line
                 ORDER BY
                           demands DESC ) t

UNION ALL
---PL018-bfpon--Brake pad fr
SELECT
       source_id
,      product_line
,      OE
,      demands
FROM
       (
                 SELECT
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty / b.oe_count AS demands
                 FROM
                           (
                                  SELECT
                                         source_id
                                  ,      product_line
                                  ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                  FROM
                                         (
                                                SELECT
                                                       source_id
                                                ,      'PL018' AS product_line
                                                ,      bfpon
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                WHERE
                                                       ds = '${yyyyMMdd,-1d}'
                                                AND    source_id IN
                                                       (
                                                              SELECT
                                                                     level_id
                                                              FROM
                                                                     dws_wks_coverage_analysis_uncovered_levelid_df
                                                              WHERE
                                                                     pday         = ${bdp.system.bizdate}
                                                              and    product_line = 'PL018' ) ) t LATERAL VIEW explode(split(bfpon, '\\|')) temp AS temp_col ) a
                 LEFT JOIN
                           (
                                    SELECT
                                             source_id
                                    ,        product_line
                                    ,        count(DISTINCT oe) AS oe_count
                                    FROM
                                             (
                                                    SELECT
                                                           source_id
                                                    ,      product_line
                                                    ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                                    FROM
                                                           (
                                                                  SELECT
                                                                         source_id
                                                                  ,      'PL018' AS product_line
                                                                  ,      bfpon
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  WHERE
                                                                         ds = '${yyyyMMdd,-1d}'
                                                                  AND    source_id IN
                                                                         (
                                                                                SELECT
                                                                                       level_id
                                                                                FROM
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_df
                                                                                WHERE
                                                                                       product_line = 'PL018' ) ) t LATERAL VIEW explode(split(bfpon, '\\|')) temp AS temp_col ) tt
                                    GROUP BY
                                             source_id
                                    ,        product_line ) b
                 ON
                           a.source_id    = b.source_id
                 AND       a.product_line = b.product_line
                 LEFT JOIN
                           (
                                  select
                                         *
                                  from
                                         dws_wks_coverage_analysis_uncovered_levelid_df
                                  where
                                         pday = ${bdp.system.bizdate}) c
                 ON
                           a.source_id    = c.level_id
                 AND       a.product_line = c.product_line
                 ORDER BY
                           demands DESC ) t

UNION ALL
---PL019--brpon--Brake pad rr
SELECT
       source_id
,      product_line
,      OE
,      demands
FROM
       (
                 SELECT
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty / b.oe_count AS demands
                 FROM
                           (
                                  SELECT
                                         source_id
                                  ,      product_line
                                  ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                  FROM
                                         (
                                                SELECT
                                                       source_id
                                                ,      'PL019' AS product_line
                                                ,      brpon
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                WHERE
                                                       ds = '${yyyyMMdd,-1d}'
                                                AND    source_id IN
                                                       (
                                                              SELECT
                                                                     level_id
                                                              FROM
                                                                     dws_wks_coverage_analysis_uncovered_levelid_df
                                                              WHERE
                                                                     pday         = ${bdp.system.bizdate}
                                                              and    product_line = 'PL019' ) ) t LATERAL VIEW explode(split(brpon, '\\|')) temp AS temp_col ) a
                 LEFT JOIN
                           (
                                    SELECT
                                             source_id
                                    ,        product_line
                                    ,        count(DISTINCT oe) AS oe_count
                                    FROM
                                             (
                                                    SELECT
                                                           source_id
                                                    ,      product_line
                                                    ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                                    FROM
                                                           (
                                                                  SELECT
                                                                         source_id
                                                                  ,      'PL019' AS product_line
                                                                  ,      brpon
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  WHERE
                                                                         ds = '${yyyyMMdd,-1d}'
                                                                  AND    source_id IN
                                                                         (
                                                                                SELECT
                                                                                       level_id
                                                                                FROM
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_df
                                                                                WHERE
                                                                                       pday         = ${bdp.system.bizdate}
                                                                                and    product_line = 'PL019' ) ) t LATERAL VIEW explode(split(brpon, '\\|')) temp AS temp_col ) tt
                                    GROUP BY
                                             source_id
                                    ,        product_line ) b
                 ON
                           a.source_id    = b.source_id
                 AND       a.product_line = b.product_line
                 LEFT JOIN
                           (
                                  select
                                         *
                                  from
                                         dws_wks_coverage_analysis_uncovered_levelid_df
                                  where
                                         pday = ${bdp.system.bizdate}) c
                 ON
                           a.source_id    = c.level_id
                 AND       a.product_line = c.product_line
                 ORDER BY
                           demands DESC ) t

UNION ALL
---PL027--foso--LS
SELECT
       source_id
,      product_line
,      OE
,      demands
FROM
       (
                 SELECT
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty / b.oe_count AS demands
                 FROM
                           (
                                  SELECT
                                         source_id
                                  ,      product_line
                                  ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                  FROM
                                         (
                                                SELECT
                                                       source_id
                                                ,      'PL027' AS product_line
                                                ,      foso
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                WHERE
                                                       ds = '${yyyyMMdd,-1d}'
                                                AND    source_id IN
                                                       (
                                                              SELECT
                                                                     level_id
                                                              FROM
                                                                     dws_wks_coverage_analysis_uncovered_levelid_df
                                                              WHERE
                                                                     pday         = ${bdp.system.bizdate}
                                                              and    product_line = 'PL027' ) ) t LATERAL VIEW explode(split(foso, '\\|')) temp AS temp_col ) a
                 LEFT JOIN
                           (
                                    SELECT
                                             source_id
                                    ,        product_line
                                    ,        count(DISTINCT oe) AS oe_count
                                    FROM
                                             (
                                                    SELECT
                                                           source_id
                                                    ,      product_line
                                                    ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                                    FROM
                                                           (
                                                                  SELECT
                                                                         source_id
                                                                  ,      'PL027' AS product_line
                                                                  ,      foso
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  WHERE
                                                                         ds = '${yyyyMMdd,-1d}'
                                                                  AND    source_id IN
                                                                         (
                                                                                SELECT
                                                                                       level_id
                                                                                FROM
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_df
                                                                                WHERE
                                                                                       pday         = ${bdp.system.bizdate}
                                                                                and    product_line = 'PL027' ) ) t LATERAL VIEW explode(split(foso, '\\|')) temp AS temp_col ) tt
                                    GROUP BY
                                             source_id
                                    ,        product_line ) b
                 ON
                           a.source_id    = b.source_id
                 AND       a.product_line = b.product_line
                 LEFT JOIN
                           (
                                  select
                                         *
                                  from
                                         dws_wks_coverage_analysis_uncovered_levelid_df
                                  where
                                         pday = ${bdp.system.bizdate}) c
                 ON
                           a.source_id    = c.level_id
                 AND       a.product_line = c.product_line
                 ORDER BY
                           demands DESC ) t

UNION ALL
---PL027--roso--LS
SELECT
       source_id
,      product_line
,      OE
,      demands
FROM
       (
                 SELECT
                           a.source_id
                 ,         a.product_line
                 ,         a.oe
                 ,         c.qty / b.oe_count AS demands
                 FROM
                           (
                                  SELECT
                                         source_id
                                  ,      product_line
                                  ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                  FROM
                                         (
                                                SELECT
                                                       source_id
                                                ,      'PL027' AS product_line
                                                ,      roso
                                                FROM
                                                       ods_spiderb_sys_vehicle_other2_df
                                                WHERE
                                                       ds = '${yyyyMMdd,-1d}'
                                                AND    source_id IN
                                                       (
                                                              SELECT
                                                                     level_id
                                                              FROM
                                                                     dws_wks_coverage_analysis_uncovered_levelid_df
                                                              WHERE
                                                                     pday         = ${bdp.system.bizdate}
                                                              and    product_line = 'PL027' ) ) t LATERAL VIEW explode(split(roso, '\\|')) temp AS temp_col ) a
                 LEFT JOIN
                           (
                                    SELECT
                                             source_id
                                    ,        product_line
                                    ,        count(DISTINCT oe) AS oe_count
                                    FROM
                                             (
                                                    SELECT
                                                           source_id
                                                    ,      product_line
                                                    ,      translate( split(split(split(temp_col, '\\(') [0], '\\)') [0], '\\.') [0], '\\ ', '\\' ) AS OE
                                                    FROM
                                                           (
                                                                  SELECT
                                                                         source_id
                                                                  ,      'PL027' AS product_line
                                                                  ,      roso
                                                                  FROM
                                                                         ods_spiderb_sys_vehicle_other2_df
                                                                  WHERE
                                                                         ds = '${yyyyMMdd,-1d}'
                                                                  AND    source_id IN
                                                                         (
                                                                                SELECT
                                                                                       level_id
                                                                                FROM
                                                                                       dws_wks_coverage_analysis_uncovered_levelid_df
                                                                                WHERE
                                                                                       pday         = ${bdp.system.bizdate}
                                                                                and    product_line = 'PL027' ) ) t LATERAL VIEW explode(split(roso, '\\|')) temp AS temp_col ) tt
                                    GROUP BY
                                             source_id
                                    ,        product_line ) b
                 ON
                           a.source_id    = b.source_id
                 AND       a.product_line = b.product_line
                 LEFT JOIN
                           (
                                  select
                                         *
                                  from
                                         dws_wks_coverage_analysis_uncovered_levelid_df
                                  where
                                         pday = ${bdp.system.bizdate}) c
                 ON
                           a.source_id    = c.level_id
                 AND       a.product_line = c.product_line
                 ORDER BY
                           demands DESC ) t