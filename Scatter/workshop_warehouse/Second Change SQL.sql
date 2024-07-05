INSERT OVERWRITE TABLE dws_workshop_warehouse_df
SELECT client_id,
       client_code,
       client_name,
       from_app2                                                              AS customer_code,
       dealer,
       case when warehouse_name2 is not null then warehouse_name2 else '' end as warehouse_name,
       city_name,
       workshop_type,
       typeof,
       date_format(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss')                AS etl_load_time

from (select a.*,
             CASE
                 WHEN a.from_app2 = 'KZ' THEN '康众'
                 ELSE c.dealer
                 END          AS dealer,
             e.warehouse_name as warehouse_name2,
             d.client_code,
             d.client_name,
             d.city_name,
             d.workshop_type,
             d.typeof
      FROM (SELECT a.*,
                   b.warehouse_id

            FROM (SELECT *
                  FROM (SELECT client_id,
                               from_app2,
                               store_code,
                               store_name,
                               warehouse_code,
                               warehouse_name,
                               row_number() OVER(
                                PARTITION by client_id
                                ORDER BY
                                    sales_amount DESC
                            ) rn1
                        FROM (SELECT client_id,
                                     from_app2,
                                     store_code,
                                     store_name,
                                     warehouse_code,
                                     warehouse_name,
                                     sum(
                                             CASE
                                                 WHEN from_app = 'casstime' THEN product_amount
                                                 ELSE sales_amount
                                                 END
                                     ) AS sales_amount
                              FROM (SELECT a.*,
                                           CASE
                                               WHEN a.from_app = 'casstime' AND a.short_name = b.erp_shop_name
                                                   THEN b.customer_code
                                               ELSE a.from_app
                                               END AS from_app2
                                    FROM (SELECT *
                                          FROM dws_del_iam_dealer_sellout_df
                                          WHERE ds = ${bdp.system.bizdate}) a
                                             LEFT JOIN
                                         ads_cass_erp_shop_name_mapping_temp_df b ON a.short_name = b.erp_shop_name) tmp
                              GROUP BY client_id,
                                       from_app2,
                                       store_code,
                                       store_name,
                                       warehouse_code,
                                       warehouse_name) t --全掉康众
                        WHERE from_app2 <> 'casstime') tt
                  WHERE rn1 = 1
                    AND client_id <> 0) a
                     LEFT JOIN
                 dim_wks_sellout_warehouse b
                 ON a.from_app2 = b.from_app AND a.store_code = b.store_code and a.store_name = b.store_name and
                    a.warehouse_code = b.warehouse_code and a.warehouse_name = b.warehouse_name) a
               LEFT JOIN
           ads_city_dealer_new c ON a.from_app2 = c.dealer_code
               LEFT JOIN
           (SELECT *
            FROM dim_wks_extra_workshop_master_data
            WHERE ds = ${bdp.system.bizdate}
              AND typeof = 'Gasoline') d ON a.client_id = d.client_id
               left join dim_wks_warehouse_master_data e on a.warehouse_id = e.warehouse_id) ttt
WHERE typeof = 'Gasoline'
  AND workshop_type IN ('BSS', 'BSS备选', 'FRC', 'extra普通')