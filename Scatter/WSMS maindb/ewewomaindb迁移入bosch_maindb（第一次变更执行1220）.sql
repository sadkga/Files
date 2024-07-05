

-- Table : construction_equipment
-- Type : alter
-- RelationTables :
-- Comment : ok
-- SQL :
ALTER TABLE `construction_equipment`
ADD UNIQUE KEY `uniq_equipment_name` (`equipment_name`),
DROP INDEX `equipment_name`;


-- Table : daily_operational_data
-- Type : alter
-- RelationTables :
-- Comment : 不变， 除新增了字段
-- SQL :
ALTER TABLE `daily_operational_data`
DROP PRIMARY KEY,
ADD PRIMARY KEY (`id`);

-- Table : month_operational_data
-- Type : alter
-- RelationTables :
-- Comment :不变， 除新增了字段
-- SQL :
ALTER TABLE `month_operational_data`
DROP PRIMARY KEY,
ADD PRIMARY KEY (`id`);

-- Table : print_store_template
-- Type : alter
-- RelationTables :
-- Comment :  ok
-- SQL :
ALTER TABLE `print_store_template`
DROP PRIMARY KEY,
ADD PRIMARY KEY (`id`),
DROP INDEX `uni_store_template`,
ADD UNIQUE KEY `uni_store_template` (`store_no`,`biz_code`,`template_id`);





-- Table : standard_labour_template
-- Type : alter
-- RelationTables :
-- Comment : ok
-- SQL :
ALTER TABLE `standard_labour_template`
ADD `car_level_description` text COMMENT '车辆等级说明信息' AFTER `updated_at`;

-- Table : standard_labour_treatment
-- Type : alter
-- RelationTables :
-- Comment :
-- SQL :
ALTER TABLE `standard_labour_treatment`
ADD UNIQUE KEY `uniq_treatment_name` (`treatment_name`);




-- Table : tb_diagnose_report_data_flow
-- Type : alter
-- RelationTables :
-- Comment :
-- SQL :
ALTER TABLE `tb_diagnose_report_data_flow`
ADD KEY `idx_diagnose_report_record_id` (`diagnose_report_record_id`);



-- Table : tb_diagnose_report_record
-- Type : alter
-- RelationTables :
-- Comment :
-- SQL :
ALTER TABLE `tb_diagnose_report_record`
ADD `launch_diagnose_id` varchar(64) NOT NULL COMMENT '元征诊断报告ID' AFTER `updated_at`,
ADD UNIQUE KEY `uniq_launch_diagnose_id` (`launch_diagnose_id`),
ADD KEY `idx_vin` (`vin`);

-- Table : tb_diagnose_report_subsystem
-- Type : alter
-- RelationTables :
-- Comment :
-- SQL :
ALTER TABLE `tb_diagnose_report_subsystem`
ADD KEY `idx_diagnose_report_record_id` (`diagnose_report_record_id`);



-- Table : tb_jy_apirecord
-- Type : alter
-- RelationTables :
-- Comment :
-- SQL :
ALTER TABLE `tb_jy_apirecord`
ADD KEY `idx_maindbstoreid` (`maindbstoreid`);



-- Table : tb_jy_carmodel
-- Type : alter
-- RelationTables :
-- Comment :
-- SQL :
ALTER TABLE `tb_jy_carmodel`
ADD `emission_standard` varchar(100) DEFAULT NULL COMMENT '排放标准' AFTER `gearboxModel`,
ADD `fuel_type` varchar(100) DEFAULT NULL COMMENT '燃料类型' AFTER `emission_standard`,
ADD `engine` varchar(100) DEFAULT NULL COMMENT '发动机号' AFTER `fuel_type`;



-- Table : tb_jy_carmodelstructparts
-- Type : alter
-- RelationTables :
-- Comment :
-- SQL :
ALTER TABLE `tb_jy_carmodelstructparts`
ADD KEY `idx_oetrimspace` (`oetrimspace`),
ADD KEY `vid` (`vehicleId`);



-- Table : tb_ly_carmodel
-- Type : alter
-- RelationTables :
-- Comment :
-- SQL :
--ALTER TABLE `tb_ly_carmodel`
--ADD UNIQUE KEY `sales_style_id` (`sales_style_id`);



-- Table : tb_stat_servicerecord
-- Type : alter
-- RelationTables :
-- Comment :
-- SQL :
ALTER TABLE `tb_stat_servicerecord`
ADD KEY `idx_sr_createnum_dbid_subdbstoreid` (`sr_createnum`,`dbid`,`subdbstoreid`),
ADD KEY `idx_stat_time_subdbstoreid_dbid` (`stat_time`,`subdbstoreid`,`dbid`,`sr_createnum`,`sr_actualtotal`,`instoragetotalmoney`);

-- Table : tb_stat_servicerecord_total
-- Type : alter
-- RelationTables :
-- Comment :
-- SQL :
ALTER TABLE `tb_stat_servicerecord_total`
CHANGE `createtime` `createtime` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP;

-- Table : tb_store
-- Type : alter
-- RelationTables :
-- Comment :
-- SQL :
ALTER TABLE `tb_store`
CHANGE `ksstoresource` `ksstoresource` int(11) DEFAULT NULL COMMENT '电商账号来源（1:易维2:一号车间3:一账通4:报价助手)',
CHANGE `customer_status` `customer_status` smallint(6) DEFAULT NULL COMMENT '客户状态：1-活跃、2-停用',
CHANGE `istargetcustomer` `istargetcustomer` bit(1) DEFAULT b'1' COMMENT '是否目标客户',
CHANGE `deliveryChief` `deliveryChief` varchar(50) DEFAULT NULL,
CHANGE `deliveryOffline` `deliveryOffline` varchar(50) DEFAULT NULL,
ADD KEY `idx_salepackage` (`salepackage`),
ADD KEY `idx_dbid_masterstoreid` (`dbid`,`masterstoreid`);



-- Table : tb_storelog
-- Type : alter
-- RelationTables :
-- Comment :
-- SQL :
ALTER TABLE `tb_storelog`
ADD `module` varchar(30) DEFAULT NULL COMMENT '模块' AFTER `systemtype`,
ADD `page` varchar(32) DEFAULT NULL COMMENT '功能页面' AFTER `module`,
ADD `businessid` varchar(16) DEFAULT NULL COMMENT '业务对象' AFTER `page`,
ADD `businesstype` varchar(16) DEFAULT NULL COMMENT '业务类型' AFTER `businessid`,
ADD `operate` varchar(8) DEFAULT NULL COMMENT '操作' AFTER `businesstype`,
ADD `url` varchar(64) DEFAULT NULL COMMENT 'url' AFTER `operate`,
ADD `remark` text COMMENT 'remark' AFTER `url`;

-- Table : tb_storelog_couponmsg
-- Type : alter
-- RelationTables :
-- Comment :
-- SQL :
ALTER TABLE `tb_storelog_couponmsg`
ADD KEY `idx_userprojectid` (`sendstate`,`userprojectid`);



-- Table : tb_storeuser
-- Type : alter
-- RelationTables :
-- Comment :
-- SQL :
ALTER TABLE `tb_storeuser`
ADD `isnewversion` int(11) DEFAULT NULL COMMENT '是否是新平台用户' AFTER `pserrorcount`,
CHANGE `default_store_id` `default_store_id` int(11) DEFAULT NULL COMMENT '默认登录的门店id',
CHANGE `store_ids` `store_ids` varchar(1024) DEFAULT NULL COMMENT '允许登录的门店列表',
CHANGE `mobile` `mobile` varchar(50) DEFAULT NULL COMMENT '用户手机号';



-- Table : tb_unionagent
-- Type : alter
-- RelationTables :
-- Comment :
-- SQL :
ALTER TABLE `tb_unionagent`
ADD `topcolour` varchar(100) DEFAULT NULL COMMENT '联盟对应首页顶部颜色' AFTER `icpno`,
ADD `switchphones` varchar(300) DEFAULT NULL COMMENT '切换图片集合（用，分隔）' AFTER `topcolour`;

-- Table : tb_upgrade_notification
-- Type : alter
-- RelationTables :
-- Comment :
-- SQL :
ALTER TABLE `tb_upgrade_notification`
CHANGE `updated_at` `updated_at` datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP;

