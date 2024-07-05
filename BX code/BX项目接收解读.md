#BX项目接收解读

## ODS层

###A单

- bosch_accessories_recycling—— A单配件回收基本信息表
- bosch_maintenance_station——A维修站基本信息表
- bosch_accessories_recycling_log——A 单状态变更表
- bosch_accessories_white_list——配件回收白名单

###B单

- bosch_dealer——B经销商基本信息表



##cdm层

###DIM

- dim_wks_bx_diesel_workshop 维修站维度表

  > 来源：A维修站基本信息表

  - 基本信息：
    - id主键、name维修站名称、phone维修站电话
    - address维修站地址、province省份、city城市

  - 位置：lat维度、lng经度
  - 客户：
    - extra_id客户易加网id
    - accumulate_points累计易加网积分


  - 标识： 
    - tag分类
    - station_role——维修站角色：CV, BX, CV&BX
    - station_number——内部编号
    - syn_code——维修站编号
    - upload_bosch——是否提交博世总部
    - update_time——更新时间
    - etl_load_time——加载时间

- dim_del_bx_dealer 经销商维度表

  > B经销商基本信息表

  - id主键、name名称、is_dealers是否总经销商（0不是，1是）、parent_distributor_id父经销商id、
  - distributor_count：子经销商数量
  - code：编号、address：地址、phone：电话、sys_user_id：经销商用户id、update_time：更新时间、etl_load_time：数据加载时间

- dim_prd_bx_core 旧件信息维度表



###DWD

- dwd_wks_bx_core_recycling_df dwd综合信息表

- dwd_wks_bx_core_recycling_tmp01_df——从A 单状态变更表抽取：旧件回收订单id、到达状态（1到，0未到）、回收状态（1-9）、当前状态对应信息、创建时间、状态发生时间、更新时间
- dwd_wks_bx_core_recycling_tmp02_df——从A单配件回收基本信息表抽取：id、维修站id、经销商id、订单号、物流id、料号
- dwd_wks_bx_core_recycling_tmp03_df

- dwd_wks_bx_core_recycling_tmp04_df

- dwd_wks_bx_core_recycling_tmp05_df——从维修站维度表中抽取：维修站id、name、客户加网id、省、市、分类、角色、维修站编号、提交总部状态



## ads层

- ads_wks_bx_core_recycling_tmp01_df dwd——转ads的中转表
- ads_wks_bx_core_recycling_avg_time_als_df ——计算到达状态十三的平均时间表
- ads_wks_bx_core_recycling_city_count_df  ——旧件订单城市维度创建数量统计表
- ads_wks_bx_core_recycling_last_status_info_df—— 旧件订单最新状态信息表
- ads_wks_bx_core_recycling_latest_status_df  ——旧件订单最新状态统计表
- ads_wks_bx_core_recycling_order_create_num_count_day_df ——旧件每天订单创建统计表
- ads_wks_bx_core_recycling_workshop_count_df ——旧件订单维修站维度创建数量统计表
