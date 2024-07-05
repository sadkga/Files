## BX数据表

### BX_CORE_info

> 以A单状态变更表为主体的综合信息表可以用来查阅具体的订单信息（过滤看条件限定）

#### 数据来源

- bosch_accessories_recycling_log——A  单状态变更表（主表）
- [bosch_accessories_recycling](#bosch_accessories_recycling!A1)——A单配件回收基本信息表
- [bosch_accessories_white_list](#bosch_accessories_white_list!A1)——配件白名单(可回收)表
- [bosch_dealer](#bosch_dealer!A1)——B经销商基本信息表
- [bosch_maintenance_station](#bosch_maintenance_station!A1)——A维修站基本信息表



#### 宽表结构

![image-20230605105231713](https://article.biliimg.com/bfs/article/b54a733edcba287c7a2f86a8d8ae673829171161.png)



#### 条件限定

```hive
WHERE
    (
        workshop_syn_code LIKE '%IWSD%' OR workshop_syn_code LIKE '%76%'
    ) AND distributor_name != '%测试%' AND accessories_recycling_id IN (
        SELECT
            accessories_recycling_id
        FROM
            dwd_bx_bosch_accessories_recycling_log
        WHERE
            STATUS = '13'
    ) AND is_arrive = 1
```





### BX_CORE_status_info

> 以上面过滤后的信息表为源，计算每个订单号的每个状态的每月的平均时间差

#### 数据来源

- BX_CORE_info



#### 表结构

![image-20230605105835432](https://article.biliimg.com/bfs/article/3817cf33089c4bf1274fd4e606423ee2cfe8ce94.png)



#### 条件限定

- 根据状态、月份分组



### BX_CORE_recycling_order_info

> 以A单回收基本信息表为主体，未进行任何过滤的综合信息表，服务于parcetage和workshop_num表

#### 数据来源

- bosch_accessories_recycling_log——A  单状态变更表
- [bosch_accessories_recycling](#bosch_accessories_recycling!A1)——A单配件回收基本信息表（主表）
- [bosch_accessories_white_list](#bosch_accessories_white_list!A1)——配件白名单(可回收)表
- [bosch_dealer](#bosch_dealer!A1)——B经销商基本信息表
- [bosch_maintenance_station](#bosch_maintenance_station!A1)——A维修站基本信息表



#### 宽表结构

![image-20230605110723567](https://article.biliimg.com/bfs/article/39b557af80e4685a882b1cb56ad215b20059c9cb.png)



#### 条件限定 

无



### BX_CORE_order_status_parcetage

> 以recycling_order_info为源，计算月订单占比

#### 数据来源

BX_CORE_recycling_order_info



#### 表结构

![image-20230605111019979](https://article.biliimg.com/bfs/article/1332f2dca4bcfc8898f56c63300aaa4d87201f6b.png)



#### 条件限定

```hive
where (workshop_syn_code like '%IWSD%' or workshop_syn_code like '%76%') and distributor_name != '%测试%' 

按照状态、描述信息、月份进行分组
```



### BX_CORE_workshop_order_num

> 以recycling_order_info为源，统计每个维修站的月订单量

#### 数据来源

BX_CORE_recycling_order_info



#### 表结构

![image-20230605111458584](https://article.biliimg.com/bfs/article/7dfcc0b4d913e59d35d8d5e8949f896e85dc2110.png)



#### 条件限定

```hive
where (workshop_syn_code like '%IWSD%' or workshop_syn_code like '%76%') and distributor_name != '%测试%' 

按照维修站编号、名称、月份分组
```

