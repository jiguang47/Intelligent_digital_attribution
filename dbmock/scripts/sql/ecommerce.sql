-- 电商数仓Doris一致性维度与原子事实建表脚本
-- 适用数据库为Apache Doris 4.0及以上版本
-- UNIQUE KEY用于保持业务装载幂等，关系约束由数据质量任务校验
-- 单节点开发环境使用一个副本，生产环境应按BE节点数调整副本策略

CREATE TABLE dim_date (
    date_key INT NOT NULL COMMENT '日期键，格式YYYYMMDD',
    full_date DATE NOT NULL COMMENT '自然日期',
    calendar_year SMALLINT NOT NULL COMMENT '自然年',
    calendar_quarter TINYINT NOT NULL COMMENT '自然季度',
    calendar_month TINYINT NOT NULL COMMENT '自然月',
    year_month_code CHAR(7) NOT NULL COMMENT '年月，格式YYYY-MM',
    week_of_year TINYINT NOT NULL COMMENT '年内周序号',
    day_of_month TINYINT NOT NULL COMMENT '月内日序号',
    day_of_week TINYINT NOT NULL COMMENT '周内日序号，1表示周一',
    day_name_cn VARCHAR(32) NOT NULL COMMENT '中文星期名称',
    is_weekend TINYINT NOT NULL DEFAULT 0 COMMENT '是否周末:0否 1是',
    is_holiday TINYINT NOT NULL DEFAULT 0 COMMENT '是否法定节假日:0否 1是',
    is_workday TINYINT NOT NULL DEFAULT 1 COMMENT '是否工作日:0否 1是',
    holiday_name VARCHAR(256) DEFAULT NULL COMMENT '节假日名称',
    fiscal_year SMALLINT NOT NULL COMMENT '财年',
    fiscal_quarter TINYINT NOT NULL COMMENT '财年季度'
)
ENGINE = OLAP
UNIQUE KEY (`date_key`)
COMMENT '公共日期维度'
DISTRIBUTED BY HASH (`date_key`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dim_channel_info (
    channel_sk BIGINT NOT NULL AUTO_INCREMENT COMMENT '渠道维度代理键',
    channel_code VARCHAR(128) NOT NULL COMMENT '渠道业务编码',
    channel_name VARCHAR(256) NOT NULL COMMENT '渠道名称',
    channel_group VARCHAR(128) DEFAULT NULL COMMENT '渠道分组',
    platform_type VARCHAR(128) DEFAULT NULL COMMENT '平台类型',
    traffic_source_type VARCHAR(128) DEFAULT NULL COMMENT '流量来源类型',
    channel_status TINYINT NOT NULL DEFAULT 1 COMMENT '状态:0停用 1启用',
    is_deleted TINYINT NOT NULL DEFAULT 0 COMMENT '源记录是否删除:0否 1是',
    attribute_hash CHAR(64) NOT NULL COMMENT '业务属性哈希',
    source_update_time DATETIME(6) DEFAULT NULL COMMENT '源系统更新时间',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '首次入仓时间',
    dw_update_time DATETIME(6) NOT NULL COMMENT '最近更新时间'
)
ENGINE = OLAP
UNIQUE KEY (`channel_sk`)
COMMENT '渠道Type 1一致性维度'
DISTRIBUTED BY HASH (`channel_sk`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dim_page_info (
    page_sk BIGINT NOT NULL AUTO_INCREMENT COMMENT '页面维度代理键',
    page_id VARCHAR(256) NOT NULL COMMENT '页面业务ID',
    page_name VARCHAR(512) NOT NULL COMMENT '页面名称',
    page_type VARCHAR(128) NOT NULL COMMENT '页面类型',
    business_domain VARCHAR(128) DEFAULT NULL COMMENT '所属业务域',
    page_path_pattern VARCHAR(2048) DEFAULT NULL COMMENT '页面路径模板',
    page_status TINYINT NOT NULL DEFAULT 1 COMMENT '状态:0停用 1启用',
    is_deleted TINYINT NOT NULL DEFAULT 0 COMMENT '源记录是否删除:0否 1是',
    attribute_hash CHAR(64) NOT NULL COMMENT '业务属性哈希',
    source_update_time DATETIME(6) DEFAULT NULL COMMENT '源系统更新时间',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '首次入仓时间',
    dw_update_time DATETIME(6) NOT NULL COMMENT '最近更新时间'
)
ENGINE = OLAP
UNIQUE KEY (`page_sk`)
COMMENT '页面Type 1一致性维度'
DISTRIBUTED BY HASH (`page_sk`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dim_geo_region_zip (
    region_sk BIGINT NOT NULL AUTO_INCREMENT COMMENT '行政区域维度代理键',
    region_code VARCHAR(80) NOT NULL COMMENT '区域业务编码',
    region_name VARCHAR(512) NOT NULL COMMENT '区域名称',
    region_level TINYINT NOT NULL COMMENT '区域级别:1国家 2省 3市 4区县 5街道',
    parent_region_code VARCHAR(80) DEFAULT NULL COMMENT '父级区域编码',
    country_code VARCHAR(80) DEFAULT NULL COMMENT '国家编码',
    country_name VARCHAR(512) DEFAULT NULL COMMENT '国家名称',
    province_code VARCHAR(80) DEFAULT NULL COMMENT '省编码',
    province_name VARCHAR(512) DEFAULT NULL COMMENT '省名称',
    city_code VARCHAR(80) DEFAULT NULL COMMENT '市编码',
    city_name VARCHAR(512) DEFAULT NULL COMMENT '市名称',
    district_code VARCHAR(80) DEFAULT NULL COMMENT '区县编码',
    district_name VARCHAR(512) DEFAULT NULL COMMENT '区县名称',
    region_path VARCHAR(2048) DEFAULT NULL COMMENT '完整区域路径',
    zip_code VARCHAR(64) DEFAULT NULL COMMENT '邮编',
    region_status TINYINT NOT NULL DEFAULT 1 COMMENT '状态:0停用 1启用',
    effective_start_time DATETIME(6) NOT NULL COMMENT '版本生效时间',
    effective_end_time DATETIME(6) NOT NULL COMMENT '版本失效时间',
    version_no INT NOT NULL DEFAULT 1 COMMENT '版本号',
    is_current TINYINT NOT NULL DEFAULT 1 COMMENT '是否当前版本:0否 1是',
    is_deleted TINYINT NOT NULL DEFAULT 0 COMMENT '源记录是否删除:0否 1是',
    attribute_hash CHAR(64) NOT NULL COMMENT '业务属性哈希',
    source_update_time DATETIME(6) DEFAULT NULL COMMENT '源系统更新时间',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '首次入仓时间',
    dw_update_time DATETIME(6) NOT NULL COMMENT '最近更新时间'
)
ENGINE = OLAP
UNIQUE KEY (`region_sk`)
COMMENT '行政区域一致性维度拉链表'
DISTRIBUTED BY HASH (`region_sk`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dim_user_info_zip (
    user_sk BIGINT NOT NULL AUTO_INCREMENT COMMENT '用户维度代理键',
    user_id BIGINT NOT NULL COMMENT '用户业务ID',
    user_name VARCHAR(256) DEFAULT NULL COMMENT '用户名',
    nick_name VARCHAR(256) DEFAULT NULL COMMENT '昵称',
    gender VARCHAR(32) NOT NULL DEFAULT '未知' COMMENT '性别:未知/男/女',
    birthday DATE DEFAULT NULL COMMENT '生日',
    phone VARCHAR(80) DEFAULT NULL COMMENT '手机号脱敏值',
    email VARCHAR(512) DEFAULT NULL COMMENT '邮箱脱敏值',
    register_time DATETIME(6) DEFAULT NULL COMMENT '注册时间',
    register_channel_code VARCHAR(128) DEFAULT NULL COMMENT '注册渠道编码',
    register_source VARCHAR(128) DEFAULT NULL COMMENT '注册来源',
    user_level VARCHAR(64) NOT NULL DEFAULT '1' COMMENT '会员等级',
    is_vip TINYINT NOT NULL DEFAULT 0 COMMENT '是否VIP:0否 1是',
    province_code VARCHAR(80) DEFAULT NULL COMMENT '常驻省编码',
    city_code VARCHAR(80) DEFAULT NULL COMMENT '常驻市编码',
    district_code VARCHAR(80) DEFAULT NULL COMMENT '常驻区编码',
    occupation VARCHAR(256) DEFAULT NULL COMMENT '职业',
    income_level VARCHAR(128) DEFAULT NULL COMMENT '收入等级',
    education_level VARCHAR(128) DEFAULT NULL COMMENT '学历等级',
    marital_status VARCHAR(64) DEFAULT NULL COMMENT '婚姻状态',
    user_status VARCHAR(64) NOT NULL DEFAULT '正常' COMMENT '用户状态',
    effective_start_time DATETIME(6) NOT NULL COMMENT '版本生效时间',
    effective_end_time DATETIME(6) NOT NULL COMMENT '版本失效时间',
    version_no INT NOT NULL DEFAULT 1 COMMENT '版本号',
    is_current TINYINT NOT NULL DEFAULT 1 COMMENT '是否当前版本:0否 1是',
    is_deleted TINYINT NOT NULL DEFAULT 0 COMMENT '源记录是否删除:0否 1是',
    attribute_hash CHAR(64) NOT NULL COMMENT '业务属性哈希',
    source_update_time DATETIME(6) DEFAULT NULL COMMENT '源系统更新时间',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '首次入仓时间',
    dw_update_time DATETIME(6) NOT NULL COMMENT '最近更新时间'
)
ENGINE = OLAP
UNIQUE KEY (`user_sk`)
COMMENT '用户一致性维度拉链表'
DISTRIBUTED BY HASH (`user_sk`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dim_user_tag_info (
    user_tag_sk BIGINT NOT NULL AUTO_INCREMENT COMMENT '用户标签代理键',
    tag_code VARCHAR(256) NOT NULL COMMENT '标签编码',
    tag_name VARCHAR(512) NOT NULL COMMENT '标签名称',
    tag_group VARCHAR(256) DEFAULT NULL COMMENT '标签分组',
    tag_value_type VARCHAR(64) NOT NULL DEFAULT 'BOOLEAN' COMMENT '标签值类型',
    tag_description VARCHAR(2048) DEFAULT NULL COMMENT '标签说明',
    tag_status TINYINT NOT NULL DEFAULT 1 COMMENT '状态:0停用 1启用',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '首次入仓时间',
    dw_update_time DATETIME(6) NOT NULL COMMENT '最近更新时间'
)
ENGINE = OLAP
UNIQUE KEY (`user_tag_sk`)
COMMENT '用户标签维度'
DISTRIBUTED BY HASH (`user_tag_sk`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE bridge_user_tag_relation_zip (
    user_tag_relation_sk BIGINT NOT NULL AUTO_INCREMENT COMMENT '用户标签关系代理键',
    user_id BIGINT NOT NULL COMMENT '用户业务ID',
    user_tag_sk BIGINT NOT NULL COMMENT '用户标签代理键',
    tag_value VARCHAR(1024) DEFAULT NULL COMMENT '标签值',
    tag_score DECIMAL(10, 6) DEFAULT NULL COMMENT '标签置信度或权重',
    effective_start_time DATETIME(6) NOT NULL COMMENT '关系生效时间',
    effective_end_time DATETIME(6) NOT NULL COMMENT '关系失效时间',
    is_current TINYINT NOT NULL DEFAULT 1 COMMENT '是否当前关系:0否 1是',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`user_tag_relation_sk`)
COMMENT '用户与标签多值关系拉链桥表'
DISTRIBUTED BY HASH (`user_tag_relation_sk`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dim_seller_info_zip (
    seller_sk BIGINT NOT NULL AUTO_INCREMENT COMMENT '商家维度代理键',
    seller_id BIGINT NOT NULL COMMENT '商家业务ID',
    seller_name VARCHAR(512) NOT NULL COMMENT '商家名称',
    seller_type VARCHAR(128) DEFAULT NULL COMMENT '商家类型',
    industry_type VARCHAR(256) DEFAULT NULL COMMENT '所属行业',
    country_code VARCHAR(80) DEFAULT NULL COMMENT '注册国家编码',
    province_code VARCHAR(80) DEFAULT NULL COMMENT '注册省编码',
    city_code VARCHAR(80) DEFAULT NULL COMMENT '注册市编码',
    settle_date DATE DEFAULT NULL COMMENT '入驻日期',
    seller_status VARCHAR(64) NOT NULL DEFAULT '正常' COMMENT '商家状态',
    effective_start_time DATETIME(6) NOT NULL COMMENT '版本生效时间',
    effective_end_time DATETIME(6) NOT NULL COMMENT '版本失效时间',
    version_no INT NOT NULL DEFAULT 1 COMMENT '版本号',
    is_current TINYINT NOT NULL DEFAULT 1 COMMENT '是否当前版本:0否 1是',
    is_deleted TINYINT NOT NULL DEFAULT 0 COMMENT '源记录是否删除:0否 1是',
    attribute_hash CHAR(64) NOT NULL COMMENT '业务属性哈希',
    source_update_time DATETIME(6) DEFAULT NULL COMMENT '源系统更新时间',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '首次入仓时间',
    dw_update_time DATETIME(6) NOT NULL COMMENT '最近更新时间'
)
ENGINE = OLAP
UNIQUE KEY (`seller_sk`)
COMMENT '商家一致性维度拉链表'
DISTRIBUTED BY HASH (`seller_sk`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dim_shop_info_zip (
    shop_sk BIGINT NOT NULL AUTO_INCREMENT COMMENT '店铺维度代理键',
    shop_id BIGINT NOT NULL COMMENT '店铺业务ID',
    shop_name VARCHAR(512) NOT NULL COMMENT '店铺名称',
    shop_type VARCHAR(128) NOT NULL COMMENT '店铺类型',
    seller_id BIGINT NOT NULL COMMENT '商家业务ID',
    industry_type VARCHAR(256) DEFAULT NULL COMMENT '行业类型',
    open_time DATETIME(6) DEFAULT NULL COMMENT '开店时间',
    province_code VARCHAR(80) DEFAULT NULL COMMENT '店铺省编码',
    city_code VARCHAR(80) DEFAULT NULL COMMENT '店铺市编码',
    district_code VARCHAR(80) DEFAULT NULL COMMENT '店铺区编码',
    is_self_operated TINYINT NOT NULL DEFAULT 0 COMMENT '是否自营:0否 1是',
    is_cross_border TINYINT NOT NULL DEFAULT 0 COMMENT '是否跨境:0否 1是',
    shop_status VARCHAR(64) NOT NULL DEFAULT '营业' COMMENT '店铺状态',
    effective_start_time DATETIME(6) NOT NULL COMMENT '版本生效时间',
    effective_end_time DATETIME(6) NOT NULL COMMENT '版本失效时间',
    version_no INT NOT NULL DEFAULT 1 COMMENT '版本号',
    is_current TINYINT NOT NULL DEFAULT 1 COMMENT '是否当前版本:0否 1是',
    is_deleted TINYINT NOT NULL DEFAULT 0 COMMENT '源记录是否删除:0否 1是',
    attribute_hash CHAR(64) NOT NULL COMMENT '业务属性哈希',
    source_update_time DATETIME(6) DEFAULT NULL COMMENT '源系统更新时间',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '首次入仓时间',
    dw_update_time DATETIME(6) NOT NULL COMMENT '最近更新时间'
)
ENGINE = OLAP
UNIQUE KEY (`shop_sk`)
COMMENT '店铺一致性维度拉链表'
DISTRIBUTED BY HASH (`shop_sk`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dim_category_info_zip (
    category_sk BIGINT NOT NULL AUTO_INCREMENT COMMENT '类目维度代理键',
    category_id BIGINT NOT NULL COMMENT '类目业务ID',
    category_name VARCHAR(512) NOT NULL COMMENT '类目名称',
    category_level TINYINT NOT NULL COMMENT '类目层级',
    parent_category_id BIGINT DEFAULT NULL COMMENT '父类目业务ID',
    parent_category_name VARCHAR(512) DEFAULT NULL COMMENT '父类目名称快照',
    root_category_id BIGINT DEFAULT NULL COMMENT '一级类目业务ID',
    root_category_name VARCHAR(512) DEFAULT NULL COMMENT '一级类目名称快照',
    category_path_ids VARCHAR(2048) NOT NULL COMMENT '类目ID完整路径',
    category_path_names VARCHAR(4096) NOT NULL COMMENT '类目名称完整路径',
    is_leaf TINYINT NOT NULL DEFAULT 0 COMMENT '是否叶子类目:0否 1是',
    sort_order INT NOT NULL DEFAULT 0 COMMENT '排序号',
    category_status TINYINT NOT NULL DEFAULT 1 COMMENT '状态:0停用 1启用',
    effective_start_time DATETIME(6) NOT NULL COMMENT '版本生效时间',
    effective_end_time DATETIME(6) NOT NULL COMMENT '版本失效时间',
    version_no INT NOT NULL DEFAULT 1 COMMENT '版本号',
    is_current TINYINT NOT NULL DEFAULT 1 COMMENT '是否当前版本:0否 1是',
    is_deleted TINYINT NOT NULL DEFAULT 0 COMMENT '源记录是否删除:0否 1是',
    attribute_hash CHAR(64) NOT NULL COMMENT '业务属性哈希',
    source_update_time DATETIME(6) DEFAULT NULL COMMENT '源系统更新时间',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '首次入仓时间',
    dw_update_time DATETIME(6) NOT NULL COMMENT '最近更新时间'
)
ENGINE = OLAP
UNIQUE KEY (`category_sk`)
COMMENT '商品类目一致性维度拉链表'
DISTRIBUTED BY HASH (`category_sk`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dim_brand_info (
    brand_sk BIGINT NOT NULL AUTO_INCREMENT COMMENT '品牌维度代理键',
    brand_id BIGINT NOT NULL COMMENT '品牌业务ID',
    brand_name VARCHAR(512) NOT NULL COMMENT '品牌名称',
    brand_name_en VARCHAR(512) DEFAULT NULL COMMENT '品牌英文名',
    brand_alias VARCHAR(512) DEFAULT NULL COMMENT '品牌别名',
    brand_logo_url VARCHAR(2048) DEFAULT NULL COMMENT '品牌Logo地址',
    brand_story VARCHAR(8000) DEFAULT NULL COMMENT '品牌故事',
    country_code VARCHAR(80) DEFAULT NULL COMMENT '品牌国家编码',
    country_name VARCHAR(256) DEFAULT NULL COMMENT '品牌国家名称',
    first_letter CHAR(1) DEFAULT NULL COMMENT '品牌首字母',
    brand_status TINYINT NOT NULL DEFAULT 1 COMMENT '状态:0停用 1启用',
    is_deleted TINYINT NOT NULL DEFAULT 0 COMMENT '源记录是否删除:0否 1是',
    attribute_hash CHAR(64) NOT NULL COMMENT '业务属性哈希',
    source_update_time DATETIME(6) DEFAULT NULL COMMENT '源系统更新时间',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '首次入仓时间',
    dw_update_time DATETIME(6) NOT NULL COMMENT '最近更新时间'
)
ENGINE = OLAP
UNIQUE KEY (`brand_sk`)
COMMENT '品牌Type 1一致性维度'
DISTRIBUTED BY HASH (`brand_sk`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dim_payment_type (
    payment_type_sk BIGINT NOT NULL AUTO_INCREMENT COMMENT '支付方式维度代理键',
    payment_type_code VARCHAR(128) NOT NULL COMMENT '支付方式业务编码',
    payment_type_name VARCHAR(256) NOT NULL COMMENT '支付方式名称',
    payment_institution_code VARCHAR(128) DEFAULT NULL COMMENT '支付机构编码',
    payment_institution_name VARCHAR(256) DEFAULT NULL COMMENT '支付机构名称',
    is_online TINYINT NOT NULL DEFAULT 1 COMMENT '是否线上支付:0否 1是',
    is_installment TINYINT NOT NULL DEFAULT 0 COMMENT '是否支持分期:0否 1是',
    payment_type_status TINYINT NOT NULL DEFAULT 1
        COMMENT '状态:0停用 1启用',
    is_deleted TINYINT NOT NULL DEFAULT 0 COMMENT '源记录是否删除:0否 1是',
    attribute_hash CHAR(64) NOT NULL COMMENT '业务属性哈希',
    source_update_time DATETIME(6) DEFAULT NULL COMMENT '源系统更新时间',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '首次入仓时间',
    dw_update_time DATETIME(6) NOT NULL COMMENT '最近更新时间'
)
ENGINE = OLAP
UNIQUE KEY (`payment_type_sk`)
COMMENT '支付方式Type 1一致性维度'
DISTRIBUTED BY HASH (`payment_type_sk`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dim_logistics_company (
    logistics_company_sk BIGINT NOT NULL AUTO_INCREMENT COMMENT '物流公司维度代理键',
    logistics_company_id BIGINT NOT NULL COMMENT '物流公司业务ID',
    logistics_company_code VARCHAR(128) NOT NULL COMMENT '物流公司编码',
    logistics_company_name VARCHAR(512) NOT NULL COMMENT '物流公司名称',
    logistics_type VARCHAR(128) DEFAULT NULL COMMENT '物流类型',
    service_phone VARCHAR(128) DEFAULT NULL COMMENT '客服电话',
    is_trace_supported TINYINT NOT NULL DEFAULT 1
        COMMENT '是否支持轨迹查询:0否 1是',
    logistics_company_status TINYINT NOT NULL DEFAULT 1
        COMMENT '状态:0停用 1启用',
    is_deleted TINYINT NOT NULL DEFAULT 0 COMMENT '源记录是否删除:0否 1是',
    attribute_hash CHAR(64) NOT NULL COMMENT '业务属性哈希',
    source_update_time DATETIME(6) DEFAULT NULL COMMENT '源系统更新时间',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '首次入仓时间',
    dw_update_time DATETIME(6) NOT NULL COMMENT '最近更新时间'
)
ENGINE = OLAP
UNIQUE KEY (`logistics_company_sk`)
COMMENT '物流公司Type 1一致性维度'
DISTRIBUTED BY HASH (`logistics_company_sk`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dim_warehouse_info_zip (
    warehouse_sk BIGINT NOT NULL AUTO_INCREMENT COMMENT '仓库维度代理键',
    warehouse_id BIGINT NOT NULL COMMENT '仓库业务ID',
    warehouse_code VARCHAR(128) NOT NULL COMMENT '仓库编码',
    warehouse_name VARCHAR(512) NOT NULL COMMENT '仓库名称',
    warehouse_type VARCHAR(128) NOT NULL COMMENT '仓库类型',
    owner_type VARCHAR(128) DEFAULT NULL COMMENT '仓库归属类型',
    owner_id BIGINT DEFAULT NULL COMMENT '仓库归属方业务ID',
    country_code VARCHAR(80) DEFAULT NULL COMMENT '国家编码',
    province_code VARCHAR(80) DEFAULT NULL COMMENT '省编码',
    city_code VARCHAR(80) DEFAULT NULL COMMENT '市编码',
    district_code VARCHAR(80) DEFAULT NULL COMMENT '区县编码',
    address VARCHAR(2048) DEFAULT NULL COMMENT '仓库地址脱敏值',
    warehouse_status TINYINT NOT NULL DEFAULT 1 COMMENT '状态:0停用 1启用',
    effective_start_time DATETIME(6) NOT NULL COMMENT '版本生效时间',
    effective_end_time DATETIME(6) NOT NULL COMMENT '版本失效时间',
    version_no INT NOT NULL DEFAULT 1 COMMENT '版本号',
    is_current TINYINT NOT NULL DEFAULT 1 COMMENT '是否当前版本:0否 1是',
    is_deleted TINYINT NOT NULL DEFAULT 0 COMMENT '源记录是否删除:0否 1是',
    attribute_hash CHAR(64) NOT NULL COMMENT '业务属性哈希',
    source_update_time DATETIME(6) DEFAULT NULL COMMENT '源系统更新时间',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '首次入仓时间',
    dw_update_time DATETIME(6) NOT NULL COMMENT '最近更新时间'
)
ENGINE = OLAP
UNIQUE KEY (`warehouse_sk`)
COMMENT '仓库一致性维度拉链表'
DISTRIBUTED BY HASH (`warehouse_sk`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dim_spu_info_zip (
    spu_sk BIGINT NOT NULL AUTO_INCREMENT COMMENT 'SPU维度代理键',
    spu_id BIGINT NOT NULL COMMENT 'SPU业务ID',
    spu_name VARCHAR(1024) NOT NULL COMMENT 'SPU名称',
    spu_sub_title VARCHAR(2048) DEFAULT NULL COMMENT 'SPU副标题',
    category_id BIGINT NOT NULL COMMENT '叶子类目业务ID',
    brand_id BIGINT DEFAULT NULL COMMENT '品牌业务ID',
    shop_id BIGINT NOT NULL COMMENT '店铺业务ID',
    is_virtual TINYINT NOT NULL DEFAULT 0 COMMENT '是否虚拟商品:0否 1是',
    is_presale TINYINT DEFAULT NULL COMMENT '是否预售:0否 1是',
    presale_start_time DATETIME(6) DEFAULT NULL COMMENT '预售开始时间',
    presale_end_time DATETIME(6) DEFAULT NULL COMMENT '预售结束时间',
    weight_kg DECIMAL(16, 3) DEFAULT NULL COMMENT '商品重量千克',
    volume_m3 DECIMAL(16, 6) DEFAULT NULL COMMENT '商品体积立方米',
    on_shelf_time DATETIME(6) DEFAULT NULL COMMENT '上架时间',
    spu_status VARCHAR(64) NOT NULL DEFAULT '在售' COMMENT 'SPU状态',
    effective_start_time DATETIME(6) NOT NULL COMMENT '版本生效时间',
    effective_end_time DATETIME(6) NOT NULL COMMENT '版本失效时间',
    version_no INT NOT NULL DEFAULT 1 COMMENT '版本号',
    is_current TINYINT NOT NULL DEFAULT 1 COMMENT '是否当前版本:0否 1是',
    is_deleted TINYINT NOT NULL DEFAULT 0 COMMENT '源记录是否删除:0否 1是',
    attribute_hash CHAR(64) NOT NULL COMMENT '业务属性哈希',
    source_update_time DATETIME(6) DEFAULT NULL COMMENT '源系统更新时间',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '首次入仓时间',
    dw_update_time DATETIME(6) NOT NULL COMMENT '最近更新时间'
)
ENGINE = OLAP
UNIQUE KEY (`spu_sk`)
COMMENT 'SPU一致性维度拉链表'
DISTRIBUTED BY HASH (`spu_sk`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dim_sku_info_zip (
    sku_sk BIGINT NOT NULL AUTO_INCREMENT COMMENT 'SKU维度代理键',
    sku_id BIGINT NOT NULL COMMENT 'SKU业务ID',
    sku_name VARCHAR(1024) NOT NULL COMMENT 'SKU名称',
    spu_id BIGINT NOT NULL COMMENT 'SPU业务ID',
    shop_id BIGINT NOT NULL COMMENT '店铺业务ID',
    category_id BIGINT NOT NULL COMMENT '叶子类目业务ID',
    brand_id BIGINT DEFAULT NULL COMMENT '品牌业务ID',
    bar_code VARCHAR(256) DEFAULT NULL COMMENT '商品条码',
    sku_specs_json JSON DEFAULT NULL COMMENT '低频SKU规格属性',
    unit VARCHAR(64) DEFAULT NULL COMMENT '计量单位',
    warning_stock_qty INT NOT NULL DEFAULT 0 COMMENT 'SKU级库存预警阈值',
    sku_status VARCHAR(64) NOT NULL DEFAULT '在售' COMMENT 'SKU状态',
    effective_start_time DATETIME(6) NOT NULL COMMENT '版本生效时间',
    effective_end_time DATETIME(6) NOT NULL COMMENT '版本失效时间',
    version_no INT NOT NULL DEFAULT 1 COMMENT '版本号',
    is_current TINYINT NOT NULL DEFAULT 1 COMMENT '是否当前版本:0否 1是',
    is_deleted TINYINT NOT NULL DEFAULT 0 COMMENT '源记录是否删除:0否 1是',
    attribute_hash CHAR(64) NOT NULL COMMENT '业务属性哈希',
    source_update_time DATETIME(6) DEFAULT NULL COMMENT '源系统更新时间',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '首次入仓时间',
    dw_update_time DATETIME(6) NOT NULL COMMENT '最近更新时间'
)
ENGINE = OLAP
UNIQUE KEY (`sku_sk`)
COMMENT 'SKU一致性维度拉链表'
DISTRIBUTED BY HASH (`sku_sk`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dim_promotion_rule_version (
    promotion_version_sk BIGINT NOT NULL AUTO_INCREMENT COMMENT '促销规则版本代理键',
    promotion_id BIGINT NOT NULL COMMENT '促销活动业务ID',
    rule_version_no INT NOT NULL COMMENT '促销规则业务版本号',
    promotion_name VARCHAR(1024) NOT NULL COMMENT '活动名称',
    promotion_type VARCHAR(128) NOT NULL COMMENT '活动类型',
    promotion_scene VARCHAR(128) NOT NULL COMMENT '活动场景',
    promotion_priority SMALLINT NOT NULL DEFAULT 1 COMMENT '活动优先级',
    activity_start_time DATETIME(6) NOT NULL COMMENT '活动开始时间',
    activity_end_time DATETIME(6) NOT NULL COMMENT '活动结束时间',
    rule_effective_start_time DATETIME(6) NOT NULL COMMENT '规则版本生效时间',
    rule_effective_end_time DATETIME(6) NOT NULL COMMENT '规则版本失效时间',
    rule_description VARCHAR(8000) DEFAULT NULL COMMENT '规则说明',
    threshold_amount DECIMAL(18, 2) DEFAULT NULL COMMENT '优惠门槛金额',
    discount_amount DECIMAL(18, 2) DEFAULT NULL COMMENT '固定优惠金额',
    discount_rate DECIMAL(10, 6) DEFAULT NULL COMMENT '优惠折扣率',
    max_discount_amount DECIMAL(18, 2) DEFAULT NULL COMMENT '最大优惠金额',
    sponsor_type VARCHAR(128) NOT NULL COMMENT '发起方类型',
    sponsor_business_id VARCHAR(256) DEFAULT NULL COMMENT '发起方业务ID',
    promotion_status VARCHAR(64) NOT NULL COMMENT '该规则版本发布状态',
    rule_hash CHAR(64) NOT NULL COMMENT '规则内容哈希',
    source_update_time DATETIME(6) DEFAULT NULL COMMENT '源系统更新时间',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`promotion_version_sk`)
COMMENT '促销活动不可变规则版本维度'
DISTRIBUTED BY HASH (`promotion_version_sk`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE bridge_promotion_scope (
    promotion_scope_sk BIGINT NOT NULL AUTO_INCREMENT COMMENT '活动范围关系代理键',
    promotion_version_sk BIGINT NOT NULL COMMENT '促销规则版本代理键',
    promotion_id BIGINT NOT NULL COMMENT '促销活动业务ID',
    scope_type VARCHAR(128) NOT NULL COMMENT '适用对象类型',
    scope_business_id VARCHAR(256) NOT NULL COMMENT '适用对象业务ID',
    is_excluded TINYINT NOT NULL DEFAULT 0 COMMENT '是否排除对象:0否 1是',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`promotion_scope_sk`)
COMMENT '促销规则版本适用范围桥表'
DISTRIBUTED BY HASH (`promotion_scope_sk`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dim_coupon_template_version (
    coupon_template_version_sk BIGINT NOT NULL AUTO_INCREMENT
        COMMENT '优惠券规则版本代理键',
    coupon_template_id BIGINT NOT NULL COMMENT '优惠券模板业务ID',
    rule_version_no INT NOT NULL COMMENT '优惠券规则业务版本号',
    coupon_name VARCHAR(1024) NOT NULL COMMENT '优惠券名称',
    coupon_type VARCHAR(128) NOT NULL COMMENT '优惠券类型',
    threshold_amount DECIMAL(18, 2) DEFAULT NULL COMMENT '使用门槛金额',
    discount_amount DECIMAL(18, 2) DEFAULT NULL COMMENT '固定优惠金额',
    discount_rate DECIMAL(10, 6) DEFAULT NULL COMMENT '优惠折扣率',
    max_discount_amount DECIMAL(18, 2) DEFAULT NULL COMMENT '最大优惠金额',
    issue_start_time DATETIME(6) NOT NULL COMMENT '发券开始时间',
    issue_end_time DATETIME(6) NOT NULL COMMENT '发券结束时间',
    use_start_time DATETIME(6) NOT NULL COMMENT '可用开始时间',
    use_end_time DATETIME(6) NOT NULL COMMENT '可用结束时间',
    rule_effective_start_time DATETIME(6) NOT NULL COMMENT '规则版本生效时间',
    rule_effective_end_time DATETIME(6) NOT NULL COMMENT '规则版本失效时间',
    total_issue_limit BIGINT DEFAULT NULL COMMENT '最大发行量',
    per_user_limit INT DEFAULT NULL COMMENT '单用户领取上限',
    coupon_status VARCHAR(64) NOT NULL COMMENT '该规则版本发布状态',
    rule_hash CHAR(64) NOT NULL COMMENT '规则内容哈希',
    source_update_time DATETIME(6) DEFAULT NULL COMMENT '源系统更新时间',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`coupon_template_version_sk`)
COMMENT '优惠券模板不可变规则版本维度'
DISTRIBUTED BY HASH (`coupon_template_version_sk`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE bridge_coupon_scope (
    coupon_scope_sk BIGINT NOT NULL AUTO_INCREMENT COMMENT '优惠券范围关系代理键',
    coupon_template_version_sk BIGINT NOT NULL COMMENT '优惠券规则版本代理键',
    coupon_template_id BIGINT NOT NULL COMMENT '优惠券模板业务ID',
    scope_type VARCHAR(128) NOT NULL COMMENT '适用对象类型',
    scope_business_id VARCHAR(256) NOT NULL COMMENT '适用对象业务ID',
    is_excluded TINYINT NOT NULL DEFAULT 0 COMMENT '是否排除对象:0否 1是',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`coupon_scope_sk`)
COMMENT '优惠券规则版本适用范围桥表'
DISTRIBUTED BY HASH (`coupon_scope_sk`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_product_sku_price_change_di (
    price_change_id BIGINT NOT NULL COMMENT '价格变更事件业务ID',
    event_date_key INT NOT NULL COMMENT '价格生效日期键',
    sku_sk BIGINT NOT NULL COMMENT '价格生效时SKU版本代理键',
    sku_id BIGINT NOT NULL COMMENT 'SKU业务ID',
    spu_sk BIGINT NOT NULL COMMENT '价格生效时SPU版本代理键',
    spu_id BIGINT NOT NULL COMMENT 'SPU业务ID',
    shop_sk BIGINT NOT NULL COMMENT '价格生效时店铺版本代理键',
    shop_id BIGINT NOT NULL COMMENT '店铺业务ID',
    category_sk BIGINT NOT NULL COMMENT '价格生效时类目版本代理键',
    category_id BIGINT NOT NULL COMMENT '类目业务ID',
    brand_sk BIGINT NOT NULL DEFAULT -1 COMMENT '价格生效时品牌代理键',
    brand_id BIGINT DEFAULT NULL COMMENT '品牌业务ID',
    previous_list_price DECIMAL(18, 4) DEFAULT NULL COMMENT '变更前吊牌单价',
    previous_sale_price DECIMAL(18, 4) DEFAULT NULL COMMENT '变更前销售单价',
    previous_cost_price DECIMAL(18, 4) DEFAULT NULL COMMENT '变更前标准成本单价',
    new_list_price DECIMAL(18, 4) NOT NULL COMMENT '变更后吊牌单价',
    new_sale_price DECIMAL(18, 4) NOT NULL COMMENT '变更后销售单价',
    new_cost_price DECIMAL(18, 4) DEFAULT NULL COMMENT '变更后标准成本单价',
    change_reason_code VARCHAR(128) DEFAULT NULL COMMENT '价格变更原因编码',
    change_reason_description VARCHAR(2048) DEFAULT NULL COMMENT '价格变更原因说明',
    currency_code CHAR(3) NOT NULL DEFAULT 'CNY' COMMENT '币种编码',
    price_effective_time DATETIME(6) NOT NULL COMMENT '新价格生效时间',
    change_time DATETIME(6) NOT NULL COMMENT '价格配置变更时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取新价格生效日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源事件唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`price_change_id`)
COMMENT '商品域SKU基础价格变更事件事实'
DISTRIBUTED BY HASH (`price_change_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_trade_order_detail_di (
    order_detail_id BIGINT NOT NULL COMMENT '订单明细业务ID',
    order_id BIGINT NOT NULL COMMENT '订单业务ID',
    parent_order_id BIGINT DEFAULT NULL COMMENT '父订单业务ID',
    trade_no VARCHAR(256) DEFAULT NULL COMMENT '交易流水号',
    order_no VARCHAR(256) NOT NULL COMMENT '订单编号',
    source_session_id VARCHAR(512) NOT NULL COMMENT '产生订单的来源会话ID',
    order_date_key INT NOT NULL COMMENT '下单日期键',
    user_sk BIGINT NOT NULL COMMENT '下单时用户版本代理键',
    user_id BIGINT NOT NULL COMMENT '用户业务ID',
    shop_sk BIGINT NOT NULL COMMENT '下单时店铺版本代理键',
    shop_id BIGINT NOT NULL COMMENT '店铺业务ID',
    seller_sk BIGINT NOT NULL DEFAULT -1 COMMENT '下单时商家版本代理键',
    seller_id BIGINT DEFAULT NULL COMMENT '商家业务ID',
    sku_sk BIGINT NOT NULL COMMENT '下单时SKU版本代理键',
    sku_id BIGINT NOT NULL COMMENT 'SKU业务ID',
    spu_sk BIGINT NOT NULL COMMENT '下单时SPU版本代理键',
    spu_id BIGINT NOT NULL COMMENT 'SPU业务ID',
    category_sk BIGINT NOT NULL COMMENT '下单时叶子类目版本代理键',
    category_id BIGINT NOT NULL COMMENT '叶子类目业务ID',
    brand_sk BIGINT NOT NULL DEFAULT -1 COMMENT '品牌代理键',
    brand_id BIGINT DEFAULT NULL COMMENT '品牌业务ID',
    receiver_region_sk BIGINT NOT NULL DEFAULT -1 COMMENT '收货区县版本代理键',
    receiver_region_code VARCHAR(80) DEFAULT NULL COMMENT '收货区县编码',
    channel_sk BIGINT NOT NULL DEFAULT -1 COMMENT '下单渠道代理键',
    channel_code VARCHAR(128) DEFAULT NULL COMMENT '下单渠道编码',
    order_source VARCHAR(128) DEFAULT NULL COMMENT '下单来源',
    order_scene VARCHAR(128) NOT NULL DEFAULT '普通' COMMENT '订单场景',
    is_first_order TINYINT NOT NULL DEFAULT 0 COMMENT '是否用户首单:0否 1是',
    is_cross_border TINYINT NOT NULL DEFAULT 0 COMMENT '是否跨境订单:0否 1是',
    is_presale TINYINT NOT NULL DEFAULT 0 COMMENT '是否预售订单:0否 1是',
    is_gift TINYINT NOT NULL DEFAULT 0 COMMENT '是否赠品:0否 1是',
    is_risk_order TINYINT NOT NULL DEFAULT 0 COMMENT '是否风险订单:0否 1是',
    sku_qty INT NOT NULL COMMENT '购买件数',
    sku_list_unit_price DECIMAL(18, 4) NOT NULL COMMENT '商品吊牌单价快照',
    sku_sale_unit_price DECIMAL(18, 4) NOT NULL COMMENT '商品销售单价快照',
    list_amount DECIMAL(18, 2) NOT NULL COMMENT '吊牌金额',
    sale_amount DECIMAL(18, 2) NOT NULL COMMENT '优惠前销售金额',
    activity_discount_amount DECIMAL(
            18, 2
        ) NOT NULL DEFAULT 0 COMMENT '活动优惠分摊金额',
    coupon_discount_amount DECIMAL(
            18, 2
        ) NOT NULL DEFAULT 0 COMMENT '优惠券优惠分摊金额',
    points_discount_amount DECIMAL(18, 2) NOT NULL DEFAULT 0 COMMENT '积分优惠分摊金额',
    freight_amount DECIMAL(18, 2) NOT NULL DEFAULT 0 COMMENT '运费分摊金额',
    tax_amount DECIMAL(18, 2) NOT NULL DEFAULT 0 COMMENT '税费分摊金额',
    receivable_amount DECIMAL(18, 2) NOT NULL COMMENT '下单应收金额',
    cost_amount DECIMAL(18, 2) DEFAULT NULL COMMENT '下单时标准成本金额快照',
    currency_code CHAR(3) NOT NULL DEFAULT 'CNY' COMMENT '币种编码',
    order_create_time DATETIME(6) NOT NULL COMMENT '下单时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取下单日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源记录唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`order_detail_id`)
COMMENT '交易域下单明细事务事实'
DISTRIBUTED BY HASH (`order_detail_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_trade_order_status_event_di (
    order_status_event_id BIGINT NOT NULL COMMENT '订单状态事件业务ID',
    order_id BIGINT NOT NULL COMMENT '订单业务ID',
    event_seq_no INT NOT NULL COMMENT '订单内事件序号',
    event_date_key INT NOT NULL COMMENT '事件日期键',
    user_sk BIGINT NOT NULL COMMENT '事件时点用户版本代理键',
    user_id BIGINT NOT NULL COMMENT '用户业务ID',
    shop_sk BIGINT NOT NULL COMMENT '事件时点店铺版本代理键',
    shop_id BIGINT NOT NULL COMMENT '店铺业务ID',
    before_order_status VARCHAR(128) DEFAULT NULL COMMENT '变更前订单状态',
    after_order_status VARCHAR(128) NOT NULL COMMENT '变更后订单状态',
    status_event_type VARCHAR(128) NOT NULL COMMENT '状态事件类型',
    status_reason_code VARCHAR(128) DEFAULT NULL COMMENT '状态原因编码',
    status_reason_description VARCHAR(2048) DEFAULT NULL COMMENT '状态原因说明',
    cancel_stage VARCHAR(128) DEFAULT NULL COMMENT '取消阶段',
    is_terminal_status TINYINT NOT NULL DEFAULT 0 COMMENT '是否终态:0否 1是',
    operator_id VARCHAR(256) DEFAULT NULL COMMENT '操作人业务ID',
    operator_type VARCHAR(128) DEFAULT NULL COMMENT '操作人类型',
    event_time DATETIME(6) NOT NULL COMMENT '状态变更时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取状态变更日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源事件唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`order_status_event_id`)
COMMENT '交易域订单状态迁移事件事实'
DISTRIBUTED BY HASH (`order_status_event_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_trade_order_detail_activity_di (
    order_detail_activity_id BIGINT NOT NULL COMMENT '订单活动分摊业务ID',
    order_detail_id BIGINT NOT NULL COMMENT '订单明细业务ID',
    order_id BIGINT NOT NULL COMMENT '订单业务ID',
    promotion_version_sk BIGINT NOT NULL COMMENT '下单命中的促销规则版本代理键',
    promotion_id BIGINT NOT NULL COMMENT '促销活动业务ID',
    promotion_discount_amount DECIMAL(18, 2) NOT NULL COMMENT '活动优惠分摊金额',
    rule_snapshot_json JSON DEFAULT NULL COMMENT '命中规则快照',
    currency_code CHAR(3) NOT NULL DEFAULT 'CNY' COMMENT '币种编码',
    order_create_time DATETIME(6) NOT NULL COMMENT '下单时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取下单日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源记录唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`order_detail_activity_id`)
COMMENT '交易域订单明细活动优惠分摊事实'
DISTRIBUTED BY HASH (`order_detail_activity_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_trade_order_detail_coupon_di (
    order_detail_coupon_id BIGINT NOT NULL COMMENT '订单优惠券分摊业务ID',
    order_detail_id BIGINT NOT NULL COMMENT '订单明细业务ID',
    order_id BIGINT NOT NULL COMMENT '订单业务ID',
    coupon_template_version_sk BIGINT NOT NULL COMMENT '用券命中的优惠券规则版本代理键',
    coupon_template_id BIGINT NOT NULL COMMENT '优惠券模板业务ID',
    user_coupon_id BIGINT NOT NULL COMMENT '用户优惠券实例ID',
    user_sk BIGINT NOT NULL COMMENT '用券时用户版本代理键',
    user_id BIGINT NOT NULL COMMENT '用户业务ID',
    coupon_discount_amount DECIMAL(18, 2) NOT NULL COMMENT '优惠券优惠分摊金额',
    coupon_batch_no VARCHAR(256) DEFAULT NULL COMMENT '发券批次号',
    coupon_receive_time DATETIME(6) DEFAULT NULL COMMENT '领券时间',
    coupon_use_time DATETIME(6) NOT NULL COMMENT '用券时间',
    currency_code CHAR(3) NOT NULL DEFAULT 'CNY' COMMENT '币种编码',
    order_create_time DATETIME(6) NOT NULL COMMENT '下单时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取下单日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源记录唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`order_detail_coupon_id`)
COMMENT '交易域订单明细优惠券优惠分摊事实'
DISTRIBUTED BY HASH (`order_detail_coupon_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_marketing_user_coupon_event_di (
    user_coupon_event_id BIGINT NOT NULL COMMENT '用户券事件业务ID',
    user_coupon_id BIGINT NOT NULL COMMENT '用户优惠券实例ID',
    event_seq_no INT NOT NULL COMMENT '用户券实例内事件序号',
    event_date_key INT NOT NULL COMMENT '事件日期键',
    coupon_template_version_sk BIGINT NOT NULL COMMENT '事件命中的优惠券规则版本代理键',
    coupon_template_id BIGINT NOT NULL COMMENT '优惠券模板业务ID',
    user_sk BIGINT NOT NULL COMMENT '事件时点用户版本代理键',
    user_id BIGINT NOT NULL COMMENT '用户业务ID',
    before_coupon_status VARCHAR(128) DEFAULT NULL COMMENT '变更前用户券状态',
    after_coupon_status VARCHAR(128) NOT NULL COMMENT '变更后用户券状态',
    coupon_event_type VARCHAR(128) NOT NULL COMMENT '事件类型:领取/锁定/使用/释放/过期/作废',
    related_order_id BIGINT DEFAULT NULL COMMENT '关联订单业务ID',
    coupon_batch_no VARCHAR(256) DEFAULT NULL COMMENT '发券批次号',
    event_time DATETIME(6) NOT NULL COMMENT '事件发生时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取事件发生日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源事件唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`user_coupon_event_id`)
COMMENT '营销域用户优惠券生命周期事件事实'
DISTRIBUTED BY HASH (`user_coupon_event_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_trade_pay_detail_di (
    pay_detail_id BIGINT NOT NULL COMMENT '支付尝试业务ID',
    pay_order_no VARCHAR(256) NOT NULL COMMENT '支付单号',
    pay_attempt_no INT NOT NULL COMMENT '支付单内尝试序号',
    pay_date_key INT NOT NULL COMMENT '支付请求日期键',
    user_sk BIGINT NOT NULL COMMENT '支付请求时用户版本代理键',
    user_id BIGINT NOT NULL COMMENT '用户业务ID',
    payment_type_sk BIGINT NOT NULL COMMENT '支付方式代理键',
    payment_type_code VARCHAR(128) NOT NULL COMMENT '支付方式业务编码',
    channel_sk BIGINT NOT NULL DEFAULT -1 COMMENT '支付渠道代理键',
    channel_code VARCHAR(128) DEFAULT NULL COMMENT '支付渠道编码',
    pay_scene VARCHAR(128) NOT NULL COMMENT '支付场景',
    requested_pay_amount DECIMAL(18, 2) NOT NULL COMMENT '本次请求支付金额',
    payment_fee_amount DECIMAL(18, 2) NOT NULL DEFAULT 0 COMMENT '支付手续费金额',
    installment_count INT DEFAULT NULL COMMENT '分期期数',
    currency_code CHAR(3) NOT NULL DEFAULT 'CNY' COMMENT '币种编码',
    pay_request_time DATETIME(6) NOT NULL COMMENT '支付请求时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取支付请求日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源记录唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`pay_detail_id`)
COMMENT '支付域支付尝试事务事实'
DISTRIBUTED BY HASH (`pay_detail_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_trade_pay_order_detail_di (
    pay_order_detail_id BIGINT NOT NULL COMMENT '支付分摊业务ID',
    pay_detail_id BIGINT NOT NULL COMMENT '支付尝试业务ID',
    order_id BIGINT NOT NULL COMMENT '订单业务ID',
    order_detail_id BIGINT NOT NULL COMMENT '订单明细业务ID',
    shop_sk BIGINT NOT NULL COMMENT '支付时店铺版本代理键',
    shop_id BIGINT NOT NULL COMMENT '店铺业务ID',
    seller_sk BIGINT NOT NULL DEFAULT -1 COMMENT '支付时商家版本代理键',
    seller_id BIGINT DEFAULT NULL COMMENT '商家业务ID',
    allocated_pay_amount DECIMAL(18, 2) NOT NULL COMMENT '支付分摊金额',
    currency_code CHAR(3) NOT NULL DEFAULT 'CNY' COMMENT '币种编码',
    pay_request_time DATETIME(6) NOT NULL COMMENT '支付请求时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取支付请求日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源记录唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`pay_order_detail_id`)
COMMENT '支付域支付到订单明细分摊事实'
DISTRIBUTED BY HASH (`pay_order_detail_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_trade_pay_status_event_di (
    pay_status_event_id BIGINT NOT NULL COMMENT '支付状态事件业务ID',
    pay_detail_id BIGINT NOT NULL COMMENT '支付尝试业务ID',
    pay_order_no VARCHAR(256) NOT NULL COMMENT '支付单号',
    event_seq_no INT NOT NULL COMMENT '支付尝试内事件序号',
    event_date_key INT NOT NULL COMMENT '事件日期键',
    third_party_pay_no VARCHAR(512) DEFAULT NULL COMMENT '第三方支付流水号',
    before_pay_status VARCHAR(128) DEFAULT NULL COMMENT '变更前支付状态',
    after_pay_status VARCHAR(128) NOT NULL COMMENT '变更后支付状态',
    status_reason_code VARCHAR(128) DEFAULT NULL COMMENT '状态原因编码',
    status_reason_description VARCHAR(2048) DEFAULT NULL COMMENT '状态原因说明',
    event_time DATETIME(6) NOT NULL COMMENT '状态变更时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取状态变更日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源事件唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`pay_status_event_id`)
COMMENT '支付域支付状态迁移事件事实'
DISTRIBUTED BY HASH (`pay_status_event_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_trade_delivery_di (
    delivery_id BIGINT NOT NULL COMMENT '物流包裹业务ID',
    delivery_no VARCHAR(256) NOT NULL COMMENT '物流单号',
    package_no VARCHAR(256) NOT NULL COMMENT '包裹编号',
    delivery_direction VARCHAR(64) NOT NULL COMMENT '物流方向:正向/逆向',
    delivery_date_key INT NOT NULL COMMENT '包裹创建日期键',
    order_id BIGINT NOT NULL COMMENT '订单业务ID',
    refund_no VARCHAR(256) DEFAULT NULL COMMENT '关联退款单号',
    user_sk BIGINT NOT NULL COMMENT '包裹创建时用户版本代理键',
    user_id BIGINT NOT NULL COMMENT '用户业务ID',
    shop_sk BIGINT NOT NULL COMMENT '包裹创建时店铺版本代理键',
    shop_id BIGINT NOT NULL COMMENT '店铺业务ID',
    seller_sk BIGINT NOT NULL DEFAULT -1 COMMENT '包裹创建时商家版本代理键',
    seller_id BIGINT DEFAULT NULL COMMENT '商家业务ID',
    warehouse_sk BIGINT NOT NULL COMMENT '出入库仓库版本代理键',
    warehouse_id BIGINT NOT NULL COMMENT '仓库业务ID',
    logistics_company_sk BIGINT NOT NULL DEFAULT -1 COMMENT '物流公司代理键',
    logistics_company_id BIGINT DEFAULT NULL COMMENT '物流公司业务ID',
    receiver_region_sk BIGINT NOT NULL DEFAULT -1 COMMENT '收件区县版本代理键',
    receiver_region_code VARCHAR(80) DEFAULT NULL COMMENT '收件区县编码',
    tracking_no VARCHAR(512) DEFAULT NULL COMMENT '运单号',
    delivery_type VARCHAR(128) NOT NULL COMMENT '配送类型',
    receiver_name VARCHAR(256) DEFAULT NULL COMMENT '收件人脱敏值',
    receiver_phone VARCHAR(80) DEFAULT NULL COMMENT '收件电话脱敏值',
    receiver_address VARCHAR(2048) DEFAULT NULL COMMENT '收件地址脱敏值',
    package_weight_kg DECIMAL(18, 3) NOT NULL DEFAULT 0 COMMENT '包裹重量千克',
    package_freight_amount DECIMAL(18, 2) NOT NULL DEFAULT 0 COMMENT '包裹运费金额',
    currency_code CHAR(3) NOT NULL DEFAULT 'CNY' COMMENT '币种编码',
    delivery_create_time DATETIME(6) NOT NULL COMMENT '包裹创建时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取包裹创建日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源记录唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`delivery_id`)
COMMENT '履约域物流包裹事务事实'
DISTRIBUTED BY HASH (`delivery_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_trade_delivery_item_di (
    delivery_item_id BIGINT NOT NULL COMMENT '包裹商品明细业务ID',
    delivery_id BIGINT NOT NULL COMMENT '物流包裹业务ID',
    order_id BIGINT NOT NULL COMMENT '订单业务ID',
    order_detail_id BIGINT NOT NULL COMMENT '订单明细业务ID',
    refund_detail_id BIGINT DEFAULT NULL COMMENT '退款明细业务ID',
    sku_sk BIGINT NOT NULL COMMENT '包裹创建时SKU版本代理键',
    sku_id BIGINT NOT NULL COMMENT 'SKU业务ID',
    spu_sk BIGINT NOT NULL COMMENT '包裹创建时SPU版本代理键',
    spu_id BIGINT NOT NULL COMMENT 'SPU业务ID',
    category_sk BIGINT NOT NULL COMMENT '包裹创建时类目版本代理键',
    category_id BIGINT NOT NULL COMMENT '类目业务ID',
    delivery_sku_qty INT NOT NULL COMMENT '本包裹商品件数',
    allocated_weight_kg DECIMAL(18, 3) NOT NULL DEFAULT 0 COMMENT '商品分摊重量千克',
    allocated_freight_amount DECIMAL(
            18, 2
        ) NOT NULL DEFAULT 0 COMMENT '商品分摊运费金额',
    currency_code CHAR(3) NOT NULL DEFAULT 'CNY' COMMENT '币种编码',
    delivery_create_time DATETIME(6) NOT NULL COMMENT '包裹创建时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取包裹创建日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源记录唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`delivery_item_id`)
COMMENT '履约域物流包裹商品明细事实'
DISTRIBUTED BY HASH (`delivery_item_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_trade_delivery_status_event_di (
    delivery_status_event_id BIGINT NOT NULL COMMENT '物流状态事件业务ID',
    delivery_id BIGINT NOT NULL COMMENT '物流包裹业务ID',
    event_seq_no INT NOT NULL COMMENT '包裹内事件序号',
    event_date_key INT NOT NULL COMMENT '事件日期键',
    before_delivery_status VARCHAR(128) DEFAULT NULL COMMENT '变更前物流状态',
    after_delivery_status VARCHAR(128) NOT NULL COMMENT '变更后物流状态',
    status_event_code VARCHAR(128) NOT NULL COMMENT '物流事件编码',
    event_region_sk BIGINT NOT NULL DEFAULT -1 COMMENT '事件地点区域版本代理键',
    event_region_code VARCHAR(80) DEFAULT NULL COMMENT '事件地点区域编码',
    event_location VARCHAR(1024) DEFAULT NULL COMMENT '事件地点说明',
    event_remark VARCHAR(2048) DEFAULT NULL COMMENT '物流事件说明',
    event_time DATETIME(6) NOT NULL COMMENT '状态变更时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取状态变更日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源事件唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`delivery_status_event_id`)
COMMENT '履约域物流状态迁移事件事实'
DISTRIBUTED BY HASH (`delivery_status_event_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_trade_refund_detail_di (
    refund_detail_id BIGINT NOT NULL COMMENT '退款明细业务ID',
    refund_no VARCHAR(256) NOT NULL COMMENT '退款单号',
    order_id BIGINT NOT NULL COMMENT '订单业务ID',
    order_detail_id BIGINT NOT NULL COMMENT '订单明细业务ID',
    apply_date_key INT NOT NULL COMMENT '退款申请日期键',
    user_sk BIGINT NOT NULL COMMENT '申请时用户版本代理键',
    user_id BIGINT NOT NULL COMMENT '用户业务ID',
    shop_sk BIGINT NOT NULL COMMENT '申请时店铺版本代理键',
    shop_id BIGINT NOT NULL COMMENT '店铺业务ID',
    seller_sk BIGINT NOT NULL DEFAULT -1 COMMENT '申请时商家版本代理键',
    seller_id BIGINT DEFAULT NULL COMMENT '商家业务ID',
    sku_sk BIGINT NOT NULL COMMENT '申请时SKU版本代理键',
    sku_id BIGINT NOT NULL COMMENT 'SKU业务ID',
    refund_sku_qty INT NOT NULL COMMENT '申请退款商品件数',
    refund_type VARCHAR(128) NOT NULL COMMENT '退款类型',
    refund_reason_code VARCHAR(128) DEFAULT NULL COMMENT '退款原因编码',
    refund_reason_description VARCHAR(1024) DEFAULT NULL COMMENT '退款原因说明',
    is_quality_issue TINYINT NOT NULL DEFAULT 0 COMMENT '是否质量问题:0否 1是',
    need_return_goods TINYINT NOT NULL DEFAULT 0
        COMMENT '是否需要退货:0否 1是',
    apply_goods_amount DECIMAL(18, 2) NOT NULL DEFAULT 0 COMMENT '申请退商品金额',
    apply_freight_amount DECIMAL(18, 2) NOT NULL DEFAULT 0 COMMENT '申请退运费金额',
    apply_tax_amount DECIMAL(18, 2) NOT NULL DEFAULT 0 COMMENT '申请退税金额',
    refund_apply_amount DECIMAL(18, 2) NOT NULL COMMENT '申请退款总金额',
    currency_code CHAR(3) NOT NULL DEFAULT 'CNY' COMMENT '币种编码',
    apply_time DATETIME(6) NOT NULL COMMENT '退款申请时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取退款申请日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源记录唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`refund_detail_id`)
COMMENT '退款域退款申请商品明细事实'
DISTRIBUTED BY HASH (`refund_detail_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_trade_refund_status_event_di (
    refund_status_event_id BIGINT NOT NULL COMMENT '退款状态事件业务ID',
    refund_detail_id BIGINT NOT NULL COMMENT '退款明细业务ID',
    refund_no VARCHAR(256) NOT NULL COMMENT '退款单号',
    event_seq_no INT NOT NULL COMMENT '退款明细内事件序号',
    event_date_key INT NOT NULL COMMENT '事件日期键',
    before_refund_status VARCHAR(128) DEFAULT NULL COMMENT '变更前退款状态',
    after_refund_status VARCHAR(128) NOT NULL COMMENT '变更后退款状态',
    approved_amount_delta DECIMAL(18, 2) DEFAULT NULL COMMENT '本事件新确认的审核通过金额',
    status_reason_code VARCHAR(128) DEFAULT NULL COMMENT '状态原因编码',
    status_reason_description VARCHAR(2048) DEFAULT NULL COMMENT '状态原因说明',
    operator_id VARCHAR(256) DEFAULT NULL COMMENT '操作人业务ID',
    operator_type VARCHAR(128) DEFAULT NULL COMMENT '操作人类型',
    event_time DATETIME(6) NOT NULL COMMENT '状态变更时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取状态变更日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源事件唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`refund_status_event_id`)
COMMENT '退款域退款状态迁移事件事实'
DISTRIBUTED BY HASH (`refund_status_event_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_trade_refund_pay_detail_di (
    refund_pay_detail_id BIGINT NOT NULL COMMENT '退款打款尝试业务ID',
    refund_no VARCHAR(256) NOT NULL COMMENT '退款单号',
    refund_detail_id BIGINT NOT NULL COMMENT '退款明细业务ID',
    refund_pay_attempt_no INT NOT NULL COMMENT '退款明细内打款尝试序号',
    original_pay_detail_id BIGINT DEFAULT NULL COMMENT '原支付尝试业务ID',
    order_id BIGINT NOT NULL COMMENT '订单业务ID',
    order_detail_id BIGINT NOT NULL COMMENT '订单明细业务ID',
    request_date_key INT NOT NULL COMMENT '退款打款请求日期键',
    user_sk BIGINT NOT NULL COMMENT '打款请求时用户版本代理键',
    user_id BIGINT NOT NULL COMMENT '用户业务ID',
    payment_type_sk BIGINT NOT NULL COMMENT '原支付方式代理键',
    payment_type_code VARCHAR(128) NOT NULL COMMENT '原支付方式业务编码',
    channel_sk BIGINT NOT NULL DEFAULT -1 COMMENT '退款渠道代理键',
    channel_code VARCHAR(128) DEFAULT NULL COMMENT '退款渠道编码',
    refund_goods_amount DECIMAL(18, 2) NOT NULL DEFAULT 0 COMMENT '退商品金额',
    refund_freight_amount DECIMAL(18, 2) NOT NULL DEFAULT 0 COMMENT '退运费金额',
    refund_tax_amount DECIMAL(18, 2) NOT NULL DEFAULT 0 COMMENT '退税金额',
    refund_amount DECIMAL(18, 2) NOT NULL COMMENT '退款打款总金额',
    refund_account_type VARCHAR(128) NOT NULL COMMENT '退款账户类型',
    currency_code CHAR(3) NOT NULL DEFAULT 'CNY' COMMENT '币种编码',
    refund_pay_request_time DATETIME(6) NOT NULL COMMENT '退款打款请求时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取退款打款请求日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源记录唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`refund_pay_detail_id`)
COMMENT '退款域退款打款尝试事务事实'
DISTRIBUTED BY HASH (`refund_pay_detail_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_trade_refund_pay_status_event_di (
    refund_pay_status_event_id BIGINT NOT NULL COMMENT '退款打款状态事件业务ID',
    refund_pay_detail_id BIGINT NOT NULL COMMENT '退款打款尝试业务ID',
    event_seq_no INT NOT NULL COMMENT '打款尝试内事件序号',
    event_date_key INT NOT NULL COMMENT '事件日期键',
    third_party_refund_no VARCHAR(512) DEFAULT NULL COMMENT '第三方退款流水号',
    before_refund_pay_status VARCHAR(128) DEFAULT NULL COMMENT '变更前打款状态',
    after_refund_pay_status VARCHAR(128) NOT NULL COMMENT '变更后打款状态',
    status_reason_code VARCHAR(128) DEFAULT NULL COMMENT '状态原因编码',
    status_reason_description VARCHAR(2048) DEFAULT NULL COMMENT '状态原因说明',
    event_time DATETIME(6) NOT NULL COMMENT '状态变更时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取状态变更日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源事件唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`refund_pay_status_event_id`)
COMMENT '退款域退款打款状态迁移事件事实'
DISTRIBUTED BY HASH (`refund_pay_status_event_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_interaction_cart_event_di (
    cart_event_id BIGINT NOT NULL COMMENT '购物车事件业务ID',
    event_no VARCHAR(256) NOT NULL COMMENT '事件流水号',
    event_date_key INT NOT NULL COMMENT '事件日期键',
    user_sk BIGINT NOT NULL DEFAULT -1 COMMENT '事件时点用户版本代理键',
    user_id BIGINT DEFAULT NULL COMMENT '用户业务ID，游客为空',
    device_id VARCHAR(512) NOT NULL COMMENT '设备ID',
    session_id VARCHAR(512) NOT NULL COMMENT '会话ID',
    shop_sk BIGINT NOT NULL DEFAULT -1 COMMENT '事件时点店铺版本代理键',
    shop_id BIGINT DEFAULT NULL COMMENT '店铺业务ID',
    sku_sk BIGINT NOT NULL COMMENT '事件时点SKU版本代理键',
    sku_id BIGINT NOT NULL COMMENT 'SKU业务ID',
    spu_sk BIGINT NOT NULL COMMENT '事件时点SPU版本代理键',
    spu_id BIGINT NOT NULL COMMENT 'SPU业务ID',
    category_sk BIGINT NOT NULL COMMENT '事件时点类目版本代理键',
    category_id BIGINT NOT NULL COMMENT '类目业务ID',
    channel_sk BIGINT NOT NULL DEFAULT -1 COMMENT '渠道代理键',
    channel_code VARCHAR(128) DEFAULT NULL COMMENT '渠道编码',
    cart_event_type VARCHAR(64) NOT NULL COMMENT '事件类型:加入/移除/改量/清空',
    cart_source VARCHAR(128) DEFAULT NULL COMMENT '购物车事件来源',
    sku_qty_delta INT NOT NULL COMMENT '本次商品数量变化量',
    cart_sku_qty_after INT NOT NULL COMMENT '事件后购物车商品数量',
    sku_unit_price DECIMAL(18, 4) DEFAULT NULL COMMENT '事件时点商品单价',
    currency_code CHAR(3) NOT NULL DEFAULT 'CNY' COMMENT '币种编码',
    event_time DATETIME(6) NOT NULL COMMENT '事件发生时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取事件发生日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源事件唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`cart_event_id`)
COMMENT '互动域购物车数量变更事件事实'
DISTRIBUTED BY HASH (`cart_event_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_interaction_favor_event_di (
    favor_event_id BIGINT NOT NULL COMMENT '收藏事件业务ID',
    event_no VARCHAR(256) NOT NULL COMMENT '事件流水号',
    event_date_key INT NOT NULL COMMENT '事件日期键',
    user_sk BIGINT NOT NULL COMMENT '事件时点用户版本代理键',
    user_id BIGINT NOT NULL COMMENT '用户业务ID',
    device_id VARCHAR(512) DEFAULT NULL COMMENT '设备ID',
    session_id VARCHAR(512) DEFAULT NULL COMMENT '会话ID',
    shop_sk BIGINT NOT NULL DEFAULT -1 COMMENT '事件时点店铺版本代理键',
    shop_id BIGINT DEFAULT NULL COMMENT '店铺业务ID',
    sku_sk BIGINT NOT NULL DEFAULT -1 COMMENT '事件时点SKU版本代理键',
    sku_id BIGINT DEFAULT NULL COMMENT 'SKU业务ID',
    spu_sk BIGINT NOT NULL DEFAULT -1 COMMENT '事件时点SPU版本代理键',
    spu_id BIGINT DEFAULT NULL COMMENT 'SPU业务ID',
    channel_sk BIGINT NOT NULL DEFAULT -1 COMMENT '渠道代理键',
    channel_code VARCHAR(128) DEFAULT NULL COMMENT '渠道编码',
    favor_target_type VARCHAR(64) NOT NULL COMMENT '收藏对象类型:商品/店铺',
    favor_event_type VARCHAR(64) NOT NULL COMMENT '事件类型:收藏/取消收藏',
    event_time DATETIME(6) NOT NULL COMMENT '事件发生时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取事件发生日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源事件唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`favor_event_id`)
COMMENT '互动域收藏状态变更事件事实'
DISTRIBUTED BY HASH (`favor_event_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_traffic_session_di (
    session_fact_id BIGINT NOT NULL COMMENT '会话事实业务ID',
    session_id VARCHAR(512) NOT NULL COMMENT '会话ID',
    session_date_key INT NOT NULL COMMENT '会话开始日期键',
    user_sk BIGINT NOT NULL DEFAULT -1 COMMENT '会话开始时用户版本代理键',
    user_id BIGINT DEFAULT NULL COMMENT '用户业务ID，游客为空',
    device_id VARCHAR(512) NOT NULL COMMENT '设备ID',
    channel_sk BIGINT NOT NULL DEFAULT -1 COMMENT '渠道代理键',
    channel_code VARCHAR(128) DEFAULT NULL COMMENT '渠道编码',
    entry_page_sk BIGINT NOT NULL DEFAULT -1 COMMENT '入口页面代理键',
    entry_page_id VARCHAR(256) DEFAULT NULL COMMENT '入口页面业务ID',
    exit_page_sk BIGINT NOT NULL DEFAULT -1 COMMENT '退出页面代理键',
    exit_page_id VARCHAR(256) DEFAULT NULL COMMENT '退出页面业务ID',
    region_sk BIGINT NOT NULL DEFAULT -1 COMMENT '会话区域版本代理键',
    region_code VARCHAR(80) DEFAULT NULL COMMENT '会话区域编码',
    client_type VARCHAR(128) DEFAULT NULL COMMENT '客户端类型',
    app_version VARCHAR(128) DEFAULT NULL COMMENT '应用版本',
    os_type VARCHAR(128) DEFAULT NULL COMMENT '操作系统',
    ip_masked VARCHAR(256) DEFAULT NULL COMMENT '访问IP脱敏值',
    page_view_count INT NOT NULL DEFAULT 0 COMMENT '页面访问次数',
    search_count INT NOT NULL DEFAULT 0 COMMENT '搜索次数',
    session_duration_sec INT NOT NULL DEFAULT 0 COMMENT '会话持续秒数',
    is_bounce TINYINT NOT NULL DEFAULT 0 COMMENT '是否跳出会话:0否 1是',
    session_start_time DATETIME(6) NOT NULL COMMENT '会话开始时间',
    session_end_time DATETIME(6) NOT NULL COMMENT '会话结束时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取会话开始日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源记录唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`session_fact_id`)
COMMENT '流量域客户端会话事实'
DISTRIBUTED BY HASH (`session_fact_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_traffic_page_view_di (
    page_view_id BIGINT NOT NULL COMMENT '页面访问事件业务ID',
    event_no VARCHAR(256) NOT NULL COMMENT '事件流水号',
    event_date_key INT NOT NULL COMMENT '事件日期键',
    user_sk BIGINT NOT NULL DEFAULT -1 COMMENT '事件时点用户版本代理键',
    user_id BIGINT DEFAULT NULL COMMENT '用户业务ID，游客为空',
    device_id VARCHAR(512) NOT NULL COMMENT '设备ID',
    session_id VARCHAR(512) NOT NULL COMMENT '会话ID',
    page_sk BIGINT NOT NULL COMMENT '页面代理键',
    page_id VARCHAR(256) NOT NULL COMMENT '页面业务ID',
    last_page_sk BIGINT NOT NULL DEFAULT -1 COMMENT '上一个页面代理键',
    last_page_id VARCHAR(256) DEFAULT NULL COMMENT '上一个页面业务ID',
    channel_sk BIGINT NOT NULL DEFAULT -1 COMMENT '渠道代理键',
    channel_code VARCHAR(128) DEFAULT NULL COMMENT '渠道编码',
    shop_sk BIGINT NOT NULL DEFAULT -1 COMMENT '关联店铺版本代理键',
    shop_id BIGINT DEFAULT NULL COMMENT '关联店铺业务ID',
    sku_sk BIGINT NOT NULL DEFAULT -1 COMMENT '关联SKU版本代理键',
    sku_id BIGINT DEFAULT NULL COMMENT '关联SKU业务ID',
    spu_sk BIGINT NOT NULL DEFAULT -1 COMMENT '关联SPU版本代理键',
    spu_id BIGINT DEFAULT NULL COMMENT '关联SPU业务ID',
    category_sk BIGINT NOT NULL DEFAULT -1 COMMENT '关联类目版本代理键',
    category_id BIGINT DEFAULT NULL COMMENT '关联类目业务ID',
    promotion_version_sk BIGINT NOT NULL DEFAULT -1 COMMENT '关联促销规则版本代理键',
    promotion_id BIGINT DEFAULT NULL COMMENT '关联活动业务ID',
    search_detail_id BIGINT DEFAULT NULL COMMENT '来源搜索请求业务ID',
    business_type VARCHAR(128) DEFAULT NULL COMMENT '其他关联业务对象类型',
    business_id VARCHAR(256) DEFAULT NULL COMMENT '其他关联业务对象ID',
    client_type VARCHAR(128) DEFAULT NULL COMMENT '客户端类型',
    app_version VARCHAR(128) DEFAULT NULL COMMENT '应用版本',
    os_type VARCHAR(128) DEFAULT NULL COMMENT '操作系统',
    region_sk BIGINT NOT NULL DEFAULT -1 COMMENT '访问区域版本代理键',
    region_code VARCHAR(80) DEFAULT NULL COMMENT '访问区域编码',
    stay_duration_sec INT DEFAULT NULL COMMENT '页面停留秒数',
    event_time DATETIME(6) NOT NULL COMMENT '页面加载时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取页面加载日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源事件唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`page_view_id`)
COMMENT '流量域页面访问事件事实'
DISTRIBUTED BY HASH (`page_view_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_traffic_search_di (
    search_detail_id BIGINT NOT NULL COMMENT '搜索请求业务ID',
    event_no VARCHAR(256) NOT NULL COMMENT '事件流水号',
    event_date_key INT NOT NULL COMMENT '事件日期键',
    user_sk BIGINT NOT NULL DEFAULT -1 COMMENT '事件时点用户版本代理键',
    user_id BIGINT DEFAULT NULL COMMENT '用户业务ID，游客为空',
    device_id VARCHAR(512) NOT NULL COMMENT '设备ID',
    session_id VARCHAR(512) NOT NULL COMMENT '会话ID',
    channel_sk BIGINT NOT NULL DEFAULT -1 COMMENT '渠道代理键',
    channel_code VARCHAR(128) DEFAULT NULL COMMENT '渠道编码',
    search_keyword VARCHAR(1024) NOT NULL COMMENT '搜索词',
    normalized_keyword VARCHAR(1024) DEFAULT NULL COMMENT '归一化搜索词',
    search_source VARCHAR(128) DEFAULT NULL COMMENT '搜索入口',
    result_total_count INT NOT NULL DEFAULT 0 COMMENT '搜索结果总数',
    is_no_result TINYINT NOT NULL DEFAULT 0 COMMENT '是否无结果:0否 1是',
    is_search_success TINYINT NOT NULL DEFAULT 1
        COMMENT '请求是否成功:0否 1是',
    event_time DATETIME(6) NOT NULL COMMENT '搜索请求时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取搜索请求日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源事件唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`search_detail_id`)
COMMENT '流量域搜索请求事件事实'
DISTRIBUTED BY HASH (`search_detail_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_traffic_search_click_di (
    search_click_id BIGINT NOT NULL COMMENT '搜索点击事件业务ID',
    search_detail_id BIGINT NOT NULL COMMENT '搜索请求业务ID',
    event_no VARCHAR(256) NOT NULL COMMENT '点击事件流水号',
    event_date_key INT NOT NULL COMMENT '点击日期键',
    user_sk BIGINT NOT NULL DEFAULT -1 COMMENT '点击时用户版本代理键',
    user_id BIGINT DEFAULT NULL COMMENT '用户业务ID，游客为空',
    device_id VARCHAR(512) NOT NULL COMMENT '设备ID',
    session_id VARCHAR(512) NOT NULL COMMENT '会话ID',
    channel_sk BIGINT NOT NULL DEFAULT -1 COMMENT '渠道代理键',
    channel_code VARCHAR(128) DEFAULT NULL COMMENT '渠道编码',
    click_sku_sk BIGINT NOT NULL COMMENT '点击时SKU版本代理键',
    click_sku_id BIGINT NOT NULL COMMENT '点击SKU业务ID',
    click_spu_sk BIGINT NOT NULL COMMENT '点击时SPU版本代理键',
    click_spu_id BIGINT NOT NULL COMMENT '点击SPU业务ID',
    click_shop_sk BIGINT NOT NULL COMMENT '点击时店铺版本代理键',
    click_shop_id BIGINT NOT NULL COMMENT '点击店铺业务ID',
    click_category_sk BIGINT NOT NULL COMMENT '点击时类目版本代理键',
    click_category_id BIGINT NOT NULL COMMENT '点击类目业务ID',
    click_rank INT NOT NULL COMMENT '点击结果位次',
    event_time DATETIME(6) NOT NULL COMMENT '点击时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取点击日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源事件唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`search_click_id`)
COMMENT '流量域搜索结果点击事件事实'
DISTRIBUTED BY HASH (`search_click_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_service_comment_detail_di (
    comment_detail_id BIGINT NOT NULL COMMENT '评价内容事件业务ID',
    comment_id BIGINT NOT NULL COMMENT '评价主题业务ID',
    parent_comment_detail_id BIGINT DEFAULT NULL COMMENT '关联初评内容事件ID',
    comment_type VARCHAR(64) NOT NULL COMMENT '评价类型:初评/追评',
    comment_date_key INT NOT NULL COMMENT '评价日期键',
    order_id BIGINT NOT NULL COMMENT '订单业务ID',
    order_detail_id BIGINT NOT NULL COMMENT '订单明细业务ID',
    user_sk BIGINT NOT NULL COMMENT '评价时用户版本代理键',
    user_id BIGINT NOT NULL COMMENT '用户业务ID',
    shop_sk BIGINT NOT NULL COMMENT '评价时店铺版本代理键',
    shop_id BIGINT NOT NULL COMMENT '店铺业务ID',
    sku_sk BIGINT NOT NULL COMMENT '评价时SKU版本代理键',
    sku_id BIGINT NOT NULL COMMENT 'SKU业务ID',
    spu_sk BIGINT NOT NULL COMMENT '评价时SPU版本代理键',
    spu_id BIGINT NOT NULL COMMENT 'SPU业务ID',
    category_sk BIGINT NOT NULL COMMENT '评价时类目版本代理键',
    category_id BIGINT NOT NULL COMMENT '类目业务ID',
    comment_level TINYINT DEFAULT NULL COMMENT '综合评分，追评可为空',
    is_anonymous TINYINT NOT NULL DEFAULT 0 COMMENT '是否匿名:0否 1是',
    image_count INT NOT NULL DEFAULT 0 COMMENT '图片数量',
    video_count INT NOT NULL DEFAULT 0 COMMENT '视频数量',
    comment_content VARCHAR(8000) DEFAULT NULL COMMENT '评价内容脱敏值',
    service_score TINYINT DEFAULT NULL COMMENT '服务评分',
    logistics_score TINYINT DEFAULT NULL COMMENT '物流评分',
    description_score TINYINT DEFAULT NULL COMMENT '描述评分',
    sensitive_tag VARCHAR(512) DEFAULT NULL COMMENT '敏感标签',
    sentiment VARCHAR(64) DEFAULT NULL COMMENT '情感分析结果',
    comment_time DATETIME(6) NOT NULL COMMENT '评价发布时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取评价发布日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源记录唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`comment_detail_id`)
COMMENT '服务域评价内容事件事实'
DISTRIBUTED BY HASH (`comment_detail_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_inventory_change_di (
    inventory_change_id BIGINT NOT NULL COMMENT '库存变更事件业务ID',
    change_no VARCHAR(256) NOT NULL COMMENT '库存变更流水号',
    event_date_key INT NOT NULL COMMENT '变更日期键',
    warehouse_sk BIGINT NOT NULL COMMENT '变更时仓库版本代理键',
    warehouse_id BIGINT NOT NULL COMMENT '仓库业务ID',
    sku_sk BIGINT NOT NULL COMMENT '变更时SKU版本代理键',
    sku_id BIGINT NOT NULL COMMENT 'SKU业务ID',
    spu_sk BIGINT NOT NULL COMMENT '变更时SPU版本代理键',
    spu_id BIGINT NOT NULL COMMENT 'SPU业务ID',
    shop_sk BIGINT NOT NULL COMMENT '变更时店铺版本代理键',
    shop_id BIGINT NOT NULL COMMENT '店铺业务ID',
    change_type VARCHAR(128) NOT NULL COMMENT '库存变更类型',
    biz_type VARCHAR(128) NOT NULL COMMENT '关联业务类型',
    biz_id VARCHAR(256) NOT NULL COMMENT '关联业务单据ID',
    before_on_hand_qty INT NOT NULL COMMENT '变更前在手库存',
    on_hand_qty_delta INT NOT NULL DEFAULT 0 COMMENT '在手库存变化量',
    after_on_hand_qty INT NOT NULL COMMENT '变更后在手库存',
    before_reserved_qty INT NOT NULL COMMENT '变更前预占库存',
    reserved_qty_delta INT NOT NULL DEFAULT 0 COMMENT '预占库存变化量',
    after_reserved_qty INT NOT NULL COMMENT '变更后预占库存',
    before_in_transit_qty INT NOT NULL COMMENT '变更前在途库存',
    in_transit_qty_delta INT NOT NULL DEFAULT 0 COMMENT '在途库存变化量',
    after_in_transit_qty INT NOT NULL COMMENT '变更后在途库存',
    unit_cost DECIMAL(18, 4) DEFAULT NULL COMMENT '变更时单位成本',
    total_cost_delta DECIMAL(18, 4) DEFAULT NULL COMMENT '库存成本变化金额',
    currency_code CHAR(3) NOT NULL DEFAULT 'CNY' COMMENT '币种编码',
    operator_id VARCHAR(256) DEFAULT NULL COMMENT '操作人业务ID',
    operator_type VARCHAR(128) DEFAULT NULL COMMENT '操作人类型',
    remark VARCHAR(2048) DEFAULT NULL COMMENT '变更说明',
    event_time DATETIME(6) NOT NULL COMMENT '库存变更时间',
    biz_date DATE NOT NULL COMMENT '业务日期，取库存变更日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源事件唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`inventory_change_id`)
COMMENT '库存域库存数量变更事件事实'
DISTRIBUTED BY HASH (`inventory_change_id`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);

CREATE TABLE dwd_inventory_daily_snapshot_df (
    snapshot_date_key INT NOT NULL COMMENT '快照日期键',
    warehouse_sk BIGINT NOT NULL COMMENT '快照时仓库版本代理键',
    sku_sk BIGINT NOT NULL COMMENT '快照时SKU版本代理键',
    warehouse_id BIGINT NOT NULL COMMENT '仓库业务ID',
    sku_id BIGINT NOT NULL COMMENT 'SKU业务ID',
    spu_sk BIGINT NOT NULL COMMENT '快照时SPU版本代理键',
    spu_id BIGINT NOT NULL COMMENT 'SPU业务ID',
    shop_sk BIGINT NOT NULL COMMENT '快照时店铺版本代理键',
    shop_id BIGINT NOT NULL COMMENT '店铺业务ID',
    on_hand_qty INT NOT NULL COMMENT '期末在手库存',
    reserved_qty INT NOT NULL COMMENT '期末预占库存',
    available_qty INT NOT NULL COMMENT '期末可用库存',
    in_transit_qty INT NOT NULL DEFAULT 0 COMMENT '期末在途库存',
    unit_cost DECIMAL(18, 4) DEFAULT NULL COMMENT '期末单位成本',
    inventory_cost_amount DECIMAL(18, 4) DEFAULT NULL COMMENT '期末库存成本金额',
    currency_code CHAR(3) NOT NULL DEFAULT 'CNY' COMMENT '币种编码',
    snapshot_time DATETIME(6) NOT NULL COMMENT '快照时点',
    biz_date DATE NOT NULL COMMENT '业务日期，取快照日期',
    source_record_id VARCHAR(512) NOT NULL COMMENT '源记录唯一标识',
    load_batch_id VARCHAR(256) NOT NULL COMMENT '装载批次ID',
    dw_load_time DATETIME(6) NOT NULL COMMENT '入仓时间'
)
ENGINE = OLAP
UNIQUE KEY (`snapshot_date_key`, `warehouse_sk`, `sku_sk`)
COMMENT '库存域SKU仓库每日库存周期快照事实'
DISTRIBUTED BY HASH (`snapshot_date_key`) BUCKETS AUTO
PROPERTIES (
    'replication_num' = '1'
);
