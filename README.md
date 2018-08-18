# weighing

无人职守称重系统API端／数据处理模块

````
外部依赖:
    kafka   数据的生产源
    mysql   用于储存数据
    redis   用于将没有得到回应的消息存储，以便后面再次发送
对外提供的接口:    url例子:  http://api.caradmin.com:9598/Sys/PostSys/sys_para_item
    以下接口，更改数据后，根据该条数据的 org_id  以及共享级别 去同步消息,消息均为通用消息
    
    请求方式post  接收和输出数据方式 json
    
    PostSys/org_info
        act:add     新增客户
        act:edit    修改客户
        act:del     删除客户
    PostSys/sys_para_item
        act:add     新增系统参数
        act:edit    修改系统参数
        act:del     删除系统参数
    PostSys/sys_address
        act:add     新增地址
        act:edit    修改地址
        act:del     删除地址
    PostSys/sys_material_goods
        act:add     新增物料名称
        act:edit    修改物料名称
        act:del     删除物料名称
    PostSys/sys_material
        act:add     新增物料
        act:edit    修改物料
        act:del     删除物料
    PostSys/sys_load_capacity
        act:add     新增载重
        act:edit    修改载重
        act:del     删除载重
    PostSys/org_factory
        act:add     新增厂区
        act:edit    修改厂区
        act:del     删除厂区
    PostSys/org_gate
        act:add     新增大门
        act:edit    修改大门
        act:del     删除大门
    PostSys/org_house
        act:add     新增磅房
        act:edit    修改磅房
        act:del     删除磅房
    PostSys/biz_contract
        act:add     新增合同
        act:edit    修改合同
        act:del     删除合同
        act:stop    终止合同
        act:trun    结余合同
    PostSys/org_client
        act:add     新增客户
        act:edit    修改客户
        act:del     删除客户
    PostSys/admin_user
        act:add
        act:reset   重置密码
        act:edit    修改用户
        act:lock    锁定解锁
        act:del     删除
        act:change  修改密码
    PostSys/biz_ponderation
        act:correction  纠错
        act:repair  磅单补录
        act:repair  磅单补录
        
调用外部接口: (详细请参见 概要设计)

    http://56.weighing.tuodui.com:8083/contract/getorderbycar
    
	http://56.weighing.tuodui.com:8083/contract/linkuporder
	
处理的消息类:

    1.通用消息
        处理方法: 按照表名 拼接所有字段去 add/edit/del
    
    2.服务类消息
        按照方法 去处理
    
本系统三大功能:
    1.监听Kafka处理数据业务的功能 ( 消息服务,数据处理,请求)
    2.提供修改数据接口  webserver  ( 需要同步其他局端发送kafka )
    3.数据重发服务    (发送kafka没得到回应的消息)
    
````
设计说明
======

````
申明:本系统 框架 自己编写，基础数据处理/转化均是自己编写, 不依赖于任何 非开源包
1.目录说明
    config  配置文件
        app.ini 配置文件
    controllers 处理api部分
        admin.go
        index.go
        ka.admin.go
        sys.go
    cron  定时服务
        weighing.cron.go
    ini     初始化
        app.go
    log     产生的log (sql)
        
    models  DB层数据处理
        amdin.go
        databases.go
        ka.admin.go
        redis.go
        sys.go
    queue   Kafka层类数据处理
        kafka.data.go
        kafka.optiopay.go
    routers 路由处理
        ini.go
        ouput.go
        
        
        
        
        weighing.api.go    api main
        weighing.execute.go    数据处理main
        weighing.execute.test.go   用于测试生产者消息 

````