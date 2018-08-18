package queue

import (
	"github.com/optiopay/kafka"
	"bufio"
	"os"
	"strings"
	"log"
	"github.com/optiopay/kafka/proto"
	"fmt"
	"weighing/models"

	"github.com/bitly/go-simplejson"
	"time"
	"encoding/json"
	"net/http"
	"io/ioutil"
	"net/url"
	"strconv"
)


var (
	lan_type string	//请求类型 data|service”，标识消息类型，data表示数据消息  service表示服务消息，根据输入参数返回输出参数
	lan_act string	//request表示请求消息		respond消息进行回复
	lan_scope string //业务范围
	lan_id string	//消息的标识id
	re_send string //re_send 重传次数，默认为0，表示初次发送
)
const (
	topic_weighSend = "weighSend"
	topic_weighReceive = "weighReceive"
	partition = 0
	p_max = 15
	getorderbycar = "http://56.weighing.tuodui.com:8083/contract/getorderbycar"
	getlinkuporder = "http://56.weighing.tuodui.com:8083/contract/linkuporder"
)
var kafkaAddrs = []string{"kafka.weighing.tuodui.com:9092"}	//	"192.168.1.56:9092,192.168.1.55:9092"

var broker  = &kafka.Broker{}

////初始化程序
func init(){

	//连接Kafka
	KafkaConf := kafka.NewBrokerConf("test-client")
	KafkaConf.AllowTopicCreation = true

	// connect to kafka cluster
	var err  error
	broker, err = kafka.Dial(kafkaAddrs, KafkaConf)
	if err != nil {
		log.Fatalf("KaConsume---cannot connect to kafka cluster: %s", err)
	}
	defer broker.Close()
	fmt.Println("3-5 Kafka consume >> weighReceive start 👌👌👌👌👌👌👌👌")

	//绑定生产者实例
}

// printConsumed read messages from kafka and print them out
func printConsumed(broker kafka.Client) {

	KafkaConf := kafka.NewConsumerConf(topic_weighReceive, partition)
	KafkaConf.StartOffset = kafka.StartOffsetNewest
	consumer, err := broker.Consumer(KafkaConf)
	if err != nil {
		log.Fatalf("printConsumed---cannot create kafka consumer for %s:%d: %s", topic_weighReceive, partition, err)
	}

	for {

		msg, err := consumer.Consume()

		if err != nil {
			if err != kafka.ErrNoData {
				log.Printf("printConsumed---cannot consume %q topic message: %s", topic_weighReceive, err)
			}
			break
		}

		//处理Kafka信息
		go HandKafkaMsg(msg)

	}

	log.Print("consumer quit")
}

func HandKafkaMsg(msg *proto.Message)  {
	//过滤空数据
	if len(msg.Value) <1 {
		fmt.Println(">>>>>数据为空")
		return
	}
	//判断数据是否为正确的json
	js, err:= simplejson.NewJson(msg.Value)

	if err != nil || js==nil {
		fmt.Println(">>>>>数据不正确:",string(msg.Value))
		return
	}
	//获取数据type 进行分类处理
	act_type,_ := js.Get("type").String()
	act_scope,_ := js.Get("scope").String()
	if act_type == "data" {		//对通用 data 类型数据进行处理
		code,result :=PublicData(msg.Value)
		//1 正常数据处理成功  2 正常数据处理失败  9 异常数据
		switch code {
		case 1:
			respond(result)
			fmt.Println("pubilc-数据处理完成--"+act_scope)
		case 6:
			fmt.Println("数据回复类处理完成")
		default:
			fmt.Printf("异舍")
		}
		return
	}else if act_type == "service"  {		//对  server类的单独处理
		code,result :=ActServer(msg.Value)
		//1 正常数据处理成功  2 正常数据处理失败  9 异常数据
		switch code {
		case 1:
			respond(result)
			//fmt.Println("server-数据处理完成--"+act_scope)
		case 2:
			respond(result)
			fmt.Println("数据处理完成2-1")
		default:
			//respond(result)
			fmt.Printf("异舍")
		}
		return
	}else {		//未识别除type:data|service之外的信息
		fmt.Printf("未识别除type:data|service之外的信息")
		return
	}
}

func HttpGet(my_url string) (bool){
	response,err := http.Get(my_url)
	if err!= nil {        //如果访问不成功,url不存在则会进入改判断
		log.Println(err)
		return false
	}
	defer response.Body.Close()    //请求完了关闭回复主体
	body,err := ioutil.ReadAll(response.Body)
	log.Println(string(body))
	return true
}

func httpPostForm(my_url string,car_no string,fty_address string) (bool,[]byte) {
	resp, err := http.PostForm(my_url,
		url.Values{"userName": {"admin_shipper_getorderbycar"}, "password": {"Hj1bdTBXttb$0uQr#7jXQ*Ddn$xCvD!XsFDo4H$$N^KkSM3b7zlFwP224HZt2xb*"},"car_no":{car_no},"fty_address":{fty_address}})

	if err != nil {
		// handle error
		return false,make([]byte, 0)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		// handle error
		fmt.Println("请求错误")
		return false,make([]byte, 0)
	}

	fmt.Println("fty_address---",fty_address)
	fmt.Println(">>>>>>>>>>>>>>>这里是请求物流平台返回数据>>>>>>",string(body))
	bs, err2 := simplejson.NewJson(body)

	if bs == nil {
		fmt.Println("数据为空")
		return false,make([]byte, 0)
	}

	if err2 != nil {
		fmt.Println("数据错误")
		return false,make([]byte, 0)
	}

	return true,body

}

func httpPostFormLink(my_url string,trade_order_id string,pon_no string,car_no string,act_type string,tare string,gross_weight string,net_weight string,former_net_weight string,collect_weight string,pon_url string) (bool,[]byte) {
	// getlinkuporder,trade_order_id,pon_no,act_type,tare,gross_weight,net_weight,former_net_weight,collect_weight,pon_url
	resp, err := http.PostForm(my_url,
		url.Values{"userName": {"admin_shipper_getorderbycar"}, "password": {"Hj1bdTBXttb$0uQr#7jXQ*Ddn$xCvD!XsFDo4H$$N^KkSM3b7zlFwP224HZt2xb*"},"trade_order_id":{trade_order_id},"pon_no":{pon_no},"car_no":{car_no},"act_type":{act_type},"tare":{tare},"gross_weight":{gross_weight},"net_weight":{net_weight},"former_net_weight":{former_net_weight},"collect_weight":{collect_weight},"pon_url":{pon_url}})

	fmt.Println(url.Values{})

	if err != nil {
		// handle error
		return false,make([]byte, 0)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		// handle error
		fmt.Println("请求错误")
		return false,make([]byte, 0)
	}

	fmt.Println(">>>>>>>>>>>>>>>这里是请求物流平台返回数据>>>>>>",string(body))
	bs, err2 := simplejson.NewJson(body)

	if bs == nil {
		fmt.Println("数据为空")
		return false,make([]byte, 0)
	}

	if err2 != nil {
		fmt.Println("数据错误")
		return false,make([]byte, 0)
	}

	return true,body

}

//处理 web-api 需要发送的Kafka信息
func ActWebApiSendMessage(SqlID string,scope_table string,handle string ,locID string)  {
	where :=" WHERE 1"

	//定义厂区ID
	sql_fty_id :=""

	//定义根据厂区ID查询企业ID
	//select_org_id :=""
	switch scope_table {
	case "admin_user":
		where +=" and `user_id` ="+SqlID

		//查询厂区ID
		sql_fty_id = models.SelsetSqlBySqlMax("select `team_id` a from `admin_user` where `user_id`="+SqlID)

		//定义查询企业ID半成品
		//select_org_id = "select `org_id` a from `admin_user` where `team_id`="
	case "org_factory":
		sql_fty_id = SqlID

		where +=" and `id` ="+SqlID
	default:
		where +=" and `id` ="+SqlID

		//查询厂区ID
		sql_fty_id = models.SelsetSqlBySqlMax("select `factory_id` a from `"+scope_table+"` where `id`="+SqlID)

	}

	//查询 share_flag  共享标志 0私有 1集团共享 2平台共享 org_id
	sql_share_flag := "select `share_flag` a,`org_id` b from `"+scope_table+"`"+where
	share_flag :=models.SelsetOrgLocal(sql_share_flag)



	fmt.Println("查询所属企业",sql_share_flag)
	//给局端发起者不同步
	whereLocID := ""
	if  locID !="" {
		whereLocID =" local_register_id <> "+locID+" AND"
	}

	switch share_flag[0] {
	case "0":		//0为私有  进行数据下发
		fmt.Println("0为私有  进行数据下发")
		//查询局端是否已注册
		org_id_str :=share_flag[1]
		sql_local_register_id:= "select `local_register_id` a from `sys_init` WHERE init_flag=2 and factory_id="+sql_fty_id
		local_register_id :=models.SelsetSqlBySqlOne64(sql_local_register_id)

		if local_register_id == 0 {
			fmt.Println("该局端还为注册，在这里不下发数据了",sql_local_register_id)
			return
		}
		//在这里下发数据
		kafkaMsg := make(map[string]interface{})
		entity := make(map[string]interface{})

		kafkaMsg["type"]="data"
		kafkaMsg["act"]="request"
		kafkaMsg["scope"]=scope_table
		b2 := time.Now().UnixNano()
		id := strconv.FormatInt(b2,10)
		kafkaMsg["id"]=id
		kafkaMsg["local_register_id"]=strconv.FormatInt(local_register_id,10)
		kafkaMsg["org_id"]=org_id_str
		kafkaMsg["act_time"]=time.Now().Local().Format("2006-01-02 15:04:05")
		kafkaMsg["direction"]="0"
		kafkaMsg["re_send"]="0"
		kafkaMsg["prog_type"]="1"
		kafkaMsg["prog_seq"]="1"

		entity["handle"]=handle
		entity["all_total"]="1"
		entity["total_page"]="1"
		entity["cur_page"]="1"
		entity["per_page"]="1"
		entity["cur_total"]="1"

		sql_select :="SELECT * FROM `"+scope_table+"`" +where
		entity["data"]=models.GetDataById(sql_select)
		kafkaMsg["entity"]=entity
		kafkaMsgJson, _ := json.Marshal(kafkaMsg)

		//发送Kafka消息
		AproduceStdin(string(kafkaMsgJson))
		//将数据存入 redis
		models.SettKafkaSend(id,kafkaMsgJson)

	case "1":		//企业共享 查询企业下所以厂区 并发送数据
		fmt.Println("企业共享 查询企业下所以厂区 并发送数据")
		org_id_this :=share_flag[1]
		sql_local_org := "select `local_register_id` a,`factory_id` b from sys_init where "+whereLocID+" `init_flag`=2 and `deleted`=0 and `org_id` ="+org_id_this

		resultRows,err :=models.SelectSqlToStringMap(sql_local_org)

		if err != nil {
			fmt.Println("这里是错误",err)
			return
		}

		if len(resultRows) < 1 {
			fmt.Println("该企业下厂区数量",len(resultRows))
			fmt.Println("该企业下厂区还未注册",err)
			fmt.Println("sql>>",sql_local_org)
			return
		}

		sql_select :="SELECT * FROM `"+scope_table+"`" +where
		resultData := models.GetDataById(sql_select)
		for _,v := range resultRows{
			b, _ := json.Marshal(v)
			js, _ := simplejson.NewJson(b)
			local_register_id,_ := js.Get("a").String()
			//org_id,_ := js.Get("b").String()
			org_id := org_id_this

			//根据 where 查询不同的语句
			kafkaMsg := make(map[string]interface{})
			entity := make(map[string]interface{})

			kafkaMsg["type"]="data"
			kafkaMsg["act"]="request"
			kafkaMsg["scope"]=scope_table
			b2 := time.Now().UnixNano()
			id := strconv.FormatInt(b2,10)
			kafkaMsg["id"]=id
			kafkaMsg["local_register_id"]=local_register_id
			kafkaMsg["org_id"]=org_id
			kafkaMsg["act_time"]=time.Now().Local().Format("2006-01-02 15:04:05")
			kafkaMsg["direction"]="0"
			kafkaMsg["re_send"]="0"
			kafkaMsg["prog_type"]="1"
			kafkaMsg["prog_seq"]="1"

			entity["handle"]=handle
			entity["all_total"]="1"
			entity["total_page"]="1"
			entity["cur_page"]="1"
			entity["per_page"]="1"
			entity["cur_total"]="1"

			entity["data"]=resultData

			kafkaMsg["entity"]=entity
			kafkaMsgJson, _ := json.Marshal(kafkaMsg)

			//发送Kafka消息
			AproduceStdin(string(kafkaMsgJson))
			//将数据存入 redis
			models.SettKafkaSend(id,kafkaMsgJson)
		}

	case "2":		//集团共享 查询集团下所以企业 并发送数据
		fmt.Println("集团共享 查询集团下所以企业 并发送数据")
		fmt.Println(share_flag[1],"集团共享 查询集团下所以企业 并发送数据")
		org_id_this :=share_flag[1]
		sql_local_org := "select `local_register_id` a,`org_id` b ,`factory_id` c from sys_init where "+whereLocID+" `init_flag`=2 and `deleted`=0 and `org_id` in (select `id` FROM org_info where (id="+org_id_this+" or parent_id in (select `parent_id` from `org_info` where id ="+org_id_this+" and `group_flag`=0 or `parent_id`="+org_id_this+")) and `deleted`=0)"

		resultRows,err :=models.SelectSqlToStringMap(sql_local_org)

		fmt.Println("这里是查询该集团下所以厂区",sql_local_org)
		fmt.Println("这里是查询该集团下所以厂区结果",resultRows)
		if err != nil {
			fmt.Println("这里是错误",err)
			return
		}

		if len(resultRows) < 1 {
			fmt.Println("该集团下企业数量",len(resultRows))
			fmt.Println("该集团下企业还未注册",err)
			fmt.Println("sql>>",sql_local_org)
			return
		}

		sql_select :="SELECT * FROM `"+scope_table+"`" +where
		resultData := models.GetDataById(sql_select)
		for _,v := range resultRows{
			b, _ := json.Marshal(v)
			js, _ := simplejson.NewJson(b)
			local_register_id,_ := js.Get("a").String()
			org_id,_ := js.Get("b").String()

			//根据 where 查询不同的语句
			kafkaMsg := make(map[string]interface{})
			entity := make(map[string]interface{})

			kafkaMsg["type"]="data"
			kafkaMsg["act"]="request"
			kafkaMsg["scope"]=scope_table
			b2 := time.Now().UnixNano()
			id := strconv.FormatInt(b2,10)
			kafkaMsg["id"]=id
			kafkaMsg["local_register_id"]=local_register_id
			kafkaMsg["org_id"]=org_id
			kafkaMsg["act_time"]=time.Now().Local().Format("2006-01-02 15:04:05")
			kafkaMsg["direction"]="0"
			kafkaMsg["re_send"]="0"
			kafkaMsg["prog_type"]="1"
			kafkaMsg["prog_seq"]="1"

			entity["handle"]=handle
			entity["all_total"]="1"
			entity["total_page"]="1"
			entity["cur_page"]="1"
			entity["per_page"]="1"
			entity["cur_total"]="1"

			entity["data"]=resultData

			kafkaMsg["entity"]=entity
			kafkaMsgJson, _ := json.Marshal(kafkaMsg)

			//发送Kafka消息
			AproduceStdin(string(kafkaMsgJson))
			//将数据存入 redis
			models.SettKafkaSend(id,kafkaMsgJson)
		}

	case "3":		//平台共享  下发所有局端
		fmt.Println("平台共享  下发所有局端")
		sql_local_org := "select `local_register_id` a,`org_id` b,`factory_id` c from sys_init where "+whereLocID+" `init_flag`=2 and `deleted`=0 "
		resultRows,err :=models.SelectSqlToStringMap(sql_local_org)
		if err != nil {
			fmt.Println("这里是错误",err)
			return
		}

		if len(resultRows) < 1 {
			fmt.Println("平台下企业还未注册",err)
			return
		}

		sql_select :="SELECT * FROM `"+scope_table+"`" +where
		resultData := models.GetDataById(sql_select)
		for _,v := range resultRows{
			b, _ := json.Marshal(v)
			js, _ := simplejson.NewJson(b)
			local_register_id,_ := js.Get("a").String()
			org_id,_ := js.Get("b").String()

			//根据 where 查询不同的语句
			kafkaMsg := make(map[string]interface{})
			entity := make(map[string]interface{})

			kafkaMsg["type"]="data"
			kafkaMsg["act"]="request"
			kafkaMsg["scope"]=scope_table
			b2 := time.Now().UnixNano()
			id := strconv.FormatInt(b2,10)
			kafkaMsg["id"]=id
			kafkaMsg["local_register_id"]=local_register_id
			kafkaMsg["org_id"]=org_id
			kafkaMsg["act_time"]=time.Now().Local().Format("2006-01-02 15:04:05")
			kafkaMsg["direction"]="0"
			kafkaMsg["re_send"]="0"
			kafkaMsg["prog_type"]="1"
			kafkaMsg["prog_seq"]="1"

			entity["handle"]=handle
			entity["all_total"]="1"
			entity["total_page"]="1"
			entity["cur_page"]="1"
			entity["per_page"]="1"
			entity["cur_total"]="1"

			entity["data"]=resultData
			kafkaMsg["entity"]=entity
			kafkaMsgJson, _ := json.Marshal(kafkaMsg)

			//发送Kafka消息
			AproduceStdin(string(kafkaMsgJson))
			//将数据存入 redis
			models.SettKafkaSend(id,kafkaMsgJson)
		}

	default:		//默认给自己企业下发
		//查询局端是否已注册
		fmt.Println("默认给自己企业下发")
		org_id_str :=share_flag[1]
		sql_local_register_id:= "select `local_register_id` a from `sys_init` WHERE init_flag=2 and org_id="+org_id_str
		local_register_id :=models.SelsetSqlBySqlOne64(sql_local_register_id)

		if local_register_id == 0 {
			fmt.Println("该局端还为注册，在这里不下发数据了")
			return
		}
		//在这里下发数据
		kafkaMsg := make(map[string]interface{})
		entity := make(map[string]interface{})

		kafkaMsg["type"]="data"
		kafkaMsg["act"]="request"
		kafkaMsg["scope"]=scope_table
		b2 := time.Now().UnixNano()
		id := strconv.FormatInt(b2,10)
		kafkaMsg["id"]=id
		kafkaMsg["local_register_id"]=strconv.FormatInt(local_register_id,10)
		kafkaMsg["org_id"]=org_id_str
		kafkaMsg["act_time"]=time.Now().Local().Format("2006-01-02 15:04:05")
		kafkaMsg["direction"]="0"
		kafkaMsg["re_send"]="0"
		kafkaMsg["prog_type"]="1"
		kafkaMsg["prog_seq"]="1"

		entity["handle"]=handle
		entity["all_total"]="1"
		entity["total_page"]="1"
		entity["cur_page"]="1"
		entity["per_page"]="1"
		entity["cur_total"]="1"

		sql_select :="SELECT * FROM `"+scope_table+"`" +where
		entity["data"]=models.GetDataById(sql_select)
		kafkaMsg["entity"]=entity
		kafkaMsgJson, _ := json.Marshal(kafkaMsg)

		//发送Kafka消息
		AproduceStdin(string(kafkaMsgJson))
		//将数据存入 redis
		models.SettKafkaSend(id,kafkaMsgJson)
	}
}

//处理server类信息
func ActServer(str_json []byte) (int ,string) {
	//1 正常数据，接收并处理成功
	//2 正常数据  接收处理失败
	//9 异常数据  舍弃
	js, _ := simplejson.NewJson(str_json)

	scope,_ := js.Get("scope").String()
	if js ==nil {
		fmt.Println(js)
		return 9,""
	}

	act,_ := js.Get("act").String()
	if act != "request" {
		return 9,""
	}
	//获取其他信息
	act_type,_ := js.Get("type").String()
	local_register_id,_ := js.Get("local_register_id").String()

	//根据注册号 查询厂区ID
	sql_fty_id := "0"
	if local_register_id != "0" {
		sql_fty_id = models.SelsetSqlBySqlMax("select `factory_id` a from `sys_init` where `local_register_id`="+local_register_id)
	}


	// 为data 的根据表面进行数据库更新  为 service 的进行不同处理
	line_arr_entity := make(map[string]interface{})

	switch scope {
	case "gate_validate":	//4.2.2.5	平台车辆进厂验证请求消息
		fmt.Println("车辆进场验证")

		car_no,_ := js.Get("entity").Get("car_no").String()
		factory_id,_ := js.Get("entity").Get("factory_id").String()

		//查找厂区地址
		sql_status := "select `address` a from `org_factory` where id ="+factory_id
		fty_address := models.SelsetSqlBySqlMax(sql_status)

		//在这里调用物流平台接口
		res,body :=httpPostForm(getorderbycar,car_no,fty_address)

		if res {
			//请求接口正确后进行验证
			bs, _ := simplejson.NewJson(body)
			code,_ := bs.Get("code").Int()
			message,_ := bs.Get("detailMessage").String()
			if code == 2147483647 {
				//在这里验证 地址是否正确
				unload_address,_ := bs.Get("databody").Get("unload_address").String()
				load_address,_ := bs.Get("databody").Get("load_address").String()
				ship_unit,_ := bs.Get("databody").Get("ship_unit").String()

				//查询数据库 判断 是装货还是卸货
				sql_status := "SELECT * FROM org_factory WHERE `id`="+factory_id+" AND (`address`='"+unload_address+"' or `address`='"+load_address+"' ) "
				resultRows,_ := models.SelectSqlToStringMap(sql_status)

				fmt.Println("查询是装货还是卸货::>>-----",sql_status,resultRows)
				//查询不到地址
				if len(resultRows) == 0 {
					line_arr_entity["result"]="0"
					line_arr_entity["result_msg"]="检测物流平台地址与在本系统中不存在，sql:"+sql_status
				}else {
					sql_status_unload_address := "SELECT * FROM org_factory WHERE `id`="+factory_id+" AND (`address`='"+unload_address+"') "
					sql_status_unload_address_rows,_ := models.SelectSqlToStringMap(sql_status_unload_address)

					fmt.Println("查询是装货还是卸货::>>+++++",sql_status_unload_address,sql_status_unload_address_rows)

					//在这里查询司机的相关信息
					identification_card_no,_ := bs.Get("databody").Get("identification_card_no").String()

					driver_no :=""
					traffic_qualification :=""
					sex :="1"
					birthday :=""
					address :=""
					valid_date :=""
					if identification_card_no != "" {
						sql_select_driver :="SELECT * from `biz_driver` where `identification_card_no`="+identification_card_no
						driver_info,_ := models.SelectSqlToStringMap(sql_select_driver)
						fmt.Println("查询司机信息..>>>>>",driver_info)
						fmt.Println("查询司机信息2..>>>>>",len(driver_info))
						if len(driver_info) > 0{
							b, _ := json.Marshal(driver_info[0])
							driver_info_js, _ := simplejson.NewJson(b)

							//更新重发次数
							driver_no_js,_ := driver_info_js.Get("driver_no").String()
							traffic_qualification_js,_ := driver_info_js.Get("traffic_qualification").String()
							sex_js,_ := driver_info_js.Get("sex").String()
							birthday_js,_ := driver_info_js.Get("birthday").String()
							address_js,_ := driver_info_js.Get("address").String()
							valid_date_js,_ := driver_info_js.Get("valid_date").String()

							driver_no = driver_no_js
							traffic_qualification =traffic_qualification_js
							sex =sex_js
							birthday =birthday_js
							address =address_js
							valid_date =valid_date_js

							fmt.Println("查询司机信息3..>>>>>",address)
							fmt.Println("查询司机信息4..>>>>>",birthday)
							fmt.Println("查询司机信息5..>>>>>",valid_date)
						}else {
							driver_no =""
							traffic_qualification =""
							sex =""
							birthday =""
							address =""
							valid_date =""
						}

					}else {
						driver_no =""
						traffic_qualification =""
						sex =""
						birthday =""
						address =""
						valid_date =""
					}


					if len(sql_status_unload_address_rows) >0 {
						//业务类型0装货、1卸货
						line_arr_entity["act_type"]="1"

						//磅单类型0厂内、2厂际、3进厂、4出厂
						line_arr_entity["biz_type"]="4"
					}else {
						//业务类型0装货、1卸货
						line_arr_entity["act_type"]="0"

						//磅单类型0厂内、2厂际、3进厂、4出厂
						line_arr_entity["biz_type"]="3"
					}

					//在这里 根据载重查询车轴
					car_load,_ := bs.Get("databody").Get("car_load").String()
					count :="0"
					if car_load == "" {
						count ="0"
					}else {
						sql_load := "select max(`car_axle`) a from sys_load_capacity where `capacity`<="+car_load
						count =models.SelsetSqlBySqlMax(sql_load)
					}

					//货车轴数
					line_arr_entity["axle_num"]=count
					//车里是返回结果信息

					//磅单数据表id，如果验证成功，此字段为空，需新建磅单信息。
					line_arr_entity["biz_pon_id"]=""

					//派车证号(运单号)
					trade_order_id,_ := bs.Get("databody").Get("trade_order_id").String()
					line_arr_entity["trade_order_id"]=trade_order_id

					//货品名称
					goods_name,_ := bs.Get("databody").Get("goods_name").String()
					line_arr_entity["goods_name"]=goods_name

					//货品大类名称
					material_name,_ := bs.Get("databody").Get("material_name").String()
					line_arr_entity["material_name"]=material_name

					//收货单位
					line_arr_entity["receive_unit"]=""

					//卸货地址
					line_arr_entity["unload_address"]=unload_address

					//承运单位
					line_arr_entity["ship_unit"]=ship_unit

					//发货单位
					line_arr_entity["send_unit"]=""

					//装货地址
					line_arr_entity["load_address"]=load_address

					//司机名称
					driver_name,_ := bs.Get("databody").Get("driver_name").String()
					line_arr_entity["driver_name"]=driver_name

					//驾驶证号码
					line_arr_entity["driver_no"]=driver_no

					//运输资格证号码
					line_arr_entity["traffic_qualification"]=traffic_qualification

					//司机身份证号码
					line_arr_entity["identification_card_no"]=identification_card_no

					//性别 1男、2 女
					line_arr_entity["sex"]=sex

					//出生日期
					line_arr_entity["birthday"]=birthday

					//住址
					line_arr_entity["address"]=address

					//身份证有效截至日期
					line_arr_entity["valid_date"]=valid_date

					//司机手机号码
					mobile_code,_ := bs.Get("databody").Get("mobile_code").String()
					line_arr_entity["mobile_code"]=mobile_code



					//车辆行驶证号
					driving_license_no,_ := bs.Get("databody").Get("driving_license_no").String()
					line_arr_entity["driving_license_no"]=driving_license_no

					//道路运输许可证号
					traffic_no,_ := bs.Get("databody").Get("traffic_no").String()
					line_arr_entity["traffic_no"]=traffic_no

					line_arr_entity["result"]="1"
					line_arr_entity["result_msg"]="成功"
				}

			}else {
				line_arr_entity["result"]="0"
				line_arr_entity["result_msg"]="验证失败:"+message
			}

		}else {
			line_arr_entity["result"]="0"
			line_arr_entity["result_msg"]="物流服务接口异常"
		}

	case "local_register":		//4.2.2.7	局端系统注册请求消息  返回初始化数据

		//调用初始化同步程序
		init_org_id :=models.SelsetSqlBySqlOne64("select `org_id` a from `sys_init` where `init_flag`=0 and `local_register_id`="+local_register_id)

		if init_org_id > 0 {
			fmt.Println("局端注册初始化..>>>>>")
			//要下发的表名称
			//查询归属企业
			org_id_sel :=strconv.FormatInt(init_org_id,10)
			//申请方sql 数组

			//集团共享问题
			where_group :=" AND `org_id` IN ( select id from org_info where (id='"+org_id_sel+"' or `parent_id` in (select `parent_id` from `org_info` where id='"+org_id_sel+"') ) )"
			where_company :=" AND `org_id` IN ("+org_id_sel+") "

			var p  int
			sqlarr := [p_max]string{}
			sqltable := [p_max]string{}
			sqlarrcount := [p_max]string{}

			//申请 where
			where := " WHERE 1 "
			where_public := " AND deleted=0  "

			where += where_public

			p =0
			sqltable[p] ="admin_user"	//账户
			where_admin_user :=" WHERE `org_id` in (select `org_id` from sys_init where `local_register_id`="+local_register_id+")"+where_public
			sqlarr[p]="SELECT * FROM `"+sqltable[p]+"` "+where_admin_user
			sqlarrcount[p] ="SELECT COUNT(1) a FROM `"+sqltable[p]+"` "+where_admin_user

			p = 1
			sqltable[p] ="org_info"		//公司
			where_org_info :=" WHERE `id` in (select `org_id` from sys_init where `local_register_id`="+local_register_id+")"+where_public
			sqlarr[p]="SELECT * FROM `"+sqltable[p]+"` "+where_org_info
			sqlarrcount[p] ="SELECT COUNT(1) a FROM `"+sqltable[p]+"` "+where_org_info


			p=2
			sqltable[p] ="org_factory"		//厂区
			where_org_factory :=" WHERE `org_id` in (select `org_id` from sys_init where `local_register_id`="+local_register_id+")"+where_public
			sqlarr[p]="SELECT * FROM `"+sqltable[p]+"` "+where_org_factory
			sqlarrcount[p] ="SELECT COUNT(1) a FROM `"+sqltable[p]+"` "+where_org_factory


			p = 3
			sqltable[p] ="org_house"		//磅房
			where_org_house :=" WHERE `org_id` in (select `org_id` from sys_init where `local_register_id`="+local_register_id+")"+where_public
			sqlarr[p]="SELECT * FROM `"+sqltable[p]+"` "+where_org_house
			sqlarrcount[p] ="SELECT COUNT(1) a FROM `"+sqltable[p]+"` "+where_org_house

			p = 4
			sqltable[p] ="org_gate"				//门岗
			where_org_gate :=" WHERE `org_id` in (select `org_id` from sys_init where `local_register_id`="+local_register_id+")"+where_public
			sqlarr[p]="SELECT * FROM `"+sqltable[p]+"` "+where_org_gate
			sqlarrcount[p] ="SELECT COUNT(1) a FROM `"+sqltable[p]+"` "+where_org_gate


			p = 5

			sqltable[p] ="sys_material"		//货物类别
			sqlarr[p]="SELECT * FROM `"+sqltable[p]+"` "+where + where_group
			sqlarrcount[p] ="SELECT COUNT(1) a FROM `"+sqltable[p]+"` "+where

			p = 6
			sqltable[p] ="sys_material_goods"		//货物名称
			sqlarr[p]="SELECT * FROM `"+sqltable[p]+"` "+where + where_group
			sqlarrcount[p] ="SELECT COUNT(1) a FROM `"+sqltable[p]+"` "+where

			p = 7
			sqltable[p] ="sys_load_capacity"		//车辆载重
			sqlarr[p]="SELECT * FROM `"+sqltable[p]+"` "+where
			sqlarrcount[p] ="SELECT COUNT(1) a FROM `"+sqltable[p]+"` "+where


			p = 8
			sqltable[8] ="org_client"		//客户
			//	in (select `org_id` from sys_init where `local_register_id`=1) or `org_id` in (select `parent_id` from org_info where id in (select `org_id` from sys_init where `local_register_id`=1)) or `org_id`=0
			where_org_client :=" WHERE `org_id` in (select `org_id` from sys_init where `local_register_id`="+local_register_id+") or `org_id` in (select `parent_id` from org_info where id in (select `org_id` from sys_init where `local_register_id`="+local_register_id+")) or `org_id`=0"+where_public
			sqlarr[p]="SELECT * FROM `"+sqltable[p]+"` "+where_org_client
			sqlarrcount[p] ="SELECT COUNT(1) a FROM `"+sqltable[p]+"` "+where_org_client


			p = 9
			sqltable[p] ="sys_address"		//地址
			sqlarr[p]="SELECT * FROM `"+sqltable[p]+"` "+where + where_group
			sqlarrcount[p] ="SELECT COUNT(1) a FROM `"+sqltable[p]+"` "+where


			p = 10
			sqltable[p] ="sys_para_item"		//系统参数
			sqlarr[p]="SELECT * FROM `"+sqltable[p]+"` "+where
			sqlarrcount[p] ="SELECT COUNT(1) a FROM `"+sqltable[p]+"` "+where


			p = 11
			sqltable[p] ="sys_init"		//初始化
			where_sys_init :=" WHERE `local_register_id`="+local_register_id
			sqlarr[p]="SELECT * FROM `"+sqltable[p]+"` "+where_sys_init
			sqlarrcount[p] ="SELECT COUNT(1) a FROM `"+sqltable[p]+"` "+where_sys_init


			p = 12
			sqltable[p] ="biz_car"		//车辆
			sqlarr[p]="SELECT * FROM `"+sqltable[p]+"` "+where
			sqlarrcount[p] ="SELECT COUNT(1) a FROM `"+sqltable[p]+"` "+where


			p = 13
			sqltable[p] ="biz_driver"		//司机
			sqlarr[p]="SELECT * FROM `"+sqltable[p]+"` "+where
			sqlarrcount[p] ="SELECT COUNT(1) a FROM `"+sqltable[p]+"` "+where

			p = 14
			//共享级别验证
			sqltable[p] ="biz_contract"		//合同
			sqlarr[p]="SELECT * FROM `"+sqltable[p]+"` "+where + where_company
			sqlarrcount[p] ="SELECT COUNT(1) a FROM `"+sqltable[p]+"` "+where

			//将初始化表改为已注册
			sql_update_sys_init :="UPDATE `sys_init` SET `init_flag`=2 ,`init_date`=now(),`updated`=now() where `local_register_id`="+local_register_id
			models.ExecSql(sql_update_sys_init)

			//将该企业 order_id 改为 大于100
			sql_this := "UPDATE `org_info` set `order_id`=101 where `id` in ("+org_id_sel+")"
			models.ExecSql(sql_this)
			sql_this_parent := "UPDATE `org_info` set `order_id`=101 where `parent_id` in ("+org_id_sel+") and `order_id`<100"
			models.ExecSql(sql_this_parent)

			for i,_ := range sqlarr {
				//查询总数
				//查看数据量
				count :=models.SelsetSqlBySqlCount(sqlarrcount[i])
				//fmt.Println(count,sqlarrcount[i])
				if  count >0{

					for co:=0;co <= count/50;co++ {

						count_min := co*50
						count_max := 50

						limit := " LIMIT "
						limit += strconv.Itoa(count_min)
						limit += ","
						limit += strconv.Itoa(count_max)
						//totalpage := count/100
						resultRows,_ := models.SelectSqlToStringMap(sqlarr[i]+limit)

						line_arr := make(map[string]interface{})
						line_arr_entity := make(map[string]interface{})
						//line_arr_entity_data := make(map[string]interface{})

						line_arr_entity["handle"]="add"
						line_arr_entity["all_total"]=strconv.Itoa(count)
						line_arr_entity["total_page"]=strconv.Itoa(count/50 +1)
						line_arr_entity["cur_page"]=strconv.Itoa(co+1)
						line_arr_entity["per_page"]="50"
						line_arr_entity["cur_total"]=strconv.Itoa(len(resultRows))
						line_arr_entity["data"]=resultRows

						line_arr["type"] = "data"
						line_arr["act"] = "request"
						line_arr["scope"] =sqltable[i]
						b2 := time.Now().UnixNano()
						id := strconv.FormatInt(b2,10)
						line_arr["id"] = id
						line_arr["local_register_id"] = local_register_id
						line_arr["org_id"] = org_id_sel
						line_arr["prog_type"] = "1"
						line_arr["prog_seq"] = "1"
						line_arr["direction"] = "0"
						line_arr["re_send"] = "0"
						line_arr["act_time"] = time.Now().Local().Format("2006-01-02 15:04:05")
						line_arr["entity"] = line_arr_entity

						b, _ := json.Marshal(line_arr)
						line := string(b)

						//发送同步信息
						//fmt.Println("向下发送数据",sqlarrcount[i])
						KaRequest(line)
					}
				}

			}
			//数据同步完成后  返回状态
			line_arr_entity["result"]="1"
			line_arr_entity["result_msg"]="成功"

		}else {
			//数据同步请求失败  返回状态
			line_arr_entity["result"]="0"
			line_arr_entity["result_msg"]="该注册码无效(已注册或不存在)"
		}


	case "sys_monitor":		//4.2.2.9	运行状态监控数据请求消息
		fmt.Println("局端运行监控..sys_monitor")
		//在车里入库，实现伟大结果
		SqlSet := ""
		id,_ := js.Get("id").String()
		SqlSet += " `id`='"+id+"',"

		org_id,_ := js.Get("org_id").String()
		SqlSet += " `org_id`='"+org_id+"',"

		prog_seq,_ := js.Get("prog_seq").String()
		SqlSet += " `prog_seq`='"+prog_seq+"',"

		prog_type,_ := js.Get("prog_type").String()
		SqlSet += " `prog_type`='"+prog_type+"',"

		level,_ := js.Get("entity").Get("level").String()
		SqlSet += " `level`='"+level+"',"

		run_tip,_ := js.Get("entity").Get("run_tip").String()
		SqlSet += " `run_tip`='"+run_tip+"',"

		//添加厂区
		SqlSet += " `factory_id`='"+sql_fty_id+"',"

		SqlSet += " `syn_status`=2,"
		SqlSet += " `syn_time`=now(),"
		SqlSet += " `created`=now(),"
		SqlSet += " `updated`=now()"

		SysMonitorSql := "INSERT INTO sys_monitor SET "+SqlSet

		//执行SQL
		fmt.Println("server....",SysMonitorSql)
		sql_rowsAffected,_,sql_err := models.ExecSql(SysMonitorSql)

		if sql_rowsAffected == 0 {
			line_arr_entity["result"]="0"
			if sql_err == nil {
				line_arr_entity["result_msg"]="没有更改数据,sql:<"+SysMonitorSql+">可能的原因是没有该条件的记录"
			}else {
				line_arr_entity["result_msg"]="数据库执行失败:"+models.RawValueToString(sql_err.Error())
			}
		}else {
			line_arr_entity["result"]="1"
			line_arr_entity["result_msg"]="成功"
		}


	case "biz_pon_into":	//4.2.2.11	车辆进厂请求消息
		fmt.Println("车辆进场请求消息..biz_pon_into")

		SqlSet := ""

		//数据 磅单ID
		biz_pon_id,_ := js.Get("entity").Get("biz_pon_id").String()


		into_factory,_ := js.Get("entity").Get("into_factory").String()
		SqlSet += " `into_factory`='"+into_factory+"',"

		SqlSet += " `syn_time`=now(),"
		SqlSet += " `created`=now(),"
		SqlSet += " `updated`=now()"

		SysMonitorSql := "UPDATE biz_ponderation SET "+SqlSet + " WHERE `id`= "+biz_pon_id +" AND `factory_id`="+sql_fty_id
		SysMonitorSqlBk := "UPDATE biz_ponderation_bk SET "+SqlSet + " WHERE `id`= "+biz_pon_id +" AND `factory_id`="+sql_fty_id

		//执行SQL
		fmt.Println("执行的sql:",SysMonitorSql)
		sql_rowsAffected,_,sql_err := models.ExecSql(SysMonitorSql)
		models.ExecSql(SysMonitorSqlBk)
		if sql_rowsAffected == 0 {
			line_arr_entity["result"]="0"
			if sql_err == nil {
				line_arr_entity["result_msg"]="没有更改数据,sql:<"+SysMonitorSql+">可能的原因是没有该条件的记录"
			}else {
				line_arr_entity["result_msg"]="数据库执行失败:"+models.RawValueToString(sql_err.Error())
			}

		}else {
			line_arr_entity["result"]="1"
			line_arr_entity["result_msg"]="成功"
		}


	case "biz_pon_first":		//4.2.2.13	车辆一次称重请求消息
		fmt.Println("车辆一次称重..biz_pon_first")
		//在车里入库，实现伟大结果
		SqlSet := ""
		//归属企业ID
		org_id,_ := js.Get("org_id").String()
		SqlSet += " `org_id`='"+org_id+"',"

		//数据 磅单ID
		biz_pon_id,_ := js.Get("entity").Get("biz_pon_id").String()

		tare,_ := js.Get("entity").Get("tare").String()
		tare_time,_ := js.Get("entity").Get("tare_time").String()
		tare_prog_seq,_ := js.Get("entity").Get("tare_prog_seq").String()
		tare_house_id,_ := js.Get("entity").Get("tare_house_id").String()
		gross_weight,_ := js.Get("entity").Get("gross_weight").String()
		gross_weight_time,_ := js.Get("entity").Get("gross_weight_time").String()
		gross_prog_seq,_ := js.Get("entity").Get("gross_prog_seq").String()
		gross_house_id,_ := js.Get("entity").Get("gross_house_id").String()
		net_weight,_ := js.Get("entity").Get("net_weight").String()
		status,_ := js.Get("entity").Get("status").String()

		SqlSet += " `tare`='"+tare+"',"
		SqlSet += " `tare_time`='"+tare_time+"',"
		SqlSet += " `tare_prog_seq`='"+tare_prog_seq+"',"
		SqlSet += " `tare_house_id`='"+tare_house_id+"',"
		SqlSet += " `gross_weight`='"+gross_weight+"',"
		SqlSet += " `gross_weight_time`='"+gross_weight_time+"',"
		SqlSet += " `gross_prog_seq`='"+gross_prog_seq+"',"
		SqlSet += " `gross_house_id`='"+gross_house_id+"',"
		SqlSet += " `net_weight`='"+net_weight+"',"
		status += " `net_weight`='"+status+"',"

		SqlSet += " `syn_time`=now(),"
		SqlSet += " `updated`=now()"

		SysMonitorSql := "UPDATE biz_ponderation SET "+SqlSet + " WHERE `id`= "+biz_pon_id+" AND `factory_id`="+sql_fty_id
		SysMonitorSqlBk := "UPDATE biz_ponderation_bk SET "+SqlSet + " WHERE `id`= "+biz_pon_id+" AND `factory_id`="+sql_fty_id

		//执行SQL
		sql_rowsAffected,_,sql_err := models.ExecSql(SysMonitorSql)
		models.ExecSql(SysMonitorSqlBk)
		if sql_rowsAffected == 0 {
			line_arr_entity["result"]="0"
			if sql_err == nil {
				line_arr_entity["result_msg"]="没有更改数据,sql:<"+SysMonitorSql+">可能的原因是没有该条件的记录"
			}else {
				line_arr_entity["result_msg"]="数据库执行失败:"+models.RawValueToString(sql_err.Error())
			}
		}else {
			line_arr_entity["result"]="1"
			line_arr_entity["result_msg"]="成功"
		}


	case "biz_pon_second":		//4.2.2.15	车辆二次称重请求消息
		fmt.Println("车辆二次称重..")
		//在车里入库，实现伟大结果

		SqlSet := ""
		//归属企业ID
		org_id,_ := js.Get("org_id").String()
		SqlSet += " `org_id`='"+org_id+"',"

		//数据 磅单ID
		biz_pon_id,_ := js.Get("entity").Get("biz_pon_id").String()

		tare,_ := js.Get("entity").Get("tare").String()
		tare_time,_ := js.Get("entity").Get("tare_time").String()
		tare_prog_seq,_ := js.Get("entity").Get("tare_prog_seq").String()
		tare_house_id,_ := js.Get("entity").Get("tare_house_id").String()
		gross_weight,_ := js.Get("entity").Get("gross_weight").String()
		gross_weight_time,_ := js.Get("entity").Get("gross_weight_time").String()
		gross_prog_seq,_ := js.Get("entity").Get("gross_prog_seq").String()
		gross_house_id,_ := js.Get("entity").Get("gross_house_id").String()
		net_weight,_ := js.Get("entity").Get("net_weight").String()
		status,_ := js.Get("entity").Get("status").String()

		SqlSet += " `tare`='"+tare+"',"
		SqlSet += " `tare_time`='"+tare_time+"',"
		SqlSet += " `tare_prog_seq`='"+tare_prog_seq+"',"
		SqlSet += " `tare_house_id`='"+tare_house_id+"',"
		SqlSet += " `gross_weight`='"+gross_weight+"',"
		SqlSet += " `gross_weight_time`='"+gross_weight_time+"',"
		SqlSet += " `gross_prog_seq`='"+gross_prog_seq+"',"
		SqlSet += " `gross_house_id`='"+gross_house_id+"',"
		SqlSet += " `net_weight`='"+net_weight+"',"
		SqlSet += " `status`='"+status+"',"

		SqlSet += " `syn_time`=now(),"
		SqlSet += " `updated`=now()"

		SysMonitorSql := "UPDATE biz_ponderation SET "+SqlSet + " WHERE `id`= "+biz_pon_id+" AND `factory_id`="+sql_fty_id
		SysMonitorSqlBk := "UPDATE biz_ponderation_bk SET "+SqlSet + " WHERE `id`= "+biz_pon_id+" AND `factory_id`="+sql_fty_id

		//执行SQL

		sql_rowsAffected,_,sql_err := models.ExecSql(SysMonitorSql)
		models.ExecSql(SysMonitorSqlBk)
		if sql_rowsAffected == 0 {
			line_arr_entity["result"]="0"
			if sql_err == nil {
				line_arr_entity["result_msg"]="没有更改数据,sql:<"+SysMonitorSql+">可能的原因是没有该条件的记录"
			}else {
				line_arr_entity["result_msg"]="数据库执行失败:"+models.RawValueToString(sql_err.Error())
			}
		}else {
			line_arr_entity["result"]="1"
			line_arr_entity["result_msg"]="成功"
		}

	case "biz_pon_chk":		//	4.2.2.21	车辆控量请求消息
		fmt.Println("车辆控量请求消息..")
		//在车里入库，实现伟大结果

		SqlSet := ""
		//归属企业ID
		org_id,_ := js.Get("org_id").String()
		SqlSet += " `org_id`='"+org_id+"',"

		//数据 磅单ID
		biz_pon_id,_ := js.Get("entity").Get("biz_pon_id").String()
		factory_id,_ := js.Get("entity").Get("factory_id").String()
		contract_id,_ := js.Get("entity").Get("contract_id").String()
		//业务类型0装货、1卸货,与磅单表一致
		act_type,_ := js.Get("entity").Get("act_type").String()
		net_weight,_ := js.Get("entity").Get("net_weight").String()
		deduct_flag,_ := js.Get("entity").Get("deduct_flag").String()

		SqlSet += " `tare`='"+biz_pon_id+"',"

		SqlSet += " `syn_time`=now(),"
		SqlSet += " `updated`=now()"

		//在这里检测数据
		if contract_id == "" {
			line_arr_entity["result"]="0"
			line_arr_entity["result_msg"]="合同ID为空，请检查"

		}else if net_weight=="" {
			line_arr_entity["result"]="0"
			line_arr_entity["result_msg"]="本次过磅净重为空，请检查"

		}

		//在这里查询		合同约定数量  和 已承运数量
		sql_sel_contract := "select IFNULL(`contract_amount`,0.00)+IFNULL(`before_balance`,0.00)  contract_amount_before,IFNULL(`contract_amount`,0.00) contract_amount,IFNULL(`carryout_amount`,0.00) carryout_amount,IFNULL(`before_balance`,0.00) before_balance from `biz_contract` where id="+contract_id
		//SysMonitorSql := "UPDATE biz_ponderation SET "+SqlSet + " WHERE `id`= "+biz_pon_id+" AND `factory_id`="+sql_fty_id

		//执行SQL

		sql_rowsAffected := models.SelsetSqlBySqlSliceOne(sql_sel_contract)

		b, _ := json.Marshal(sql_rowsAffected)
		contract_js, _ := simplejson.NewJson(b)

		//合同 能用的总数量  约定数量加 前期结余
		contract_amount,_ := contract_js.Get("contract_amount").String()
		before_balance,_ := contract_js.Get("before_balance").String()
		contract_amount_before,_ := contract_js.Get("contract_amount_before").String()

		//合同已运的数量
		carryout_amount,_ := contract_js.Get("carryout_amount").String()
		carryout_amount_int, _ := strconv.ParseFloat(carryout_amount, 64)
		net_weight_int, _ := strconv.ParseFloat(net_weight, 64)
		//加 本次过磅数量

		carryout_amount_net := carryout_amount_int + net_weight_int



		contract_amount_before_int, _ := strconv.ParseFloat(contract_amount_before, 64)


		//对结果 原样返回
		line_arr_entity["biz_pon_id"]=biz_pon_id
		line_arr_entity["factory_id"]=factory_id
		line_arr_entity["contract_id"]=contract_id
		line_arr_entity["act_type"]=act_type
		line_arr_entity["net_weight"]=net_weight
		line_arr_entity["deduct_flag"]=deduct_flag
		line_arr_entity["contract_amount"]=contract_amount
		line_arr_entity["before_balance"]=before_balance
		line_arr_entity["carryout_amount"]=carryout_amount

		//比较 大小
		if carryout_amount_net > contract_amount_before_int && act_type=="0" {
			//装货磅单超量
			line_arr_entity["result"]="0"
			line_arr_entity["result_msg"]="超量"
		}else {
			//卸货磅单 超量 则更改合同状态为完成  继续加
			if carryout_amount_net > contract_amount_before_int {

				sel_status := "select `status` as a from biz_contract  where id="+contract_id
				cont_status :=models.SelsetSqlBySqlMax(sel_status)
				if cont_status =="0" {
					sql_update_contract_status :="update `biz_contract` set `status`=2 where id="+contract_id
					models.ExecSql(sql_update_contract_status)
				}
			}
			if net_weight == "0.00" {
				line_arr_entity["result"]="1"
				line_arr_entity["result_msg"]="成功"
				fmt.Println("过磅为0")
			}else {

				if deduct_flag =="1" {	//扣减标志，1扣减、，0不扣减
					//对需要扣减的  先就行扣减

					carryout_amount_net_str := strconv.FormatFloat(carryout_amount_net, 'f', -1, 64)
					sql_update_contract :="update `biz_contract` set `carryout_amount`="+carryout_amount_net_str+" where id="+contract_id


					sql_rowsAffected,_,sql_err := models.ExecSql(sql_update_contract)
					if sql_rowsAffected == 0 {
						line_arr_entity["result"]="0"
						if sql_err == nil {
							line_arr_entity["result_msg"]="没有更改数据,sql:<"+sql_update_contract+">可能的原因是没有该条件的记录"
						}else {
							line_arr_entity["result_msg"]="数据库执行失败:"+models.RawValueToString(sql_err.Error())
						}
					}else {
						//下发合同
						ActWebApiSendMessage(contract_id ,"biz_contract" ,"update" ,"")

						line_arr_entity["result"]="1"
						line_arr_entity["result_msg"]="成功"
					}


				}else {//对不扣减 的直接过
					line_arr_entity["result"]="1"
					line_arr_entity["result_msg"]="成功"
				}

			}


		}


	case "biz_pon_linkup":		//	4.2.2.21	4.2.2.23	车辆运单通知请求消息
		fmt.Println("车辆控量请求消息..")
		//在车里入库，实现伟大结果

		SqlSet := ""
		//归属企业ID
		org_id,_ := js.Get("org_id").String()
		SqlSet += " `org_id`='"+org_id+"',"

		//数据 磅单ID
		//factory_id,_ := js.Get("entity").Get("factory_id").String()
		//gate_id,_ := js.Get("entity").Get("gate_id").String()
		//pon_id,_ := js.Get("entity").Get("biz_pon_id").String()
		pon_no,_ := js.Get("entity").Get("pon_no").String()
		trade_order_id,_ := js.Get("entity").Get("trade_order_id").String()
		car_no,_ := js.Get("entity").Get("car_no").String()
		act_type,_ := js.Get("entity").Get("act_type").String()
		tare,_ := js.Get("entity").Get("tare").String()
		gross_weight,_ := js.Get("entity").Get("gross_weight").String()
		net_weight,_ := js.Get("entity").Get("net_weight").String()
		former_net_weight,_ := js.Get("entity").Get("former_net_weight").String()
		collect_weight,_ := js.Get("entity").Get("collect_weight").String()
		//pon_file,_ := js.Get("entity").Get("pon_file").String()

		pon_url :="0"




		//在这里调用物流平台接口
		/**
		trade_order_id	运单号(派车证号)
		pon_no	磅单码
		act_type	业务类型0装货、1卸货
		tare	磅单皮重
		gross_weight	磅单毛重
		net_weight	磅单净重
		former_net_weight	原发净重
		collect_weight	磅单实收
		pon_url	默认“0”
		*/
		res,body :=httpPostFormLink(getlinkuporder,trade_order_id,pon_no,car_no,act_type,tare,gross_weight,net_weight,former_net_weight,collect_weight,pon_url)

		if res {
			//请求接口正确后进行验证
			bs, _ := simplejson.NewJson(body)
			code,_ := bs.Get("code").Int()
			message,_ := bs.Get("detailMessage").String()
			if code == 2147483647 {
				//在这里验证 地址是否正确

				//对结果 原样返回
				line_arr_entity["pon_no"]=pon_no
				line_arr_entity["trade_order_id"]=trade_order_id
				line_arr_entity["car_no"]=car_no
				line_arr_entity["act_type"]=act_type

				line_arr_entity["result"]="1"
				line_arr_entity["result_msg"]="成功"


			}else {
				line_arr_entity["result"]="0"
				line_arr_entity["result_msg"]="验证失败:"+message
			}

		}else {
			line_arr_entity["result"]="0"
			line_arr_entity["result_msg"]="物流服务接口异常,请联系彭继兵去解决"
		}


	case "biz_pon_receive":		//4.2.2.17	收货确认请求消息

		rfid,_ := js.Get("entity").Get("rfid").String()
		receive_affirm,_ := js.Get("entity").Get("receive_affirm").String()
		receive_time,_ := js.Get("entity").Get("receive_time").String()
		receive_man,_ := js.Get("entity").Get("receive_man").String()

		resp,err := http.Get("http://api-huole.51huole.cn/Login/index?phone=186"+rfid+receive_affirm+receive_time+receive_man+"&check_registered=1")
		if err != nil {
			// handle error
			//fmt.Println("2这里是发送失败....")
		}
		defer resp.Body.Close()
		//body, _ := ioutil.ReadAll(resp.Body)
		//fmt.Println(string(body))

		line_arr_entity["result"]="0"
		line_arr_entity["result_msg"]="预留中"

	case "biz_pon_out":		//4.2.2.19	车辆离厂请求消息
		fmt.Println("车辆离场请求消息..")
		//在车里入库，实现伟大结果

		SqlSet := ""
		//数据库ID
		id,_ := js.Get("id").String()
		SqlSet += " `id`='"+id+"',"

		//归属企业ID
		org_id,_ := js.Get("org_id").String()
		SqlSet += " `org_id`='"+org_id+"',"

		//程序类型
		prog_seq,_ := js.Get("prog_seq").String()
		SqlSet += " `prog_seq`='"+prog_seq+"',"

		//程序编号
		prog_type,_ := js.Get("prog_type").String()
		SqlSet += " `prog_type`='"+prog_type+"',"

		//数据 磅单ID
		biz_pon_id,_ := js.Get("entity").Get("biz_pon_id").String()


		out_factory,_ := js.Get("entity").Get("out_factory").String()
		SqlSet += " `out_factory`='"+out_factory+"',"

		SqlSet += " `syn_time`=now(),"
		SqlSet += " `updated`=now()"

		SysMonitorSql := "UPDATE biz_ponderation SET "+SqlSet + " WHERE `id`= "+biz_pon_id+" AND `factory_id`="+sql_fty_id
		SysMonitorSqlBk := "UPDATE biz_ponderation_bk SET "+SqlSet + " WHERE `id`= "+biz_pon_id+" AND `factory_id`="+sql_fty_id

		//执行SQL
		sql_rowsAffected,_,sql_err := models.ExecSql(SysMonitorSql)
		models.ExecSql(SysMonitorSqlBk)
		if sql_rowsAffected == 0 {
			line_arr_entity["result"]="0"
			if sql_err == nil {
				line_arr_entity["result_msg"]="没有更改数据,sql:<"+SysMonitorSql+">可能的原因是没有该条件的记录"
			}else {
				line_arr_entity["result_msg"]="数据库执行失败:"+models.RawValueToString(sql_err.Error())
			}
		}else {
			line_arr_entity["result"]="1"
			line_arr_entity["result_msg"]="成功"
		}

	case "biz_photo_save":		//4.2.2.21	抓拍照片保存请求消息

		line_arr_entity["result"]="0"
		line_arr_entity["result_msg"]="暂未开发"

	default:					//对于未识别的方案就行回复
		line_arr_entity["result"]="0"
		line_arr_entity["result_msg"]="错误，该方案未开发"
	}

	act_id,_ := js.Get("id").String()
	act_org_id,_ := js.Get("org_id").String()
	direction,_ := js.Get("direction").String()
	re_send,_ := js.Get("re_send").String()
	prog_type,_ := js.Get("prog_type").String()
	prog_seq,_ := js.Get("prog_seq").String()

	line_arr := make(map[string]interface{})
	//消息回复头
	line_arr["local_register_id"]=local_register_id
	line_arr["type"]=act_type
	line_arr["act"]="respond"
	line_arr["scope"]=scope
	line_arr["id"]=act_id
	line_arr["org_id"]=act_org_id
	line_arr["direction"]=direction
	line_arr["re_send"]=re_send
	line_arr["prog_type"]=prog_type
	line_arr["prog_seq"]=prog_seq
	line_arr["act_time"]=time.Now().Local().Format("2006-01-02 15:04:05")
	//消息回复体
	line_arr["entity"]=line_arr_entity

	b, _ := json.Marshal(line_arr)
	line := string(b)
	return 1,line

}

func KaRequest(line string)  {
	//KafkaConf := kafka.NewBrokerConf("test-client")
	//KafkaConf.AllowTopicCreation = true
	//
	//// connect to kafka cluster
	//
	//broker, err := kafka.Dial(kafkaAddrs, KafkaConf)
	//if err != nil {
	//	log.Fatalf("KaRequest---cannot connect to kafka cluster: %s", err)
	//	//尝试重新连接
	//
	//}
	//defer broker.Close()
	producer := broker.Producer(kafka.NewProducerConf())

	//line := `{
	//"type":"data",
	//"act":"respond",
	//"scope":"org_info",
	//"id":"14880505866497",
	//"local_register_id":"1",
	//"org_id":"13280505866498",
	//"direction":"0",
	//"re_send":"0",
	//"act_time":"2016-12-30 19:30:36",
	//"entity":{"result":"1","result_msg":"成功"}
	//}`

	msg := &proto.Message{Value: []byte(line)}
	if _, err := producer.Produce(topic_weighSend, partition, msg); err != nil {
		log.Fatalf("KaRequest---cannot produce message to %s:%d: %s", topic_weighSend, partition, err)
	}

	//fmt.Println(line)

}


func respond(line string)  {
	//KafkaConf := kafka.NewBrokerConf("test-client")
	//KafkaConf.AllowTopicCreation = true
	//
	//// connect to kafka cluster
	//broker, err := kafka.Dial(kafkaAddrs, KafkaConf)
	//if err != nil {
	//	log.Fatalf("respond---cannot connect to kafka cluster: %s", err)
	//}
	//
	//defer broker.Close()
	//
	producer := broker.Producer(kafka.NewProducerConf())

		//line := `{
    //"type":"data",
    //"act":"respond",
    //"scope":"org_info",
    //"id":"14880505866497",
    //"local_register_id":"1",
    //"org_id":"13280505866498",
    //"direction":"0",
    //"re_send":"0",
    //"act_time":"2016-12-30 19:30:36",
    //"entity":{"result":"1","result_msg":"成功"}
    //}`

		msg := &proto.Message{Value: []byte(line)}
		if _, err := producer.Produce(topic_weighSend, partition, msg); err != nil {
			log.Fatalf("respond---cannot produce message to %s:%d: %s", topic_weighSend, partition, err)
		}

		//fmt.Println(line)

}

// produceStdin read stdin and send every non empty line as message
func produceStdin(broker kafka.Client) {
	producer := broker.Producer(kafka.NewProducerConf())
	input := bufio.NewReader(os.Stdin)

	for {
		line, err := input.ReadString('\n')
		if err != nil {
			log.Fatalf("input error: %s", err)
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		//line = `{"type":"service","act":"request","scope":"local_register","id":"14880505866497","local_register_id":"1","org_id":"13280505866498","direction":"0","re_send":"0","act_time":"2016-12-30 19:30:36","entity":{ "handle":"add","all_total":"100","total_page":"10","cur_ page":"3","per_page":"10","cur_ total":"10","data":[{"id":"13280505866498","parent_id":"13200505866498","full_name":"美锦集团煤化工XXXX","short_name":"美锦煤化工XXXX","group_flag":"0","local_register_id": "11112","syn_flag":"1","syn_status":"1","syn_time":"2016-12-30 19:30:37","share_flag":"0","user_id":"11112","org_id":"111121121212","order_id":"1","deleted":"1","updated":"2016-12-30 19:30:37","created":"2016-11-30 19:30:36"},{"id":"13280505866497","parent_id":"13200505866498","full_name":"美锦集团煤化工XXXX","short_name":"美锦煤化工XXXX","group_flag":"0","local_register_id":"11112","syn_flag":"1","syn_status":"1","syn_time":"2016-12-30 19:30:37","share_flag":"0","user_id":"11112","org_id":"111121121212","order_id":"1","deleted":"1","updated":"2016-12-30 19:30:37","created":"2016-11-30 19:30:36"}]}}`
		//line = `{"type":"service","act":"request","scope":"gate_validate","id":"14880505866497","local_register_id":"0","org_id":"13280505866498","direction":"0","re_send":"0","act_time":"2016-12-30 19:30:36","prog_type":"0","prog_seq":"1","entity":{"biz_pon_id":"16880505811498","first_weight":"36.58","first_time":"2016-12-30 19:30:36","house_id":"1121231313"}}`
		line = `{"type":"service","act":"request","scope":"local_register","id":"14880505866497","local_register_id":"1","org_id":"13280505866498","direction":"0","re_send":"0","act_time":"2016-12-30 19:30:36","prog_type":"0","prog_seq":"1","entity":{"rfid":"E20034120138170000","rfid_type":"0","car_no":"冀C82900","factory_id":"1503995713793521647","gate_id":"1504073835629073954"}}`

		msg := &proto.Message{Value: []byte(line)}
		if _, err := producer.Produce(topic_weighReceive, partition, msg); err != nil {
			log.Fatalf("produceStdin---cannot produce message to %s:%d: %s", topic_weighReceive, partition, err)
		}

		fmt.Println(line)
	}
}

func produceStdinTest(broker kafka.Client) {
	producer := broker.Producer(kafka.NewProducerConf())

	for i:=0;i<1;i++ {
		//line := `{"type":"service","act":"request","scope":"local_register","id":"14880505866497","local_register_id":"1","org_id":"13280505866498","direction":"0","re_send":"0","act_time":"2016-12-30 19:30:36","entity":{ "handle":"add","all_total":"100","total_page":"10","cur_ page":"3","per_page":"10","cur_ total":"10","data":[{"id":"13280505866498","parent_id":"13200505866498","full_name":"美锦集团煤化工XXXX","short_name":"美锦煤化工XXXX","group_flag":"0","local_register_id": "11112","syn_flag":"1","syn_status":"1","syn_time":"2016-12-30 19:30:37","share_flag":"0","user_id":"11112","org_id":"111121121212","order_id":"1","deleted":"1","updated":"2016-12-30 19:30:37","created":"2016-11-30 19:30:36"},{"id":"13280505866497","parent_id":"13200505866498","full_name":"美锦集团煤化工XXXX","short_name":"美锦煤化工XXXX","group_flag":"0","local_register_id":"11112","syn_flag":"1","syn_status":"1","syn_time":"2016-12-30 19:30:37","share_flag":"0","user_id":"11112","org_id":"111121121212","order_id":"1","deleted":"1","updated":"2016-12-30 19:30:37","created":"2016-11-30 19:30:36"}]}}`
		//line = `{"type":"service","act":"request","scope":"gate_validate","id":"14880505866497","local_register_id":"0","org_id":"13280505866498","direction":"0","re_send":"0","act_time":"2016-12-30 19:30:36","prog_type":"0","prog_seq":"1","entity":{"biz_pon_id":"16880505811498","first_weight":"36.58","first_time":"2016-12-30 19:30:36","house_id":"1121231313"}}`

		//for a:=0;a<1;a++ {
			b2 := time.Now().UnixNano()
			ID := strconv.FormatInt(b2,10)
			//line := `{"act":"request","act_time":"2017-09-14 15:15:59","direction":"1","entity":{"level":"0","run_tip":"clouds transfer service startup!"},"id":"`+ID+`","local_register_id":"5","org_id":"1508232880750876155","prog_seq":"9","prog_type":"9","re_send":"0","scope":"sys_monitor","type":"service"}`
			//line := `{"act":"request","act_time":"2017-09-21 15-39-41","direction":"1","entity":{"handle":"add","all_total":"1","total_page":"1","cur_page":"1","per_page":"1","cur_total":"1","data":[{"id":"150","rfid":"E20034120138170001","rfid_type":"1","car_no":"内K88222","axle_num":"12","driving_license_no":"1","traffic_no":"1","car_type":"1","use_type":"1","oil_type":"1","vin_no":"1","agent_name":"1","agent_id_no":"1","share_flag":"2","syn_flag":"2","syn_status":"0","syn_time":"2017-10-15 17:13:25","user_id":"1","org_id":"1504167179890262543","valid_flag":"1","deleted":"0","updated":"2017-10-15 17:13:25","created":"2017-10-15 17:13:25"}]},"id":"`+ID+`","local_register_id":"2","org_id":"1504167286925938300","prog_seq":"1","prog_type":"4","re_send":"0","scope":"biz_car","type":"data"}`
			//line := `{"act":"request","act_time":"2018-06-08 17:51:57","direction":"1","entity":{"act_type":"1","biz_pon_id":"15284460873501197","contract_id":"1528374898743835429","deduct_flag":"1","factory_id":"1526104023803576154","gate_id":"1526107002967510510","net_weight":"34.00"},"id":"1528451518","local_register_id":"1","org_id":"1520941865695195446","prog_seq":"01","prog_type":"4","re_send":"0","scope":"biz_pon_chk","type":"service"}`
			//line := `{"type":"service","act":"request","scope":"gate_validate","id":"15286302428448224","local_register_id":"1","org_id":"1520941865695195446","direction":"1","re_send":"0","act_time":"2018-06-10 19:30:42","prog_type":"4","prog_seq":"01","entity":{"rfid":"013902000033B224","rfid_type":"Platform","car_no":"京C32786","factory_id":"1526104023803576154","gate_id":"1526107002967510510"}}`

			//局端监控
			//line := `{"act":"request","act_time":"2018-06-28 11:49:28","direction":"1","entity":{"level":"0","run_tip":"clouds transfer service is running!"},"id":"12021188755969","local_register_id":"22","org_id":"0","prog_seq":"1","prog_type":"2","re_send":"0","scope":"sys_monitor","type":"service"}`
			//line := `{"act":"request","act_time":"2018-07-13 20:23:27","direction":"1","entity":{"level":"1","run_tip":"入厂射频正常， 出厂射频正常， IO采集卡正常， 视频监控正常， 文件同步Socket正常"},"id":"15314846078201886","local_register_id":"1","org_id":"1528017792494228982","prog_seq":"01","prog_type":"5","re_send":"0","scope":"sys_monitor","type":"service"}`

			//平台运单通知消息

			//line := `{"act":"request","act_time":"2018-07-13 20:23:27","direction":"1","entity":{"factory_id":"16880505811498","gate_id":"14880505866491","biz_pon_id":"16880505811498","pon_no":"MJ01010505811498","trade_order_id":"16880505811498","car_no":"晋B1Q119","act_type":"1","tare":"15.00","gross_weight":"49.00","net_weight":"34.00","former_net_weight":"34.00","collect_weight":"34.00","pon_file":"0"},"id":"15314846078201886","local_register_id":"1","org_id":"1528017792494228982","prog_seq":"01","prog_type":"5","re_send":"0","scope":"biz_pon_linkup","type":"service"}`


			line := `{"act":"request","act_time":"2017-09-21 15-39-41","direction":"1","entity":{"init_flag":"0"},"id":"`+ID+`","local_register_id":"7","org_id":"1532691074181469690","prog_seq":"1","prog_type":"4","re_send":"0","scope":"local_register","type":"service"}`

			//车辆局端向上同步 并 同步到其他局端
			//line := `{"act":"request","act_time":"2018-07-31 19:59:30","direction":"1","entity":{"all_total":"1","cur_page":"1","cur_total":"1","data":[{"agent_id_no":"211324198904171616","agent_name":"李扬","axle_num":"19","car_no":"京C32787","car_type":"1","created":"2018-07-31 19:59:30","deleted":"0","driving_license_no":"1234567890123","factory_id":"1532691012899861700","id":"15330383704456901","oil_type":"0","org_id":"1532690803569632889","rfid":"0132FC000B9BE9E2","rfid_type":"1","share_flag":"2","syn_flag":"2","syn_status":"4","syn_time":"2018-07-31 19:59:30","sys_load_capacity_id":"1528028478","traffic_no":"京货123456789","updated":"2018-07-31 19:59:30","use_type":"3","user_id":"1533037833981363641","valid_flag":"1","vin_no":"vinc32787"}],"handle":"add","per_page":"1","total_page":"1"},"id":"12758622860289","local_register_id":"2","org_id":"1532690803569632889","prog_seq":"1","prog_type":"3","re_send":"0","scope":"biz_car","type":"data"}`
			msg := &proto.Message{Value: []byte(line)}
			fmt.Println(i)
			producer.Produce(topic_weighReceive, partition, msg);
		//}

	}
}

func AproduceStdin(line string) {
	producer := broker.Producer(kafka.NewProducerConf())
	//input := bufio.NewReader(os.Stdin)

		//line := `{"type":"service","act":"request","scope":"local_register","id":"14880505866497","local_register_id":"1","org_id":"13280505866498","direction":"0","re_send":"0","act_time":"2016-12-30 19:30:36","entity":{ "handle":"add","all_total":"100","total_page":"10","cur_ page":"3","per_page":"10","cur_ total":"10","data":[{"id":"13280505866498","parent_id":"13200505866498","full_name":"美锦集团煤化工XXXX","short_name":"美锦煤化工XXXX","group_flag":"0","local_register_id": "11112","syn_flag":"1","syn_status":"1","syn_time":"2016-12-30 19:30:37","share_flag":"0","user_id":"11112","org_id":"111121121212","order_id":"1","deleted":"1","updated":"2016-12-30 19:30:37","created":"2016-11-30 19:30:36"},{"id":"13280505866497","parent_id":"13200505866498","full_name":"美锦集团煤化工XXXX","short_name":"美锦煤化工XXXX","group_flag":"0","local_register_id":"11112","syn_flag":"1","syn_status":"1","syn_time":"2016-12-30 19:30:37","share_flag":"0","user_id":"11112","org_id":"111121121212","order_id":"1","deleted":"1","updated":"2016-12-30 19:30:37","created":"2016-11-30 19:30:36"}]}}`

		msg := &proto.Message{Value: []byte(line)}
		if _, err := producer.Produce(topic_weighSend, partition, msg); err != nil {
			log.Fatalf("produceStdin---cannot produce message to %s:%d: %s", topic_weighSend, partition, err)
		}

		fmt.Println(line)
}

func KaProucer() {
	fmt.Println("生产1")
	//username := Config.GetValue("database", "hostname") //database是你的[section]，username是你要获取值的key名称
	//fmt.Println(username + ":3306") //root

	KafkaConf := kafka.NewBrokerConf("test-client")
	KafkaConf.AllowTopicCreation = true

	// connect to kafka cluster
	broker, err := kafka.Dial(kafkaAddrs, KafkaConf)
	if err != nil {
		log.Fatalf("KaProucer---cannot connect to kafka cluster: %s", err)
	}
	defer broker.Close()

	fmt.Println("生产")
	//go printConsumed(broker)
	produceStdinTest(broker)
}

func KaConsume() {

	//KafkaConf := kafka.NewBrokerConf("test-client")
	//KafkaConf.AllowTopicCreation = true
	//
	//// connect to kafka cluster
	//broker, err := kafka.Dial(kafkaAddrs, KafkaConf)
	//if err != nil {
	//	log.Fatalf("KaConsume---cannot connect to kafka cluster: %s", err)
	//}
	//defer broker.Close()
	//
	//fmt.Println("消费者consume >> weighReceive start ...")
	printConsumed(broker)
}
