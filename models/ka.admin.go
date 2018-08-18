package models

import (
	"strings"
	"github.com/bitly/go-simplejson"
	"bytes"
	"fmt"
	"strconv"
	"time"
	"encoding/json"
	"github.com/astaxie/beego"
)

func actData(str_json []byte)  {
	js, _ := simplejson.NewJson(str_json)
	scope,_ := js.Get("scope").String()
	local_register_id,_ := js.Get("local_register_id").String()

	//对 type 为 data  就行数据解析同步

	entity_handle,_ := js.Get("entity").Get("handle").String()

	//局端传上来的数据，需要同步到其他局端
	var synLocOthers bool
	if scope =="biz_car" || scope=="biz_driver" {
		synLocOthers = true
	}else {
		synLocOthers = false
	}

	//对 data数据部分就行解析
	lan_data, _ := js.Get("entity").Get("data").Array()
	var exe_sql_arr= [100]string{}
	//对数据进行循环
	for  a:=0;a<len(lan_data);a++{

		lan_data_arr := lan_data[a]

		lan_data_map := lan_data_arr.(map[string]interface{})

		var buffer bytes.Buffer

		var exe_sql string
		var exe_sql_id string
		var biz_insert_id string
		var biz_type string
		var created string
		if entity_handle == "add" {
			buffer.WriteString("insert into ")
			buffer.WriteString("`")
			buffer.WriteString(scope)
			buffer.WriteString("`")
			buffer.WriteString(" set ")

			//解析 map
			for k, v := range lan_data_map {
				//提取公共参数
				if k == "id" {
					exe_sql_id = v.(string)
				}
				//拼接字符串
				//针对biz_photo 表进行提取数据
				if scope == "biz_photo" {
					switch k {
					case "id":
						biz_insert_id = v.(string)
					case "type":
						biz_type = v.(string)
					case "created":
						created =beego.Substr(v.(string),0,10)
					}
				}

				buffer.WriteString("`")
				buffer.WriteString(string(k))
				buffer.WriteString("`")
				buffer.WriteString("=")

				va := v.(string)
				if k == "id_1" || k == "syn_status" || k=="syn_time" {

					switch k {
					case "id_1":
						buffer.WriteString("'")
						b2 := time.Now().UnixNano()
						c := strconv.FormatInt(b2,10)
						buffer.WriteString(c)
						buffer.WriteString("'")
					case "syn_status":
						buffer.WriteString("'")
						c := "2"
						buffer.WriteString(c)
						buffer.WriteString("'")
					case "syn_time":
						c := "now()"
						buffer.WriteString(c)
					}

				}else {
					buffer.WriteString("'")
					buffer.WriteString(va)
					buffer.WriteString("'")
				}

				buffer.WriteString(",")
			}
			//删除 最后一个 ,
			exe_sql = buffer.String()
			exe_sql =strings.TrimRight(exe_sql,",")


			//同步到其他局端
			fmt.Print(exe_sql_id)
			fmt.Print(synLocOthers)
			//if synLocOthers {
			//	queue.ActWebApiSendMessage(exe_sql_id ,scope ,"add" ,local_register_id)
			//}
		}else if entity_handle == "edit" {
			buffer.WriteString("update ")
			buffer.WriteString("`")
			buffer.WriteString(scope)
			buffer.WriteString("`")
			buffer.WriteString(" set ")

			var update_id  string
			//解析 map
			for k, v := range lan_data_map {

				if k == "id" {
					update_id = v.(string)
				}else {
					buffer.WriteString("`")
					buffer.WriteString(string(k))
					buffer.WriteString("`")
					buffer.WriteString("=")
					buffer.WriteString("'")
					va := v.(string)
					buffer.WriteString(va)
					buffer.WriteString("'")
					buffer.WriteString(",")
				}
				//拼接字符串
			}

			//删除 最后一个 ,
			exe_sql = buffer.String()
			exe_sql =strings.TrimRight(exe_sql,",")


			exe_sql += " where `id`=" +update_id
		}else if entity_handle == "del" {

			buffer.WriteString("delete from ")
			buffer.WriteString("`")
			buffer.WriteString(scope)
			buffer.WriteString("`")

			var update_id  string
			//解析 map
			for k, v := range lan_data_map {

				if k == "id" {
					update_id = v.(string)
				}
				//拼接字符串

			}

			//删除 最后一个 ,
			exe_sql = buffer.String()
			exe_sql += " where `id`=" +update_id

		}else {
			exe_sql = "select * from `"+scope+"` limit 1"
		}

		fmt.Println(exe_sql)
		ExecSql(exe_sql)

		//对照片记录更新地址
		if scope == "biz_photo" {
			sql_update_photo := "update biz_photo set save_path= CONCAT('/mnt/','"+local_register_id+"/','"+biz_type+"/' ,'"+created+"/') where id = "+biz_insert_id
			ExecSql(sql_update_photo)
		}

		//给数组写值
		exe_sql_arr[0] = exe_sql
	}

}

//处理通用数据 回复接口
func PublicDataRespond(id string)  {
	//redis 获取该数据  并删除 redis记录
	ClearKafkaSend(id)
}

//通用数据
func PublicData(str_json []byte) (int ,string){
	//1 正常数据，接收并处理成功
	//2 正常数据  接收处理失败
	//9 异常数据  舍弃
	js, _ := simplejson.NewJson(str_json)

	scope,_ := js.Get("scope").String()

	act,_ := js.Get("act").String()
	if act == "request" {
		act_type,_ := js.Get("type").String()

		// 进行数据处理
		actData(str_json)

		act_id,_ := js.Get("id").String()
		act_org_id,_ := js.Get("org_id").String()
		local_register_id,_ := js.Get("local_register_id").String()
		prog_type,_ := js.Get("prog_type").String()
		prog_seq,_ := js.Get("prog_seq").String()
		direction,_ := js.Get("direction").String()
		re_send,_ := js.Get("re_send").String()

		line_arr := make(map[string]interface{})
		line_arr_entity := make(map[string]interface{})
		line_arr["type"]=act_type
		line_arr["act"]="respond"
		line_arr["scope"]=scope
		line_arr["id"]=act_id
		line_arr["org_id"]=act_org_id
		line_arr["direction"]=direction
		line_arr["prog_type"]=prog_type
		line_arr["prog_seq"]=prog_seq
		line_arr["local_register_id"]=local_register_id
		line_arr["re_send"]=re_send
		line_arr["act_time"]=time.Now().Local().Format("2006-01-02 15:04:05")
		line_arr["entity"]=line_arr_entity
		line_arr_entity["result"]="1"
		line_arr_entity["result_msg"]="成功"

		b, _ := json.Marshal(line_arr)
		line := string(b)
		return 1,line
	}else if act == "respond" {
		//在这里处理 相应信息
		id,_ := js.Get("id").String()
		PublicDataRespond(id)
		return 6,"回复类消息处理完成"
	}else {
		return 9,""
	}
	return 9,""
}

func ExeSql(exe_sql string) bool {
	//db := getDB()
	_, err := db.Exec(exe_sql)
	if err!= nil {
		//记录日志
		go WELog(0,exe_sql,err)
		return false
	}else {
		go WSLog(0,exe_sql)
		return true
	}
}