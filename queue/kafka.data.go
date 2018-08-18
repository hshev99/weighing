package queue
import (
	"strings"
	"github.com/bitly/go-simplejson"
	"bytes"
	"strconv"
	"time"
	"encoding/json"
	"github.com/astaxie/beego"
	"weighing/models"
)

func ActData(str_json []byte)  (bool,string){
	js, _ := simplejson.NewJson(str_json)
	scope,_ := js.Get("scope").String()
	local_register_id,_ := js.Get("local_register_id").String()

	//对 type 为 data  就行数据解析同步

	entity_handle,_ := js.Get("entity").Get("handle").String()

	//定义错误数量
	err_num :=0
	err_msg :=""

	//局端传上来的数据，需要同步到其他局端
	var synLocOthers bool
	if scope =="biz_car" || scope=="biz_driver" || scope=="sys_material" || scope=="sys_material_goods" ||  scope=="org_client" || scope=="sys_address" {
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

		var exe_sql_bk string
		var exe_sql string
		var exe_sql_id string
		var biz_insert_id string
		var biz_type string
		var created string

		//正对于磅单表定义三个作为主键 和add 转 update
		var pond_factory_id string

		var pond_add_to_update string
		pond_add_to_update =" DELETE FROM  `"+scope+"` where `id`="

		if entity_handle == "add" {
			buffer.WriteString("insert into ")
			buffer.WriteString("`")
			buffer.WriteString(scope)
			buffer.WriteString("`")
			buffer.WriteString(" set ")

			//解析 map
			for k, v := range lan_data_map {
				//定义
				va := v.(string)

				//提取公共参数
				if k == "id" {
					exe_sql_id = v.(string)
					pond_add_to_update += exe_sql_id
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
					case "save_path":
						va = ""
						
					}
				}else if scope == "biz_ponderation" {
					if k == "factory_id" {
						pond_factory_id = v.(string)
						pond_add_to_update += " AND `factory_id`="+pond_factory_id
					}

					if v.(string) == "" {
						continue
					}	
				}else if scope == "biz_car" {
					if v.(string) == "" {
						continue
					}
				}

				buffer.WriteString("`")
				buffer.WriteString(string(k))
				buffer.WriteString("`")
				buffer.WriteString("=")


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


		}else if entity_handle == "edit" {
			buffer.WriteString("update ")
			buffer.WriteString("`")
			buffer.WriteString(scope)
			buffer.WriteString("`")
			buffer.WriteString(" set ")

			var update_id  string
			//解析 map
			for k, v := range lan_data_map {
				//提取公共参数
				if k == "id" {
					exe_sql_id = v.(string)
				}

				//对指定表进行必要的数据更改
				if scope == "biz_ponderation" {
					if k == "factory_id" {
						pond_factory_id = v.(string)
						pond_add_to_update += " AND `factory_id`="+pond_factory_id
					}

					if v.(string) == "" {
						continue
					}
				}else if scope == "biz_contract" {
					if v.(string) == "" {
						continue
					}
				}

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

			//正对指定表进行更改
			if scope == "biz_ponderation" {
				exe_sql += " AND `factory_id`="+pond_factory_id

			}


		}else if entity_handle == "del" {

			buffer.WriteString("delete from ")
			buffer.WriteString("`")
			buffer.WriteString(scope)
			buffer.WriteString("`")

			var update_id  string
			//解析 map
			for k, v := range lan_data_map {
				//提取公共参数
				if k == "id" {
					exe_sql_id = v.(string)
				}


				if k == "id" {
					update_id = v.(string)
				}
				//拼接字符串

			}

			//删除 最后一个 ,
			exe_sql = buffer.String()
			exe_sql += " where `id`=" +update_id

			//正对指定表进行更改
			if scope == "biz_ponderation" {
				exe_sql += " AND `factory_id`="+pond_factory_id

			}


		}else {
			exe_sql = "select * from `"+scope+"` limit 1"
			err_num++
			err_msg+="handle 缺失,请自查"
		}

		//fmt.Println(exe_sql)
		sql_rowsAffected,_,sql_err := models.ExecSql(exe_sql)

		if sql_rowsAffected == 0 {

			if entity_handle == "add" {
				//在这里进行补救 如还是失败在返回失败
				//查询是否存在ID 的值
				sql_sel :=""
				if scope == "admin_user" {
					sql_sel = "SELECT `user_id` a FROM `"+scope+"` WHERE `user_id`="+exe_sql_id
				}else {
					sql_sel = "SELECT `id` a FROM `"+scope+"` WHERE `id`="+exe_sql_id
				}

				sql_sel_count := models.SelsetSqlBySqlCount(sql_sel)

				if sql_sel_count >0 {
					//新增失败 更新再试试
					sql_del :=""
					if scope == "admin_user" {
						sql_del = "DELETE FROM `"+scope+"` WHERE `user_id`="+exe_sql_id
					}else {
						sql_del = "DELETE FROM `"+scope+"` WHERE `id`="+exe_sql_id
					}
					models.ExecSql(sql_del)
					sql_rowsAffected_two_end,_,sql_err_two_end := models.ExecSql(exe_sql)

					if sql_rowsAffected_two_end ==0 {
						if sql_err_two_end != nil {
							err_num ++
							err_msg += "删除ID重新尝试>>"+models.RawValueToString(sql_err_two_end.Error())
						}else {
							err_num ++
							err_msg += "无知错误"
						}
					}
				}else {

					if sql_err != nil {
						err_num ++
						err_msg += "删除ID重新尝试>>"+models.RawValueToString(sql_err.Error())
					}else {
						err_num ++
						err_msg += "无知错误"
					}
				}

			}else if entity_handle == "edit" {
				if sql_err != nil {
					err_num ++
					err_msg += models.RawValueToString(sql_err.Error())
				}else {
					err_num ++
					err_msg += "影响行数为0;可能原因:没有该主键的记录"
				}
			}else if entity_handle == "del"{
				err_num ++
				if sql_err == nil {
					err_msg += "删除失败"
				}else {
					err_msg += models.RawValueToString(sql_err.Error())
				}

			}

		}else {
			//给备用的表再吸入人数据
			if scope == "biz_ponderation" {
				exe_sql_bk = strings.Replace(exe_sql,"biz_ponderation","biz_ponderation_bk",-1)
				models.ExecSql(exe_sql_bk)
			}
		}
		//同步到其他局端
		if synLocOthers {
			ActWebApiSendMessage(exe_sql_id ,scope ,"add" ,local_register_id)
		}

		//对照片记录更新地址
		if scope == "biz_photo" {
			sql_update_photo := "update biz_photo set save_path= CONCAT('/mnt/','"+local_register_id+"/','"+biz_type+"/' ,'"+created+"/') where id = "+biz_insert_id
			models.ExecSql(sql_update_photo)
		}

		//给数组写值
		exe_sql_arr[0] = exe_sql
	}

	if err_num > 0 {
		return false,err_msg
	}else {
		return true,err_msg
	}
}

//处理通用数据 回复接口
func PublicDataRespond(id string)  {
	//redis 获取该数据  并删除 redis记录
	models.ClearKafkaSend(id)
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
		sql_result,err_msg:=ActData(str_json)

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

		if sql_result {
			line_arr_entity["result"]="1"
			line_arr_entity["result_msg"]="成功"
		}else {
			line_arr_entity["result"]="0"
			line_arr_entity["result_msg"]="数据库执行失败："+err_msg
		}


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