package controllers

import (
	"net/http"
	"github.com/julienschmidt/httprouter"
	"fmt"
	"encoding/json"
	"io/ioutil"
	"weighing/models"
	"weighing/queue"
	"github.com/bitly/go-simplejson"
	"time"
	"strconv"
	"crypto/md5"
	"strings"
	"reflect"
	"unsafe"
	"math"
)
const MIN = 0.000001

func String(b []byte) (s string) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
}
func Slice(s string) (b []byte) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pbytes.Data = pstring.Data
	pbytes.Len = pstring.Len
	pbytes.Cap = pstring.Len
	return
}

func VerifyUser(r *http.Request) (bool ,string,string) {
	//在这里获取cookie
	//return true,"1527042476685522437"
	ci_session_user_id, err := r.Cookie("ci_session_user_id")
	ci_session_org_type, _ := r.Cookie("ci_session_org_type")
	return true,ci_session_user_id.Value,ci_session_org_type.Value

	//ci_session_atom := "C4CA4238A0B923820DCC509A6F75849B"
	if err == nil || ci_session_user_id.Value == "" {
		return false,"登录过期,请重新登录1",ci_session_org_type.Value
	}

	ci_session_atom, err := r.Cookie("ci_session_atom")
	if err == nil || ci_session_atom.Value == ""{
		return false,"登录过期,请重新登录2",ci_session_org_type.Value
	}

	user_id :=  ci_session_user_id.Value
	//在这里查看redis 中存在的
	fmt.Println("在这里查看redis 中存在的>>>>>")
	res,value := models.GetUserTooken(user_id,ci_session_atom.Value)
	//fmt.Println("true/false",res)
	//fmt.Println("value",value)
	if res {
		return true,value,ci_session_org_type.Value
	}else {
		return false,value,ci_session_org_type.Value
	}


}

func Sys(w http.ResponseWriter, r *http.Request, ps httprouter.Params)  {

	fmt.Println(ps.ByName("name"))
	fmt.Println(ps.ByName("scope"))

	//在这里获取cookie
	verify,user_id,org_type := VerifyUser(r)
	if !verify {
		SysNoVerify(w,"143","登录过期，请重新登录")
		fmt.Println(user_id)
		return
	}

	roue := ps.ByName("name")
	scope := ps.ByName("scope")
	//对接收到的 参数就行分析
	con, err := ioutil.ReadAll(r.Body) //获取post的数据
	if err != nil {
		SysNoVerify(w,"109","数据获取错误")
		fmt.Println(err)
		return
	}

	api_params := make(map[string]string)
	err = json.Unmarshal(con, &api_params)

	switch roue {
	case "PostSys":
		PostSys(w ,api_params,scope,user_id,org_type)
	case "GetSys":
		GetSys(w,api_params)
	default:
		SysDefault(w)
	}

	return
}


/*
* 1
*
*/
func GetFieldByTable(api_params map[string]string,scope string,user_id string,org_type string)  (int,string,string,int,string,string,string,string) {
	b, _ := json.Marshal(api_params)
	js, _ := simplejson.NewJson(b)

	where := ""
	field := ""

	sql_exc :=""
	sql_count :="SELECT COUNT(1) a FROM "

	act,_ := js.Get("act").String()
	p,_ := js.Get("page").String()

	if p == "" {
		p = "1"
	}

	page,_:=strconv.Atoi(p)

	//对公共的参数进行提炼
	set_public :=""

	//	同步标志0不同步、1同步
	syn_flag,_ := js.Get("syn_flag").String()
	if syn_flag =="" {
		syn_flag ="1"
	}
	set_public += "`syn_flag`='"+syn_flag+"',"


	//同步状态，0初始值 、1正同步中、2 同步成功、3 同步失败
	syn_status := "0"
	set_public += "`syn_status`='"+syn_status+"',"

	//操作人员user_id，系统自动处理为1，1为超级系统用户admin
	//user_id := "1"
	set_public += "`user_id`='"+user_id+"',"

	//查询用户所属 org_id
	sel_user_org_id := "select `org_id` as a from `admin_user` where `user_id`="+user_id
	user_org_id :=models.SelsetSqlBySqlMax(sel_user_org_id)

	if user_org_id == "0" && user_id !="1" {
		return 0,"","该账户异常，请联系管理员(org_id uid不对应)",page,"",scope,"",""
	}

	fmt.Println("^^^^^^^^^^^org_id",user_org_id)
	//共享标志0私有、1对外共享
	//后面更改共享标示		0私有、1企业共享、2集团共享、3平台共享
	if scope=="org_factory" || scope=="org_gate" || scope=="org_house" || scope=="admin_user" || scope=="biz_ponderation" || scope=="biz_photo" {
		set_public += "`share_flag`=0,"
	}else if scope =="org_client" || scope=="sys_material" || scope=="sys_material_goods" || scope=="sys_address"{

		set_public += "`share_flag`=2,"
	}else if  scope =="biz_car" || scope=="biz_driver" || scope=="sys_load_capacity" || scope=="sys_para_item" {

		set_public += "`share_flag`=3,"

	}else if scope =="org_info" || scope=="biz_contract" {
		set_public += "`share_flag`=1,"
	}else{
		set_public += "`share_flag`=1,"
	}

	err_del_msg := "该记录已删除"

	//定义操作时间
	opt_time_log :=time.Now().Local().Format("2006-01-02 15:04:05")
	switch scope {

	case "org_info":
		//归属企业id，0为共有

		set :=""
		//对org_info 企业信息表进行操作
		id,_ := js.Get("id").String()

		where += " where 1 "

		parent_id,_ := js.Get("parent_id").String()
		full_name,_ := js.Get("full_name").String()
		short_name,_ := js.Get("short_name").String()
		group_flag,_ := js.Get("group_flag").String()

		//没有选择父类ID 则 给默认值0
		if parent_id == "" {
			parent_id = "0"
		}

		//集团用户添加
		if parent_id =="0" && user_id !="1" {
			parent_id = user_org_id
		}

		//管理员权限可以修改父类
		if user_id=="1" {
			set += "`parent_id`='"+parent_id+"',"
		}else if act =="add" {
			set += "`parent_id`='"+parent_id+"',"
		}


		set += "`full_name`='"+full_name+"',"
		set += "`short_name`='"+short_name+"',"

		if user_id == "1"{
			if group_flag == "" {
				if parent_id == "0"{
					set += "`group_flag`='1',"
				}else {
					set += "`group_flag`='0',"
				}
			}else {
				set += "`group_flag`='"+group_flag+"',"
			}
		}
		if group_flag == ""{
			sel_sql :="select `group_flag` as a from `org_info` where `id`='"+id+"'"
			group_flag = models.SelsetSqlBySqlMax(sel_sql)
		}

		pon_code,_ := js.Get("pon_code").String()
		if pon_code =="" && act=="add" {
			if parent_id=="" {
				//set += "`pon_code`= (select a.pon_code+1 from `org_info` a where a.pon_code not regexp '[^0-9]'  ORDER BY a.pon_code DESC limit 1),"
				return 0,"","参数错误，缺少pon_code",page,id,scope,"",""
			}else{
				set += "`pon_code`= (select a.pon_code from `org_info` a where a.id ="+parent_id+" ORDER BY a.pon_code DESC limit 1),"
			}
		}else {
			set += "`pon_code`='"+pon_code+"',"
		}
		set += set_public

		//查询有用的字段
		field += " `id`,`parent_id`,`full_name`,`short_name`,`group_flag`,`local_register_id`,`created` ";

		switch act {
		case "add":
			//对值进行效验
			if full_name == "" {
				return 0,"","参数错误，缺少full_name",page,id,scope,"",""
			}
			//申请ID
			b2 := time.Now().UnixNano()
			id = strconv.FormatInt(b2,10)
			set += "`id`= "+id+","
			set += "`org_id`= "+id+","
			set += "`deleted`= 0,"
			//set += "`pon_code`= (select a.pon_code+1 from `org_info` a where a.pon_code not regexp '[^0-9]'  ORDER BY a.pon_code DESC limit 1),"
			set += "`syn_time`= now(),"
			set += "`created`= now(),"
			set += "`updated`= now()"
			sql_exc += "INSERT INTO "+scope+" set "+set;

			return 2,sql_exc,"添加公司",page,id,scope,"","企业管理,公司管理,添加,添加公司 全称:"+full_name+" ; ID:"+id+","+user_id+","+opt_time_log+","+user_org_id
		case "edit":

			// table string,field string,value string,target string
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 158,"",err_del_msg,page,id,scope,"",""
			}

			//查询旧数据
			//查询合同内容
			sql_sel_org :="select * from `org_info` where `id`='"+id+"'"
			sql_org_rowsAffected := models.SelsetSqlBySqlSliceOne(sql_sel_org)

			org_b, _ := json.Marshal(sql_org_rowsAffected)
			org_js, _ := simplejson.NewJson(org_b)

			sel_group_flag,_ := org_js.Get("group_flag").String()
			sel_parent_id,_ := org_js.Get("parent_id").String()

			//集团降级了
			if (user_id == "1"){

				if (group_flag != sel_group_flag && group_flag=="0" && sel_group_flag=="1") ||
					(parent_id!=sel_parent_id && group_flag=="0" && sel_group_flag=="1"){
					//修改本身

					//受影响的项
					sql_select :="SELECT `id`,`pon_code` FROM `org_info` where `parent_id` ="+id
					resultRows,_ := models.SelectSqlToStringMap(sql_select)
					if len(resultRows) >0 {
						for _,v := range resultRows{
							b, _ := json.Marshal(v)
							js, _ := simplejson.NewJson(b)
							child_id,_ := js.Get("id").String()

							update_child_sql :="UPDATE `org_info` set `parent_id`=0 where `id`="+child_id
							models.PostSys(update_child_sql)

							fmt.Println("父类降级子类归属平台..>",update_child_sql)
							//发送同步数据
							queue.ActWebApiSendMessage(child_id ,"org_info" ,"edit" ,"")
						}
					}

				}

			}

			//集团解散

			//对 集团公司 则要连带 子公司修改磅单码
			if group_flag =="1" || parent_id=="0"{
				where +=" and id = "+id;
				set += "`deleted`= 0,"
				set += "`updated`= now()"
				sql_exc += "UPDATE "+scope+" set "+set + where;

				fmt.Println("修改父类公司..")
				return 11,sql_exc,"修改公司",page,id,scope,"","企业管理,公司管理,修改,修改公司 全称:"+full_name+" ; ID"+id+","+user_id+","+opt_time_log+","+user_org_id
			}else {
				where +=" and id = "+id;
				set += "`deleted`= 0,"
				set += "`updated`= now()"
				sql_exc += "UPDATE "+scope+" set "+set + where;

				return 3,sql_exc,"修改公司",page,id,scope,"","企业管理,公司管理,修改,修改公司 全称:"+full_name+" ; ID"+id+","+user_id+","+opt_time_log+","+user_org_id
			}

		case "del":
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 0,"",err_del_msg,page,id,scope,"",""
			}

			//判断是有有子类， 有子类则不能删除
			sel_deleted_goods :="select `id` as a from `org_info` where `parent_id`='"+id+"' and `deleted`=0 limit 1"

			is_deleted_goods := models.SelsetSqlBySqlMax(sel_deleted_goods)
			if is_deleted_goods!="0" {
				return 0,"","该公司下有子公司，不允许删除",page,id,scope,"",""
			}

			where +=" and id = "+id;
			set = "`deleted`= 1,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set + where;

			return 4,sql_exc,"删除公司",page,id,scope,"","企业管理,公司管理,删除,删除公司 全称:"+full_name+" ; ID"+id+","+user_id+","+opt_time_log+","+user_org_id
		case "get":
			sql_exc += "SELECT "+field+" FROM "+scope;
			sql_count += scope
			return 1,sql_exc,sql_count,page,id,scope,"",""
		default:
			return 0,sql_exc,"缺少act",page,id,scope,"",""
		}


	//对系统参数表进行操作
	case "sys_para_item":
		//归属企业id，0为共有
		org_id := user_org_id
		set_public += "`org_id`='"+org_id+"',"

		set :=""
		//对org_info 企业信息表进行操作
		id,_ := js.Get("id").String()

		where += " where 1 "

		parent_id,_ := js.Get("parent_id").String()
		item_name,_ := js.Get("item_name").String()
		item_value,_ := js.Get("item_value").String()
		item_code,_ := js.Get("item_code").String()

		deep_value,_ := js.Get("deep_value").String()
		valid_flag,_ := js.Get("valid_flag").String()
		if valid_flag=="" {
			valid_flag ="1"
		}

		set += "`parent_id`='"+parent_id+"',"
		set += "`item_name`='"+item_name+"',"
		if item_code != ""{
			set += "`item_code`='"+item_code+"',"
		}
		set += "`item_value`='"+item_value+"',"
		set += "`valid_flag`='"+valid_flag+"',"

		set += "`deep_value`='"+deep_value+"',"
		set += set_public

		//字段提取
		field += " `id`,`parent_id`,`item_name`,`item_value`,`item_code`,`deep_value`,`valid_flag`,`created` "

		switch act {
		case "add":
			b2 := time.Now().UnixNano()
			id = strconv.FormatInt(b2,10)
			set += "`id`= "+id+","
			set += "`deleted`= 0,"
			set += "`order_id`= 1,"
			set += "`syn_time`= now(),"
			set += "`created`= now(),"
			set += "`updated`= now()"
			sql_exc += "INSERT INTO "+scope+" set "+set;

			return 2,sql_exc,"添加系统参数",page,id,scope,",该编码已存在","系统管理,系统参数,添加,添加系统参数:"+item_name+","+user_id+","+opt_time_log+","+user_org_id
		case "edit":

			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 158,"",err_del_msg,page,id,scope,"",""
			}

			where +=" and id = "+id;
			set += "`deleted`= 0,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set + where;

			return 3,sql_exc,"修改系统参数",page,id,scope,"","系统管理,系统参数,修改,修改系统参数:"+item_name+","+user_id+","+opt_time_log+","+user_org_id
		case "del":
			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 0,"",err_del_msg,page,id,scope,"",""
			}

			where +=" and id = "+id;

			set := "`deleted`= 1,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set+ where;

			if item_name ==""{
				sel_name := "select `item_name` as a from `"+scope+"` where  `id`='"+id+"'"
				item_name = models.SelsetSqlBySqlMax(sel_name)
			}

			return 4,sql_exc,"删除系统参数",page,id,scope,"","系统管理,系统参数,删除,删除系统参数:"+item_name+","+user_id+","+opt_time_log+","+user_org_id
		case "get":

			sql_exc += "SELECT "+field+" FROM "+scope;
			sql_count += scope
			return 1,sql_exc,sql_count,page,id,scope,"",""
		default:
			return 0,sql_exc,"缺少act",page,id,scope,"",""
		}


	//对地址进行管理
	case "sys_address":
		//归属企业id，0为共有
		org_id :=user_org_id
		set_public += "`org_id`='"+org_id+"',"

		set :=""
		//对org_info 企业信息表进行操作
		id,_ := js.Get("id").String()

		where += " where 1 "

		address,_ := js.Get("address").String()
		valid_flag,_ := js.Get("valid_flag").String()
		if valid_flag == "" {
			valid_flag ="1"
		}
		set += "`address`='"+address+"',"

		//公共参数
		set += "`valid_flag`='"+valid_flag+"',"

		set += set_public

		//字段提取
		field += " `id`,`address`,`created` "

		switch act {
		case "add":
			b2 := time.Now().UnixNano()
			id = strconv.FormatInt(b2,10)
			set += "`id`= "+id+","
			set += "`order_id`= 0,"
			set += "`deleted`= 0,"
			set += "`syn_time`= now(),"
			set += "`created`= now(),"
			set += "`updated`= now()"
			sql_exc += "INSERT INTO "+scope+" set "+set;

			return 2,sql_exc,"添加地址",page,id,scope,"","系统管理,地址管理,添加,添加地址:"+address+","+user_id+","+opt_time_log+","+user_org_id
		case "edit":
			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 158,"",err_del_msg,page,id,scope,"",""
			}

			where +=" and id = "+id;
			set += "`deleted`= 0,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set + where;

			return 3,sql_exc,"修改地址",page,id,scope,"","系统管理,地址管理,修改,修改地址:"+address+","+user_id+","+opt_time_log+","+user_org_id
		case "del":
			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 0,"",err_del_msg,page,id,scope,"",""
			}

			where +=" and id = "+id;

			set = "`deleted`= 1,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set+ where;

			if address =="" {
				sel_name := "select `address` as a from `"+scope+"` where  `id`='"+id+"'"
				address = models.SelsetSqlBySqlMax(sel_name)
			}

			return 4,sql_exc,"删除地址",page,id,scope,"","系统管理,地址管理,删除,删除地址:"+address+","+user_id+","+opt_time_log+","+user_org_id
		case "get":
			sql_count += scope
			sql_exc += "SELECT "+field+" FROM "+scope;

			return 1,sql_exc,sql_count,page,id,scope,"",""

		default:
			return 0,sql_exc,"缺少act",page,id,scope,"",""
		}

	//物料名称
	case "sys_material_goods":
		//归属企业id，0为共有
		org_id :=user_org_id
		set_public += "`org_id`='"+org_id+"',"

		set :=""
		//对org_info 企业信息表进行操作
		id,_ := js.Get("id").String()

		where += " where 1 "

		name,_ := js.Get("name").String()
		material_id,_ := js.Get("material_id").String()
		code,_ := js.Get("code").String()


		valid_flag,_ := js.Get("valid_flag").String()
		if valid_flag =="" {
			valid_flag ="1"
		}

		set += "`name`='"+name+"',"
		set += "`material_id`='"+material_id+"',"
		set += "`code`='"+code+"',"

		//公共参数
		set += "`valid_flag`='"+valid_flag+"',"
		set += set_public

		//字段提取
		field += " `id`,`name`,`material_id`,`code`,`created` "

		switch act {
		case "add":

			//物流名称排重
			sel_isset_login_name := "select `id` as a from `"+scope+"` where  `name`='"+name+"' and `material_id`='"+material_id+"' and deleted=0 limit 1"
			sel_isset := models.SelsetSqlBySqlMax(sel_isset_login_name)
			if sel_isset!="0" {
				return 0,"","该名称已存在",page,id,scope,"",""
			}

			sel_isset_login_code := "select `id` as a from `"+scope+"` where  `code`='"+code+"' and and `material_id`='"+material_id+"' deleted=0 limit 1"
			sel_isset_code := models.SelsetSqlBySqlMax(sel_isset_login_code)
			if sel_isset_code!="0" {
				return 0,"","该编码已存在",page,id,scope,"",""
			}

			b2 := time.Now().UnixNano()
			id = strconv.FormatInt(b2,10)
			set += "`id`= "+id+","
			set += "`order_id`= 0,"
			set += "`deleted`= 0,"
			set += "`syn_time`= now(),"
			set += "`created`= now(),"
			set += "`updated`= now()"
			sql_exc += "INSERT INTO "+scope+" set "+set;

			return 2,sql_exc,"添加物品名称",page,id,scope,"","系统管理,物料名称,添加,添加物料名称:"+name+","+user_id+","+opt_time_log+","+user_org_id
		case "edit":
			//物流名称排重
			sel_isset_login_name := "select `id` as a from `"+scope+"` where  `name`='"+name+"' and `material_id`='"+material_id+"' and deleted=0 limit 1"
			sel_isset := models.SelsetSqlBySqlMax(sel_isset_login_name)
			if sel_isset!="0" && sel_isset !=id {
				return 0,"","该名称已存在",page,id,scope,"",""
			}

			//检测榜单码是否存在
			sel_isset_login_code := "select `id` as a from `"+scope+"` where  `code`='"+code+"' and `material_id`='"+material_id+"' and deleted=0 limit 1"
			sel_isset_code := models.SelsetSqlBySqlMax(sel_isset_login_code)
			if sel_isset_code!="0" && sel_isset_code!=id{
				return 0,"","该编码已存在",page,id,scope,"",""
			}

			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 158,"",err_del_msg,page,id,scope,"",""
			}

			where +=" and id = "+id;
			set += "`deleted`= 0,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set + where;

			return 3,sql_exc,"修改物品名称",page,id,scope,"","系统管理,物料名称,修改,修改物料名称:"+name+","+user_id+","+opt_time_log+","+user_org_id
		case "del":
			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 0,"",err_del_msg,page,id,scope,"",""
			}
			
			//判断大类下面是否有子类
			sel_deleted_goods :="select `id` as a from `sys_material_goods` where `material_id`='"+id+"' and `deleted`=0 limit 1"

			is_deleted_goods := models.SelsetSqlBySqlMax(sel_deleted_goods)
			if is_deleted_goods!="0" {
				return 0,"","该类别下有子类，不允许删除",page,id,scope,"",""
			}

			where +=" and id = "+id;

			set = "`deleted`= 1,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set+ where;

			if name == "" {
				sel_name := "select `name` as a from `"+scope+"` where  `id`='"+id+"'"
				name = models.SelsetSqlBySqlMax(sel_name)
			}

			return 4,sql_exc,"删除物品名称",page,id,scope,"","系统管理,物料名称,删除,删除物料名称:"+name+","+user_id+","+opt_time_log+","+user_org_id
		case "get":
			sql_exc += "SELECT "+field+" FROM "+scope
			sql_count += scope
			return 1,sql_exc,sql_count,page,id,scope,"",""
		default:
			return 0,sql_exc,"缺少act",page,id,scope,"",""
		}


	//物料类别 sys_material
	case "sys_material":
		//归属企业id，0为共有
		org_id := user_org_id
		set_public += "`org_id`='"+org_id+"',"

		set :=""
		//对org_info 企业信息表进行操作
		id,_ := js.Get("id").String()

		where += " where 1 "

		name,_ := js.Get("name").String()
		code,_ := js.Get("code").String()

		valid_flag,_ := js.Get("valid_flag").String()
		if valid_flag == "" {
			valid_flag = "1"
		}

		set += "`name`='"+name+"',"
		set += "`code`='"+code+"',"

		//公共参数
		set += "`valid_flag`='"+valid_flag+"',"
		set += set_public

		//字段提取
		field += " `id`,`name`,`code`,`created` "
		switch act {
		case "add":
			//物流类别排重
			sel_isset_login_name := "select `id` as a from `"+scope+"` where  `name`='"+name+"' and deleted=0 limit 1"
			sel_isset := models.SelsetSqlBySqlMax(sel_isset_login_name)
			if sel_isset!="0" {
				return 0,"","该名称已存在",page,id,scope,"",""
			}

			sel_isset_login_code := "select `id` as a from `"+scope+"` where  `code`='"+code+"' and deleted=0 limit 1"
			sel_isset_code := models.SelsetSqlBySqlMax(sel_isset_login_code)
			if sel_isset_code!="0" {
				return 0,"","该编码已存在",page,id,scope,"",""
			}

			b2 := time.Now().UnixNano()
			id = strconv.FormatInt(b2,10)
			set += "`id`= "+id+","
			set += "`order_id`= 0,"
			set += "`deleted`= 0,"
			set += "`syn_time`= now(),"
			set += "`created`= now(),"
			set += "`updated`= now()"
			sql_exc += "INSERT INTO "+scope+" set "+set;

			return 2,sql_exc,"添加物品名称",page,id,scope,"","系统管理,物料类别,添加,添加物料类别:"+code+","+user_id+","+opt_time_log+","+user_org_id
		case "edit":
			//物流类别排重
			sel_isset_login_name := "select `id` as a from `"+scope+"` where  `name`='"+name+"' and deleted=0 limit 1"
			sel_isset := models.SelsetSqlBySqlMax(sel_isset_login_name)
			if sel_isset!="0" && sel_isset !=id {
				return 0,"","该名称已存在",page,id,scope,"",""
			}

			//检测榜单码是否存在
			sel_isset_login_code := "select `id` as a from `"+scope+"` where  `code`='"+code+"' and deleted=0 limit 1"
			sel_isset_code := models.SelsetSqlBySqlMax(sel_isset_login_code)
			if sel_isset_code!="0" && sel_isset_code!=id{
				return 0,"","该编码已存在",page,id,scope,"",""
			}


			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 158,"",err_del_msg,page,id,scope,"",""
			}

			where +=" and id = "+id;
			set += "`deleted`= 0,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set + where;

			return 3,sql_exc,"修改物品名称",page,id,scope,"","系统管理,物料类别,修改,修改物料类别:"+code+","+user_id+","+opt_time_log+","+user_org_id
		case "del":
			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 0,"",err_del_msg,page,id,scope,"",""
			}

			where +=" and id = "+id;

			set = "`deleted`= 1,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set+ where;
			if name =="" {
				sel_name := "select `name` as a from `"+scope+"` where  `id`='"+id+"'"
				name = models.SelsetSqlBySqlMax(sel_name)
			}

			return 4,sql_exc,"删除物品名称",page,id,scope,"","系统管理,物料类别,删除,删除物料类别:"+name+","+user_id+","+opt_time_log+","+user_org_id
		case "get":
			sql_exc += "SELECT "+field+" FROM "+scope
			sql_count += scope
			return 1,sql_exc,sql_count,page,id,scope,"",""
		default:
			return 0,sql_exc,"缺少act",page,id,scope,"",""
		}


	//车辆载重	sys_load_capacity
	case "sys_load_capacity":
		//归属企业id，0为共有
		org_id:=user_org_id
		set_public += "`org_id`='"+org_id+"',"

		set :=""
		//对org_info 企业信息表进行操作
		id,_ := js.Get("id").String()

		where += " where 1 "

		car_axle,_ := js.Get("car_axle").String()
		capacity,_ := js.Get("capacity").String()
		load_type,_ := js.Get("load_type").String()
		drive_axle,_ := js.Get("drive_axle").String()
		tyre_num,_ := js.Get("tyre_num").String()
		axle_layout,_ := js.Get("axle_layout").String()

		valid_flag,_ := js.Get("valid_flag").String()
		if valid_flag == "" {
			valid_flag ="1"
		}
		set += "`car_axle`='"+car_axle+"',"
		set += "`capacity`='"+capacity+"',"
		set += "`load_type`='"+load_type+"',"
		set += "`drive_axle`='"+drive_axle+"',"
		set += "`tyre_num`='"+tyre_num+"',"
		set += "`axle_layout`='"+axle_layout+"',"

		//公共参数
		set += "`valid_flag`='"+valid_flag+"',"
		set += set_public

		//字段提取
		field +=" `id`,`car_axle`,`capacity`,`created` "

		switch act {
		case "add":
			b2 := time.Now().Unix()
			id = strconv.FormatInt(b2,10)
			set += "`id`= "+id+","
			set += "`order_id`= 0,"
			set += "`deleted`= 0,"
			set += "`syn_time`= now(),"
			set += "`created`= now(),"
			set += "`updated`= now()"
			sql_exc += "INSERT INTO "+scope+" set "+set;

			return 2,sql_exc,"添加车辆载重",page,id,scope,"","系统管理,载重管理,添加,添加车轴示意:"+axle_layout+","+user_id+","+opt_time_log+","+user_org_id
		case "edit":
			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 158,"",err_del_msg,page,id,scope,"",""
			}

			where +=" and id = "+id;
			set += "`deleted`= 0,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set + where;

			return 3,sql_exc,"修改车辆载重",page,id,scope,"","系统管理,载重管理,修改,修改车轴示意:"+axle_layout+","+user_id+","+opt_time_log+","+user_org_id
		case "del":
			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 0,"",err_del_msg,page,id,scope,"",""
			}

			where +=" and id = "+id;

			set = "`deleted`= 1,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set+ where;

			if axle_layout =="" {
				sel_name := "select `axle_layout` as a from `"+scope+"` where  `id`='"+id+"'"
				axle_layout = models.SelsetSqlBySqlMax(sel_name)
			}

			return 4,sql_exc,"删除车辆载重",page,id,scope,"","系统管理,载重管理,删除,删除车轴示意:"+axle_layout+","+user_id+","+opt_time_log+","+user_org_id
		case "get":
			sql_exc += "SELECT "+field+" FROM "+scope
			sql_count += scope
			return 1,sql_exc,sql_count,page,id,scope,"",""
		default:
			return 0,sql_exc,"缺少act-sys_load_capacity-"+act,page,id,scope,"",""
		}


	//企业厂区表	org_factory
	case "org_factory":
		set :=""
		//对org_info 企业信息表进行操作
		id,_ := js.Get("id").String()

		where += " where 1 "

		factory_name,_ := js.Get("factory_name").String()
		address,_ := js.Get("address").String()
		org_id,_ := js.Get("org_id").String()
		if org_id == "" {
			org_id ="0"
		}

		valid_flag,_ := js.Get("valid_flag").String()
		if valid_flag == "" {
			valid_flag ="1"
		}

		pon_code,_ := js.Get("pon_code").String()
		if pon_code == "" {
			set += "`pon_code`= (select a.pon_code+1 from `org_factory` a ORDER BY a.pon_code DESC limit 1),"
		}else {
			set += "`pon_code`= '"+pon_code+"',"
		}

		set += "`factory_name`='"+factory_name+"',"
		set += "`address`='"+address+"',"

		//公共参数
		set += "`valid_flag`='"+valid_flag+"',"
		set += set_public

		//字段提取
		field += " `id`,`factory_name`,`address`,`created` "

		switch act {
		case "add":
			set += "`org_id`='"+org_id+"',"

			b2 := time.Now().UnixNano()
			id = strconv.FormatInt(b2,10)
			set += "`id`= "+id+","
			set += "`order_id`= 0,"
			set += "`deleted`= 0,"
			//磅单码  注册码

			set += "`local_register_id`= (select a.local_register_id+1 from `org_factory` a ORDER BY a.local_register_id DESC limit 1),"

			set += "`syn_time`= now(),"
			set += "`created`= now(),"
			set += "`updated`= now()"
			sql_exc += "INSERT INTO "+scope+" set "+set;

			return 2,sql_exc,"添加企业厂区",page,id,scope,"","企业管理,厂区管理,添加,添加厂区:"+factory_name+","+user_id+","+opt_time_log+","+user_org_id
		case "edit":
			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 158,"",err_del_msg,page,id,scope,"",""
			}

			where +=" and id = "+id;
			set += "`deleted`= 0,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set + where;

			return 3,sql_exc,"修改企业厂区",page,id,scope,"","企业管理,厂区管理,修改,修改厂区:"+factory_name+","+user_id+","+opt_time_log+","+user_org_id
		case "del":
			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 0,"",err_del_msg,page,id,scope,"",""
			}

			if factory_name ==""{
				sel_name := "select `factory_name` as a from `"+scope+"` where  `id`='"+id+"'"
				factory_name = models.SelsetSqlBySqlMax(sel_name)
			}

			where +=" and id = "+id;

			set = "`deleted`= 1,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set+ where;

			return 4,sql_exc,"删除企业厂区",page,id,scope,"","企业管理,厂区管理,删除,删除厂区:"+factory_name+";ID:"+id+","+user_id+","+opt_time_log+","+user_org_id
		case "get":
			sql_exc += "SELECT "+field+" FROM "+scope
			sql_count += scope
			return 1,sql_exc,sql_count,page,id,scope,"",""
		default:
			return 0,sql_exc,"缺少act",page,id,scope,"",""
		}


	//门岗表	org_gate
	case "org_gate":
		set :=""
		//对org_info 企业信息表进行操作
		id,_ := js.Get("id").String()

		where += " where 1 "

		factory_id,_ := js.Get("factory_id").String()
		gate_name,_ := js.Get("gate_name").String()

		valid_flag,_ := js.Get("valid_flag").String()
		if valid_flag == "" {
			valid_flag ="1"
		}
		set += "`factory_id`='"+factory_id+"',"
		set += "`gate_name`='"+gate_name+"',"

		//添加所属企业
		set += "`org_id`=(select `org_id` from `org_factory` where `id`="+factory_id+"),"
		//公共参数
		set += "`valid_flag`='"+valid_flag+"',"
		set += set_public

		//字段提取
		field += " `id`,`factory_id`,`gate_name`,`created` "
		switch act {
		case "add":
			b2 := time.Now().UnixNano()
			id = strconv.FormatInt(b2,10)
			set += "`id`= "+id+","
			set += "`order_id`= 0,"
			set += "`deleted`= 0,"
			set += "`syn_time`= now(),"
			set += "`created`= now(),"
			set += "`updated`= now()"
			sql_exc += "INSERT INTO "+scope+" set "+set;

			return 2,sql_exc,"添加门房",page,id,scope,"","企业管理,门房管理,添加,添加门房:"+gate_name+","+user_id+","+opt_time_log+","+user_org_id
		case "edit":
			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 158,"",err_del_msg,page,id,scope,"",""
			}

			where +=" and id = "+id;
			set += "`deleted`= 0,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set + where;

			return 3,sql_exc,"修改门房",page,id,scope,"","企业管理,门房管理,修改,修改门房:"+gate_name+","+user_id+","+opt_time_log+","+user_org_id
		case "del":

			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 0,"",err_del_msg,page,id,scope,"",""
			}

			where +=" and id = "+id;

			set = "`deleted`= 1,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set+ where;

			if gate_name ==""{
				sel_name := "select `gate_name` as a from `"+scope+"` where  `id`='"+id+"'"
				gate_name = models.SelsetSqlBySqlMax(sel_name)
			}

			return 4,sql_exc,"删除门房",page,id,scope,"","企业管理,门房管理,删除,删除门房:"+gate_name+";ID:"+id+","+user_id+","+opt_time_log+","+user_org_id
		case "get":
			sql_exc += "SELECT "+field+" FROM "+scope
			sql_count += scope
			return 1,sql_exc,sql_count,page,id,scope,"",""
		default:
			return 0,sql_exc,"缺少act",page,id,scope,"",""
		}


	//磅房	org_house
	case "org_house":
		//归属企业id，0为共有
		/*
		org_id,_ := js.Get("org_id").String()
		if org_id =="" {
			org_id ="0"
		}
		set_public += "`org_id`='"+org_id+"',"
		*/
		set :=""
		//对org_info 企业信息表进行操作
		id,_ := js.Get("id").String()

		where += " where 1 "

		factory_id,_ := js.Get("factory_id").String()
		house_name,_ := js.Get("house_name").String()

		valid_flag,_ := js.Get("valid_flag").String()
		if valid_flag == "" {
			valid_flag ="1"
		}
		set += "`factory_id`='"+factory_id+"',"
		set += "`house_name`='"+house_name+"',"
		//添加所属厂区
		set += "`org_id`=(select `org_id` from `org_factory` where `id`="+factory_id+"),"

		//公共参数
		set += "`valid_flag`='"+valid_flag+"',"
		set += set_public

		//字段提取
		field += " `id`,`factory_id`,`house_name`,`created` "

		switch act {
		case "add":
			b2 := time.Now().UnixNano()
			id = strconv.FormatInt(b2,10)
			set += "`id`= "+id+","
			set += "`order_id`= 0,"
			set += "`deleted`= 0,"
			set += "`syn_time`= now(),"
			set += "`created`= now(),"
			set += "`updated`= now()"
			sql_exc += "INSERT INTO "+scope+" set "+set;

			return 2,sql_exc,"添加磅房",page,id,scope,"","企业管理,磅房管理,添加,添加磅房:"+house_name+","+user_id+","+opt_time_log+","+user_org_id
		case "edit":
			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 158,"",err_del_msg,page,id,scope,"",""
			}

			where +=" and id = "+id;
			set += "`deleted`= 0,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set + where;

			return 3,sql_exc,"修改磅房",page,id,scope,"","企业管理,磅房管理,修改,修改磅房:"+house_name+","+user_id+","+opt_time_log+","+user_org_id
		case "del":
			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 0,"",err_del_msg,page,id,scope,"",""
			}

			where +=" and id = "+id;

			set = "`deleted`= 1,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set+ where;

			if house_name ==""{
				sel_name := "select `house_name` as a from `"+scope+"` where  `id`='"+id+"'"
				house_name = models.SelsetSqlBySqlMax(sel_name)
			}

			return 4,sql_exc,"删除磅房",page,id,scope,"","企业管理,磅房管理,删除,删除磅房:"+house_name+";ID:"+id+","+user_id+","+opt_time_log+","+user_org_id
		case "get":
			sql_exc += "SELECT "+field+" FROM "+scope
			sql_count += scope
			return 1,sql_exc,sql_count,page,id,scope,"",""
		default:
			return 0,sql_exc,"缺少act",page,id,scope,"",""
		}


	//合同	biz_contract
	case "biz_contract":
		//归属企业id，0为共有
		if act=="add" {
			org_id := user_org_id
			set_public += "`org_id`='"+org_id+"',"
		}

		set :=""
		//对org_info 企业信息表进行操作
		id,_ := js.Get("id").String()

		where += " where 1 "

		contract_no,_ := js.Get("contract_no").String()
		//替换 '

		biz_type,_ := js.Get("biz_type").String()
		send_unit_id,_ := js.Get("send_unit_id").String()
		load_address_id,_ := js.Get("load_address_id").String()
		receive_unit_id,_ := js.Get("receive_unit_id").String()
		unload_address_id,_ := js.Get("unload_address_id").String()
		consign_unit_id,_ := js.Get("consign_unit_id").String()
		ship_unit_id,_ := js.Get("ship_unit_id").String()
		material_id,_ := js.Get("material_id").String()
		material_goods_id,_ := js.Get("material_goods_id").String()
		start_date,_ := js.Get("start_date").String()
		end_date,_ := js.Get("end_date").String()
		contract_amount,_ := js.Get("contract_amount").String()
		control_mode,_ := js.Get("control_mode").String()

		if control_mode =="" {
			control_mode = "0"
		}
		set += "`control_mode`='"+control_mode+"',"

		valid_flag,_ := js.Get("valid_flag").String()
		if valid_flag == "" {
			valid_flag ="1"
		}

		if contract_no != ""{
			set += "`contract_no`='"+contract_no+"',"
		}else {
			sel_contract_no := "select contract_no as a from `biz_contract` where id="+id
			contract_no =models.SelsetSqlBySqlMax(sel_contract_no)
		}

		if biz_type !="" {
			set += "`biz_type`='"+biz_type+"',"
		}

		if receive_unit_id != "" {
			set += "`receive_unit_id`='"+receive_unit_id+"',"
			set += "`receive_unit_fty_id`=(select factory_id from org_client where id = "+receive_unit_id+"),"
			set += "`receive_unit_org_id`=(select org_id from org_client where id = "+receive_unit_id+"),"
		}

		if unload_address_id !="" {
			set += "`unload_address_id`='"+unload_address_id+"',"
			set += "`unload_address_fty_id`=(select factory_id from sys_address where id = "+unload_address_id+"),"
			set += "`unload_address_org_id`=(select org_id from sys_address where id = "+unload_address_id+"),"
		}

		if send_unit_id !="" {
			set += "`send_unit_id`='"+send_unit_id+"',"
			set += "`send_unit_fty_id`=(select factory_id from org_client where id = "+send_unit_id+"),"
			set += "`send_unit_org_id`=(select org_id from org_client where id = "+send_unit_id+"),"
		}

		if load_address_id != "" {
			set += "`load_address_id`='"+load_address_id+"',"
			set += "`load_address_fty_id`=(select factory_id from sys_address where id = "+load_address_id+"),"
			set += "`load_address_org_id`=(select org_id from sys_address where id = "+load_address_id+"),"
		}

		if ship_unit_id != "" {
			set += "`ship_unit_id`='"+ship_unit_id+"',"
			set += "`ship_unit_fty_id`=(select factory_id from org_client where id = "+ship_unit_id+"),"
			set += "`ship_unit_org_id`=(select org_id from org_client where id = "+ship_unit_id+"),"
		}

		if consign_unit_id != "" {
			set += "`consign_unit_id`='"+consign_unit_id+"',"
			set += "`consign_unit_fty_id`=(select factory_id from org_client where id = "+consign_unit_id+"),"
			set += "`consign_unit_org_id`=(select org_id from org_client where id = "+consign_unit_id+"),"

		}

		if material_id != "" {
			set += "`material_id`='"+material_id+"',"
			set += "`material_fty_id`=(select factory_id from sys_material where id = "+material_id+"),"
			set += "`material_org_id`=(select org_id from sys_material where id = "+material_id+"),"
		}

		if material_goods_id != "" {
			set += "`material_goods_id`='"+material_goods_id+"',"
			set += "`material_goods_fty_id`=(select factory_id from sys_material_goods where id = "+material_goods_id+"),"
			set += "`material_goods_org_id`=(select org_id from sys_material_goods where id = "+material_goods_id+"),"
		}

		if start_date != "" {
			set += "`start_date`='"+start_date+"',"
		}
		if end_date != "" {
			set += "`end_date`='"+end_date+"',"
		}
		if contract_amount !="" {
			set += "`contract_amount`='"+contract_amount+"',"
		}

		//公共参数
		//set += "`valid_flag`='"+valid_flag+"',"
		set += set_public

		//字段提取
		field += " `id`,`name`,`short_name`,`code`,`clientTypeID`,`address`,`created` "


		switch act {

		case "add":		//新增合同

			//判断合同号是否重复
			sel_is_contract_no := "select `id` as a from `biz_contract` where  `contract_no`='"+contract_no+"' and `org_id`='"+user_org_id+"' "
			is_contract_no := models.SelsetSqlBySqlMax(sel_is_contract_no)
			if is_contract_no != "0" {
				return 0,"","该合同号已存在",page,id,scope,"",""
			}

			b2 := time.Now().UnixNano()
			id = strconv.FormatInt(b2,10)
			set += "`id`= "+id+","
			set += "`deleted`= 0,"
			set += "`syn_time`= now(),"
			set += "`created`= now(),"
			set += "`status_time`= now(),"
			set += "`updated`= now()"
			sql_exc += "INSERT INTO "+scope+" set "+set;

			return 2,sql_exc,"添加合同",page,id,scope,"","合同管理,合同发布,发布,发布了合同:"+contract_no+","+user_id+","+opt_time_log+","+user_org_id

		case "edit":	//修改合同
			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 158,"",err_del_msg,page,id,scope,"",""
			}

			//判断 合同是否已经 进入终止状态
			sql_status_0 := "select `id` as a from `biz_contract` where `id`="+id+" and (`status`<>0 or  `end_date`<now() )"
			sql_status := models.SelsetSqlBySqlMax(sql_status_0)
			if sql_status != "0" {
				return 0,"","该合同已终止或完成，无法修改,请刷新",page,id,scope,"",""
			}

			if end_date =="" {
				return 0,"","时间选择为空，请检查",page,id,scope,"",""
			}
			where +=" and id = "+id;
			set = "`end_date`= '"+end_date+"',"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set + where;

			return 3,sql_exc,"修改合同",page,id,scope,"","合同管理,合同查询,修改,修改了合同:"+contract_no+","+user_id+","+opt_time_log+","+user_org_id

		case "del":		//删除合同
			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 0,"",err_del_msg,page,id,scope,"",""
			}

			//判断 合同是否已经 进入开始状态
			sql_status_0 := "select `id` as a from `biz_contract` where `id`="+id+" and `status`=0  and `start_date` < now() "
			sql_status := models.SelsetSqlBySqlMax(sql_status_0)
			if sql_status != "0" {
				return 0,"","该合同已在就行中，无法删除,请刷新",page,id,scope,"",""
			}

			where +=" and id = "+id;
			set = "`deleted`= 1,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set+ where;

			return 4,sql_exc,"删除合同",page,id,scope,"","合同管理,合同查询,删除,删除了合同:"+contract_no+","+user_id+","+opt_time_log+","+user_org_id

		case "stop":	//终止合同
			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 0,"",err_del_msg,page,id,scope,"",""
			}

			//判断是否已终止
			sel_stop := GetDeleted(scope,"id",id,"status")
			if sel_stop =="1" {
				return 0,"","该记录已终止,请刷新",page,id,scope,"",""
			}

			//判断 合同是否已经 进入终止状态
			sql_status_0 := "select `id` as a from `biz_contract` where `id`="+id+" and `status`=1  and `end_date` < now() "
			sql_status := models.SelsetSqlBySqlMax(sql_status_0)
			if sql_status != "0" {
				return 0,"","该合同已终止，请刷新",page,id,scope,"",""
			}

			where +=" and id = "+id;
			set = "`status`= 1,"
			set += "balance =(select ub.uc_balance from ( select IFNULL(uc.contract_amount,0)+ IFNULL(uc.`before_balance`,0) -IFNULL(uc.carryout_amount,0) as uc_balance from biz_contract as uc where uc.id="+id+")ub),"
			set += "`status_time`= now(),"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set+ where;

			//查询合同号
			if contract_no == "" {
				sel_contract_no := "select contract_no as a from `biz_contract` where id="+id
				contract_no =models.SelsetSqlBySqlMax(sel_contract_no)
			}

			return 4,sql_exc,"终止合同",page,id,scope,"","合同管理,合同查询,终止,终止了合同 合同号:"+contract_no+" ; ID:"+id+","+user_id+","+opt_time_log+","+user_org_id

		case "trun":	//结余合同    contract_no和当前    id是目标

			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 0,"",err_del_msg,page,id,scope,"",""
			}

			//判断是否已结余
			sel_tar_id := "select IFNULL(entry_contract_id, '0') as a from `biz_contract` where `contract_no`='"+contract_no+"'"
			sel_stop := models.SelsetSqlBySqlMax(sel_tar_id)
			fmt.Println("((((((((",sel_tar_id)
			fmt.Println("((((((((",sel_stop)
			if sel_stop !="0" {
				sel_stop_no := GetDeleted(scope,"id",sel_stop,"contract_no")
				return 0,"","该记录已结余,结余合同号:"+sel_stop_no,page,id,scope,"",""
			}

			//处理当前合同
			//处理合同
			contract_no_replace := strings.Replace(contract_no, "'", "\\'", -1)

			where +=" and contract_no = '"+contract_no_replace+"'";


			sel_balance := "select ( IFNULL(c.contract_amount,0) + IFNULL(c.before_balance,0) - IFNULL(c.carryout_amount,0) ) as a from biz_contract as c where c.contract_no='"+contract_no_replace+"'"
			balance :=models.SelsetSqlBySqlMax(sel_balance)

			fmt.Println("$$$$$$$$$$$$$$$>>>",sel_balance)
			fmt.Println("$$$$$$$$$$$$$$$>>>>>>>",balance)

			set = " balance="+balance+","
			set += "`balance_time`= now(),"
			set += "`status`= 2,"
			set += "`entry_contract_id`= "+id+","
			set += "`status_time`= now(),"
			set += "`balance_user_id`= "+user_id+","
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set+ where;

			//处理目标合同

			trun_sql_1 :="update `"+scope+"` set `before_balance` = IFNULL(`before_balance`,0) + "+balance+",updated=now() where `id`='"+id+"'"

			fmt.Println("结余目标合同>>>>>>>",trun_sql_1)
			//记录日志
			tar_id :=""
			if tar_id == "" {
				sel_tar_id := "select id as a from `biz_contract` where contract_no='"+contract_no+"'"
				tar_id =models.SelsetSqlBySqlMax(sel_tar_id)
			}
			tar_id_int,_:=strconv.Atoi(tar_id)

			return 8,sql_exc,"结余合同",tar_id_int,id,scope,trun_sql_1,"合同管理,合同查询,结余,结余了合同 合同号:"+contract_no+" ; ID:"+id+","+user_id+","+opt_time_log+","+user_org_id

		case "get":		//获取合同
			sql_exc += "SELECT "+field+" FROM "+scope
			sql_count += scope
			return 1,sql_exc,sql_count,page,id,scope,"",""

		default:	// 无 act
			return 0,sql_exc,"缺少act",page,id,scope,"",""
		}

		//客户	org_house
	case "org_client":
		//归属企业id，0为共有
		if act=="add" {
			org_id,_ := js.Get("org_id").String()
			if org_id =="" {
				org_id ="0"
			}
			set_public += "`org_id`='"+org_id+"',"
		}

		set :=""
		//对org_info 企业信息表进行操作
		id,_ := js.Get("id").String()

		where += " where 1 "

		name,_ := js.Get("name").String()
		short_name,_ := js.Get("short_name").String()
		code,_ := js.Get("code").String()
		client_type,_ := js.Get("client_type").String()
		address,_ := js.Get("address").String()

		valid_flag,_ := js.Get("valid_flag").String()
		if valid_flag == "" {
			valid_flag ="1"
		}
		set += "`name`='"+name+"',"
		set += "`short_name`='"+short_name+"',"
		set += "`code`='"+code+"',"
		set += "`client_type`='"+client_type+"',"
		set += "`address`='"+address+"',"

		//公共参数
		set += "`valid_flag`='"+valid_flag+"',"
		set += set_public

		//字段提取
		field += " `id`,`name`,`short_name`,`code`,`clientTypeID`,`address`,`created` "

		switch act {
		case "add":
			b2 := time.Now().UnixNano()
			id = strconv.FormatInt(b2,10)
			set += "`id`= "+id+","
			set += "`order_id`= 0,"
			set += "`deleted`= 0,"
			set += "`syn_time`= now(),"
			set += "`created`= now(),"
			set += "`updated`= now()"
			sql_exc += "INSERT INTO "+scope+" set "+set;

			return 2,sql_exc,"添加客户",page,id,scope,"","企业管理,客户管理,添加,添加了客户:"+name+","+user_id+","+opt_time_log+","+user_org_id
		case "edit":
			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 158,"",err_del_msg,page,id,scope,"",""
			}

			where +=" and id = "+id;
			set += "`deleted`= 0,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set + where;

			return 3,sql_exc,"修改客户",page,id,scope,"","企业管理,客户管理,修改,修改了客户:"+name+","+user_id+","+opt_time_log+","+user_org_id
		case "del":
			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"id",id,"deleted")
			if sel_deleted =="1" {
				return 0,"",err_del_msg,page,id,scope,"",""
			}

			where +=" and id = "+id;
			set = "`deleted`= 1,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set+ where;

			sel_name := "select `name` as a from `"+scope+"` where  `id`='"+id+"'"
			name = models.SelsetSqlBySqlMax(sel_name)

			return 4,sql_exc,"删除客户",page,id,scope,"","企业管理,客户管理,删除,删除了客户:"+name+","+user_id+","+opt_time_log+","+user_org_id
		case "get":
			sql_exc += "SELECT "+field+" FROM "+scope
			sql_count += scope
			return 1,sql_exc,sql_count,page,id,scope,"",""
		default:
			return 0,sql_exc,"缺少act",page,id,scope,"",""
		}

	//管理员账户 admin_user
	case "admin_user":
		set :=""
		//归属企业id，0为共有
		org_id,_ := js.Get("org_id").String()
		if org_id =="" {
			org_id ="0"
		}

		id,_ := js.Get("id").String()

		where += " where 1 "

		login_name,_ := js.Get("login_name").String()
		if login_name == "" {
			sel_login_name := "select `login_name` as a from `"+scope+"` where  `user_id`='"+id+"'"
			login_name = models.SelsetSqlBySqlMax(sel_login_name)
		}
		user_name,_ := js.Get("user_name").String()
		password,_ := js.Get("password").String()
		factory_id,_ := js.Get("factory_id").String()
		enabled_value,_ := js.Get("enabled_value").String()
		if enabled_value == "" {
			enabled_value = "0"
		}
		password = GetPasswd(password)

		set += "`user_name`='"+user_name+"',"
		set += "`login_name`='"+login_name+"',"
		set += "`password`='"+password+"',"
		set += "`mobile`='"+password+"',"
		set += "`org_id`='"+org_id+"',"

		if factory_id == "" {
			set += "`team_id`=0,"
		}else {
			set += "`team_id`='"+factory_id+"',"
		}

		switch act {
		case "add":
			//登录用户名排重
			sel_isset_login_name := "select `user_id` as a from `"+scope+"` where  `login_name`='"+login_name+"' and deleted=0 limit 1"
			sel_isset := models.SelsetSqlBySqlMax(sel_isset_login_name)
			if sel_isset !="0" {
				return 0,"","该账户已存在",page,id,scope,"",""
			}

			b2 := time.Now().UnixNano()
			id = strconv.FormatInt(b2,10)
			set += "`user_id`= "+id+","
			set += "`enabled`= 1,"
			set += "`order_id`= 1,"
			set += "`email`= '',"

			set += "`syn_flag`= 1,"		//0不同步、1同步、2新增同步、3编辑同步、4删除同步
			set += "`syn_status`= 0,"	//0初始值 、1正同步中、2 同步成功、3 同步失败
			set += "`share_flag`= 0,"	//0私有、1集团共享、2平台共享
			set += "`project`= 1,"	//项目ID
			set += "`deleted`= 0,"	//删除标志
			set += "`syn_time`= now(),"

			set += "`created`= now(),"
			set += "`updated`= now()"
			sql_exc += "INSERT INTO "+scope+" set "+set;

			return 2,sql_exc,"添加账户",page,id,scope,"","用户管理,用户管理,添加,添加了用户:"+login_name+","+user_id+","+opt_time_log+","+user_org_id
		case "reset":

			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"user_id",id,"deleted")
			if sel_deleted =="1" {
				return 0,"",err_del_msg,page,id,scope,"",""
			}

			where := " where 1 "
			where +=" and `user_id` = "+id;
			set := "`password`= '"+GetPasswd("")+"',"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set + where;

			return 5,sql_exc,"重置密码",page,id,scope,"","用户管理,用户管理,重置密码,重置:"+login_name+","+user_id+","+opt_time_log+","+user_org_id
		case "edit":

			//登录用户名排重
			sel_isset_login_name := "select `user_id` as a from `"+scope+"` where  `login_name`='"+login_name+"' and deleted=0 limit 1"
			sel_isset := models.SelsetSqlBySqlMax(sel_isset_login_name)
			if sel_isset!="0" && sel_isset !=id {
				return 0,"","该用户名已存在",page,id,scope,"",""
			}

			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"user_id",id,"deleted")
			if sel_deleted =="1" {
				return 158,"",err_del_msg,page,id,scope,"",""
			}

			where := " where 1 "
			where +=" and `user_id` = "+id;
			set := "`login_name`= '"+login_name+"',"
			set += "`user_name`= '"+user_name+"',"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set + where;

			return 5,sql_exc,"修改用户名",page,id,scope,"","用户管理,用户管理,修改,修改用户名:"+login_name+","+user_id+","+opt_time_log+","+user_org_id
		case "lock":
			//判断是否已删除
			sel_plan_id := "select `enabled` as a from `"+scope+"` where  `user_id`="+id
			sel_enabled := models.SelsetSqlBySqlMax(sel_plan_id)

			fmt.Println("sel_enabled >>",sel_enabled)
			fmt.Println("enabled_value >>",enabled_value)
			if sel_enabled == enabled_value {
				if enabled_value == "0" {
					return 0,"","该账户已被锁定",page,id,scope,"",""
				}else {
					return 0,"","该账户已被解锁",page,id,scope,"",""
				}

			}

			//判断是否已锁定
			sel_deleted:= GetDeleted(scope,"user_id",id,"deleted")
			if sel_deleted =="1" {
				return 0,"","该账户已被删除",page,id,scope,"",""
			}

			where := " where 1 "
			where +=" and `user_id` = "+id;
			set := "`enabled`= '"+enabled_value+"',"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set + where;
			if enabled_value == "0" {
				return 5,sql_exc,"禁用账户",page,id,scope,"","用户管理,用户管理,禁用,锁定:"+login_name+","+user_id+","+opt_time_log+","+user_org_id
			}else {
				return 5,sql_exc,"启用账户",page,id,scope,"","用户管理,用户管理,解锁,解锁:"+login_name+","+user_id+","+opt_time_log+","+user_org_id
			}
		case "del":
			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"user_id",id,"deleted")
			if sel_deleted =="1" {
				return 0,"",err_del_msg,page,id,scope,"",""
			}

			where := " where 1 "
			where +=" and `user_id` = "+id;
			set := "`deleted`= 1,"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set + where;
			return 6,sql_exc,"删除账户",page,id,scope,"","用户管理,用户管理,删除,删除:"+login_name+","+user_id+","+opt_time_log+","+user_org_id

		case "change":
			//判断是否已删除
			sel_deleted:= GetDeleted(scope,"user_id",id,"deleted")
			if sel_deleted =="1" {
				return 0,"",err_del_msg,page,id,scope,"",""
			}

			where := " where 1 "
			where +=" and `user_id` = "+id;
			set := "`password`= '"+password+"',"
			set += "`updated`= now()"
			sql_exc += "UPDATE "+scope+" set "+set + where;

			return 5,sql_exc,"修改密码",page,id,scope,"","用户管理,用户管理,修改,修改密码:"+login_name+","+user_id+","+opt_time_log+","+user_org_id
		default:
			fmt.Println(scope)
			return 0,sql_exc,"缺少act",page,id,scope,"",""
		}


		//磅单	biz_ponderation
	case "biz_ponderation":
		//权限判断
		if org_type == "1" {
			return 0,"","集团账户无权限",page,"",scope,"",""
		}
		//归属企业id，0为共有
		set_public += "`org_id`='"+user_org_id+"',"

		set :=""
		//对org_info 企业信息表进行操作
		id :=""

		where += " where 1 "

		into_factory,_ := js.Get("into_factory").String()
		out_factory,_ := js.Get("out_factory").String()
		factory_id,_ := js.Get("factory_id").String()
		gate_id,_ := js.Get("gate_id").String()
		tare_house_id,_ := js.Get("tare_house_id").String()
		act_type,_ := js.Get("act_type").String()
		consign_unit_id,_ := js.Get("consign_unit_id").String()
		ship_unit_id,_ := js.Get("ship_unit_id").String()
		load_address_id,_ := js.Get("load_address_id").String()
		unload_address_id,_ := js.Get("unload_address_id").String()
		material_id,_ := js.Get("material_id").String()
		material_goods_id,_ := js.Get("material_goods_id").String()
		contract_id,_ := js.Get("contract_id").String()
		trade_order_id,_ := js.Get("trade_order_id").String()
		gross_weight,_ := js.Get("gross_weight").String()
		tare,_ := js.Get("tare").String()
		net_weight,_ := js.Get("net_weight").String()
		rebate_weight,_ := js.Get("rebate_weight").String()
		collect_weight,_ := js.Get("collect_weight").String()
		former_net_weight,_ := js.Get("former_net_weight").String()
		biz_car_id,_ := js.Get("car_id").String()
		pon_no,_ := js.Get("pon_no").String()
		weighman,_ := js.Get("weighman").String()
		car_no,_ := js.Get("car_no").String()
		send_unit_id,_ := js.Get("send_unit_id").String()
		receive_unit_id,_ := js.Get("receive_unit_id").String()
		//检测是否新车
		if biz_car_id == "" && car_no !=""{

			//在次检查车辆是否存在
			sel_is_trade := "select `id` as a from `biz_car` where  `car_no`='"+car_no+"' limit 1 "
			sel_car_id := models.SelsetSqlBySqlMax(sel_is_trade)
			if sel_car_id != "0" {
				//车辆已存在，提取ID
				biz_car_id = sel_car_id

			}else {

				b2 := time.Now().UnixNano()
				car_id := strconv.FormatInt(b2,10)

				axle_num,_ := js.Get("axle_num").String()
				insert_car_sql :="insert into `biz_car` set id="+car_id+",rfid_type='0',car_no='"+car_no+"' , sys_load_capacity_id = '"+axle_num+"',share_flag='3',syn_flag='2',syn_status='0',user_id='1',org_id='"+sel_user_org_id+"'"

				models.PostSys(insert_car_sql)

				fmt.Println("磅单补录..>",insert_car_sql)
				//发送同步数据
				go queue.ActWebApiSendMessage(car_id ,"biz_car" ,"add" ,"")

				biz_car_id = car_id

			}



			set += "`rfid`=0,"
			set += "`rfid_type`=0,"
		}else {
			set += "`rfid`=0,"
			set += "`rfid_type`=0,"
		}

		//set += "`user_id`="+user_id+","
		//set += "`org_id`="+user_org_id+","

		if into_factory != "" {set += "`into_factory`='"+into_factory+"',"}
		if out_factory != "" {set += "`out_factory`='"+out_factory+"',"}
		if factory_id != "" {set += "`factory_id`='"+factory_id+"',"}
		if gate_id != "" {set += "`gate_id`='"+gate_id+"',"}
		if tare_house_id != "" {set += "`tare_house_id`='"+tare_house_id+"',"}
		if act_type != "" {set += "`act_type`='"+act_type+"',"}
		if consign_unit_id != "" {set += "`consign_unit_id`='"+consign_unit_id+"',"}
		if ship_unit_id != "" {set += "`ship_unit_id`='"+ship_unit_id+"',"}
		if load_address_id != "" {set += "`load_address_id`='"+load_address_id+"',"}
		if unload_address_id != "" {set += "`unload_address_id`='"+unload_address_id+"',"}
		if material_id != "" {set += "`material_id`='"+material_id+"',"}
		if material_goods_id != "" {set += "`material_goods_id`='"+material_goods_id+"',"}
		if contract_id != "" {set += "`contract_id`='"+contract_id+"',"}
		if trade_order_id != "" {set += "`trade_order_id`='"+trade_order_id+"',"}
		if gross_weight != "" {set += "`gross_weight`='"+gross_weight+"',"}
		if tare != "" {set += "`tare`='"+tare+"',"}
		if net_weight != "" {set += "`net_weight`='"+net_weight+"',"}
		if rebate_weight != "" {set += "`rebate_weight`='"+rebate_weight+"',"}
		if collect_weight != "" {set += "`collect_weight`='"+collect_weight+"',"}
		if former_net_weight != "" {set += "`former_net_weight`='"+former_net_weight+"',"}
		if biz_car_id != "" {set += "`biz_car_id`='"+biz_car_id+"',"}
		if pon_no != "" {set += "`pon_no`='"+pon_no+"',"}
		if weighman != "" {set += "`weighman`='"+weighman+"',"}
		if send_unit_id != "" {set += "`send_unit_id`='"+send_unit_id+"',"}
		if receive_unit_id != "" {set += "`receive_unit_id`='"+receive_unit_id+"',"}

		valid_flag,_ := js.Get("valid_flag").String()
		if valid_flag == "" {
			valid_flag ="1"
		}

		set += "`new_flag`='2',"
		set += "`print_count`='0',"
		set += "`status`='5',"

		fmt.Println("444>>")
		//查询 计划号

		//计划号
		/*
		plan_id := ""
		if contract_id != ""{
			sel_plan_id := "select max(IFNULL(`plan_id`,0))+1 as a from biz_ponderation where  contract_id="+contract_id
			plan_id = models.SelsetSqlBySqlMax(sel_plan_id)
		}else {
			plan_id = "1"
		}

		set += "`plan_id`='"+plan_id+"',"
		*/

		/*
		if receive_unit_id != "" {
			set += "`receive_unit_id`='"+receive_unit_id+"',"
			set += "`receive_unit_fty_id`=(select factory_id from org_client where id = "+receive_unit_id+"),"
			set += "`receive_unit_org_id`=(select org_id from org_client where id = "+receive_unit_id+"),"
		}*/
		//其他无法获取参数

		//公共参数
		//set += "`valid_flag`='"+valid_flag+"',"
		set += set_public

		//字段提取
		field += " `id`,`name`,`short_name`,`code`,`clientTypeID`,`address`,`created` "


		switch act {

		case "correction":		//纠错

			cont_id,_ := js.Get("cont_id").String()
			pond_id,_ := js.Get("pond_id").String()

			//验证合同是否有效
			if cont_id =="" || pond_id=="" {
				return 0,"","合同或磅单ID不正确",page,id,scope,"",""
			}

			//判断磅单是否已删除
			sel_deleted:= GetDeleted(scope,"id",pond_id,"deleted")
			if sel_deleted =="1" {
				return 0,"","该磅单已删除",page,id,scope,"",""
			}

			//判断合同是否已删除
			sel_deleted_cont:= GetDeleted(scope,"id",cont_id,"deleted")
			if sel_deleted_cont =="1" {
				return 0,"",err_del_msg,page,cont_id,scope,"",""
			}

			//判断 合同是否已经 进入终止状态
			sql_status_0 := "select `id` as a from `biz_contract` where `id`="+cont_id+" and (`status`<>0 or  `end_date`<now() )"
			sql_status := models.SelsetSqlBySqlMax(sql_status_0)
			if sql_status != "0" {
				return 0,"","所选合同已终止或完成",page,cont_id,scope,"",""
			}

			//查询磅单内容
			sql_pond :="select * from `biz_ponderation` where `id`='"+pond_id+"'"
			sql_pond_rowsAffected := models.SelsetSqlBySqlSliceOne(sql_pond)
			fmt.Println("^^^^^^^1>",sql_pond)

			pond_b, _ := json.Marshal(sql_pond_rowsAffected)
			pond_js, _ := simplejson.NewJson(pond_b)

			//查询磅单类型 0装货、1卸货
			p_act_type,_ := pond_js.Get("act_type").String()
			pon_no,_ := pond_js.Get("pon_no").String()
			old_contract_id,_ := pond_js.Get("contract_id").String()

			if cont_id ==  old_contract_id{
				return 0,"","当前磅单已属于该合同，请核对后处理",page,id,scope,"",""
			}
			//毛重重量
			/*
			p_gross_weight,_ := pond_js.Get("gross_weight").String()
			//实收净重重量
			p_net_weight,_ := pond_js.Get("net_weight").String()
			//原发净重重量
			p_former_net_weight,_ := pond_js.Get("former_net_weight").String()
			//扣减重量
			p_rebate_weight,_ := pond_js.Get("rebate_weight").String()
			//实收重量
			*/

			// 0装货 装货磅单用 净重   ; 1卸货 卸货磅单用实收重量
			p_collect_weight :=""
			p_collect_weight_float:=0.00
			fmt.Println("^^^^^^^^^^^^^^^^^^^^^^1.0",p_act_type)
			if p_act_type == "1" {
				p_collect_weight,_ = pond_js.Get("collect_weight").String()
				p_collect_weight_float, _ = strconv.ParseFloat(p_collect_weight, 32)
				fmt.Println("^^^^1.3",p_collect_weight)
				fmt.Println("^^^^1.4",p_collect_weight_float)
			}else {

				p_collect_weight,_ = pond_js.Get("net_weight").String()
				p_collect_weight_float, _ = strconv.ParseFloat(p_collect_weight, 32)
				fmt.Println("^^^^1.1",p_collect_weight)
				fmt.Println("^^^^1.2",p_collect_weight_float)
			}

			if p_collect_weight_float <= 0 {
				fmt.Println("^^^^&&&&&&&&&&&&&&&&&&&&&&&&&&&&1.2",p_collect_weight_float)
			}

			//查询合同内容
			sql_cont :="select `contract_no`,`consign_unit_id`,`ship_unit_id`,`send_unit_id`,`load_address_id`,`receive_unit_id`," +
				"`unload_address_id`,`material_id`,`material_goods_id`," +
					"IFNULL(contract_amount,0) as contract_amount,IFNULL(before_balance,0) as before_balance," +
						"IFNULL(carryout_amount,0) as carryout_amount from biz_contract where `id`='"+cont_id+"'"
			fmt.Println("^^^^^^^2>",sql_cont)
			sql_cont_rowsAffected := models.SelsetSqlBySqlSliceOne(sql_cont)

			cont_b, _ := json.Marshal(sql_cont_rowsAffected)
			cont_js, _ := simplejson.NewJson(cont_b)

			consign_unit_id,_ := cont_js.Get("consign_unit_id").String()
			ship_unit_id,_ := cont_js.Get("ship_unit_id").String()
			send_unit_id,_ := cont_js.Get("send_unit_id").String()
			load_address_id,_ := cont_js.Get("load_address_id").String()
			receive_unit_id,_ := cont_js.Get("receive_unit_id").String()
			unload_address_id,_ := cont_js.Get("unload_address_id").String()
			material_id,_ := cont_js.Get("material_id").String()
			material_goods_id,_ := cont_js.Get("material_goods_id").String()
			contract_no,_ := cont_js.Get("contract_no").String()
			
			//合同吨数
			contract_amount,_ := cont_js.Get("contract_amount").String()
			contract_amount_float, _ := strconv.ParseFloat(contract_amount, 32)
			//前期结余
			before_balance,_ := cont_js.Get("before_balance").String()
			before_balance_float, _ := strconv.ParseFloat(before_balance, 32)
			//已承运吨数
			carryout_amount,_ := cont_js.Get("carryout_amount").String()
			carryout_amount_float, _ := strconv.ParseFloat(carryout_amount, 32)

			//合同可用吨数
			cont_amount := contract_amount_float + before_balance_float - carryout_amount_float
			//判断合同 余量是否能达到要求
			if p_act_type =="0" {
				if p_collect_weight_float > cont_amount {
					//余量不足
					return 0,"","合同余量不足，请核对",page,id,scope,"",""
				}
			}
			
			//更新合同 已承运吨数

			cont_carryout_amount := carryout_amount_float + p_collect_weight_float
			cont_carryout_amount_string := strconv.FormatFloat(cont_carryout_amount, 'f', -1, 32)

			fmt.Println("^^^^2.1",carryout_amount_float)
			fmt.Println("^^^^2.2",p_collect_weight_float)
			fmt.Println("^^^^2.3",cont_carryout_amount_string)

			sql_update_cont := "UPDATE `biz_contract` set `carryout_amount`='"+cont_carryout_amount_string+"', `updated`=now() where id="+cont_id
			fmt.Println("^^^^^^^3>",sql_update_cont)
			models.PostSys(sql_update_cont)
			//下发
			queue.ActWebApiSendMessage(cont_id ,"biz_contract" ,"edit" ,"")
			
			//更新旧合同已承运吨数
			//查询旧合同内容
			sql_cont_old :="select `contract_no`,`consign_unit_id`,`ship_unit_id`,`send_unit_id`,`load_address_id`,`receive_unit_id`," +
				"`unload_address_id`,`material_id`,`material_goods_id`," +
				"IFNULL(contract_amount,0) as contract_amount,IFNULL(before_balance,0) as before_balance," +
				"IFNULL(carryout_amount,0) as carryout_amount from biz_contract where `id`='"+old_contract_id+"'"
			fmt.Println("^^^^^^^2>",sql_cont)
			sql_cont_old_rowsAffected := models.SelsetSqlBySqlSliceOne(sql_cont_old)

			cont_b_old, _ := json.Marshal(sql_cont_old_rowsAffected)
			cont_js_old, _ := simplejson.NewJson(cont_b_old)

			carryout_amount_old,_ := cont_js_old.Get("carryout_amount").String()
			contract_no_old,_ := cont_js_old.Get("contract_no").String()
			carryout_amount_old_float, _ := strconv.ParseFloat(carryout_amount_old, 32)
			cont_carryout_amount_old := carryout_amount_old_float - p_collect_weight_float
			cont_carryout_amount_old_string := strconv.FormatFloat(cont_carryout_amount_old, 'f', -1, 32)
			sql_update_cont_old := "UPDATE `biz_contract` set `carryout_amount`='"+cont_carryout_amount_old_string+"', `updated`=now() where id="+old_contract_id
			fmt.Println("^^^^^^^5>",sql_update_cont_old)
			models.PostSys(sql_update_cont_old)
			//下发
			queue.ActWebApiSendMessage(old_contract_id ,"biz_contract" ,"edit" ,"")
			
			//更新磅单信息 托运单位，2、承运单位，3、发货单位，4、装货地址，5、收货单位，6、卸货地址，7、物料类别、8、物料名称，9、合同号。】
			up_set :=""
			
			up_set +=" `consign_unit_id`='"+consign_unit_id+"'," //托运单位
			up_set +=" `ship_unit_id`='"+ship_unit_id+"'," //承运单位
			up_set +=" `send_unit_id`='"+send_unit_id+"'," //发货单位
			up_set +=" `load_address_id`='"+load_address_id+"'," //装货地址
			up_set +=" `receive_unit_id`='"+receive_unit_id+"'," //收货单位
			up_set +=" `unload_address_id`='"+unload_address_id+"'," //卸货地址
			up_set +=" `material_id`='"+material_id+"'," //物料类别
			up_set +=" `material_goods_id`='"+material_goods_id+"'," //物料名称
			up_set +=" `contract_id`='"+cont_id+"'," //合同ID
			up_set +=" `updated`=now()"//updated
			sql_exc_bk := "UPDATE `biz_ponderation_bk` SET "+up_set+" where `id`="+pond_id
			fmt.Println("^^^^^^^4>",sql_exc_bk)
			models.PostSys(sql_exc_bk)

			sql_exc := "UPDATE `biz_ponderation` SET "+up_set+" where `id`="+pond_id
			fmt.Println("^^^^^^^6>",sql_exc)
			
			return 2,sql_exc,"纠错",page,pond_id,scope,"","磅单管理,磅单管理,纠错,纠错:新合同号:"+contract_no+"磅单号:"+pon_no+"旧合同号:"+contract_no_old+","+user_id+","+opt_time_log+","+user_org_id

		case "repair":		//磅单补录

			//在次判断磅单号是否重复
			//判断合同号是否重复
			sel_is_pond_no := "select `id` as a from `biz_ponderation` where  `pon_no`='"+pon_no+"' "
			is_pond_no := models.SelsetSqlBySqlMax(sel_is_pond_no)
			if is_pond_no != "0" {
				return 0,"","该磅单号已存在，请核对或更改时间",page,id,scope,"",""
			}

			//派车证号排重
			sel_is_trade := "select `id` as a from `biz_ponderation` where  `trade_order_id`='"+trade_order_id+"' limit 1 "
			is_trade := models.SelsetSqlBySqlMax(sel_is_trade)
			if is_trade != "0" {
				return 0,"","派车证号已存在",page,id,scope,"",""
			}

			sel_is_trade_bk := "select `id` as a from `biz_ponderation_bk` where  `trade_order_id`='"+trade_order_id+"' limit 1 "
			is_trade_bk := models.SelsetSqlBySqlMax(sel_is_trade_bk)
			if is_trade_bk != "0" {
				return 0,"","派车证号已存在",page,id,scope,"",""
			}

			//检查车辆是否存在


			b2 := time.Now().UnixNano()
			id = strconv.FormatInt(b2,10)
			set += "`id`= "+id+","
			set += "`deleted`= 0,"
			set += "`syn_time`= now(),"
			set += "`created`= now(),"
			set += "`updated`= now()"
			sql_exc += "INSERT INTO "+scope+" set "+set;

			//扣减合同里的吨数

			//查询合同
			sql_amount := "select ifnull(contract_amount,0)+ifnull(before_balance,0) -ifnull(carryout_amount,0) as a from biz_contract where `id`="+contract_id
			sel_amount := models.SelsetSqlBySqlMax(sql_amount)

			sel_amount_float, _ := strconv.ParseFloat(sel_amount, 32)

			if act_type =="0" {	//装货磅单判断合同余量

				//磅单 净重 net_weight
				sel_pond_amount_float, _ := strconv.ParseFloat(net_weight, 32)

				if sel_pond_amount_float > sel_amount_float {
					//余量不足
					return 0,"","该装货磅单净重超过了合同所能承运的吨数，请核对",page,id,scope,"",""
				}

				//查看合同吨数 并 更改状态
				set_cont :=""



				//查询合同已承运吨数
				sql_carryout_amount := "select ifnull(carryout_amount,0) as a from biz_contract where `id`="+contract_id
				sel_carryout_amount := models.SelsetSqlBySqlMax(sql_carryout_amount)

				sel_carryout_amount_float, _ := strconv.ParseFloat(sel_carryout_amount, 32)

				carryout_amount := sel_carryout_amount_float + sel_pond_amount_float

				carryout_amount_string := strconv.FormatFloat(carryout_amount, 'f', -1, 32)


				//查询合同约定数量
				sql_contract_amount := "select ifnull(contract_amount,0)+ifnull(before_balance,0) as a from biz_contract where `id`="+contract_id
				sel_contract_amount := models.SelsetSqlBySqlMax(sql_contract_amount)

				sel_contract_amount_float, _ := strconv.ParseFloat(sel_contract_amount, 32)

				if carryout_amount > sel_contract_amount_float{
					set_cont += " `status`=1, "
				}else if (carryout_amount==sel_contract_amount_float) {
					set_cont += " `status`=2, "
				}

				sql_exc_cont := "UPDATE `biz_contract` set `carryout_amount`='"+carryout_amount_string+"', "+set_cont+" `updated`=now() where `id`="+contract_id
				models.PostSys(sql_exc_cont)
				fmt.Println("磅单补录，更改合同1>>>>>>>",sql_exc_cont)

			}else {//卸货磅单 对于进行中的合同， 如超量 变成完成

				//磅单 实收重量 collect_weight
				pond_collect_weight, _ := strconv.ParseFloat(collect_weight, 32)

				set_cont :=""
				/*
				if pond_collect_weight > sel_amount_float {
					//超过量  变更合同状态
					set_cont += " `status`=2, "
				}
				*/

				//查询合同已承运吨数
				sql_carryout_amount := "select ifnull(carryout_amount,0) as a from biz_contract where `id`="+contract_id
				sel_carryout_amount := models.SelsetSqlBySqlMax(sql_carryout_amount)

				sel_carryout_amount_float, _ := strconv.ParseFloat(sel_carryout_amount, 32)
				carryout_amount := sel_carryout_amount_float + pond_collect_weight

				//查询合同约定数量
				sql_contract_amount := "select ifnull(contract_amount,0)+ifnull(before_balance,0) as a from biz_contract where `id`="+contract_id
				sel_contract_amount := models.SelsetSqlBySqlMax(sql_contract_amount)

				sel_contract_amount_float, _ := strconv.ParseFloat(sel_contract_amount, 32)



				//卸货类型的合同，补入磅单，补入超了合同状态变为已终止
				//卸货合同如果恰恰好达到合同量，则合同状态变为已完成
				if carryout_amount > sel_contract_amount_float{
					set_cont += " `status`=1, "
				}else if (carryout_amount == sel_contract_amount_float) {
					set_cont += " `status`=2, "
				}

				//已承运吨数
				carryout_amount_string := strconv.FormatFloat(carryout_amount, 'f', -1, 32)

				set_cont += " `carryout_amount`='"+carryout_amount_string+"', "

				sql_exc_cont := "UPDATE `biz_contract` set "+set_cont+" `updated`=now() where `id`="+contract_id
				models.PostSys(sql_exc_cont)
				fmt.Println("磅单补录，更改合同2>>>>>>>",sql_exc_cont)
			}

			//写入备份库
			sql_exc_bk := "INSERT INTO `biz_ponderation_bk` set "+set
			models.PostSys(sql_exc_bk)

			fmt.Println("磅单补录..>",sql_exc_bk)

			//发送同步数据
			//queue.ActWebApiSendMessage(id ,"biz_ponderation_bk" ,"add" ,"")
			//写入备份库结束

			return 2,sql_exc,"磅单补录",page,id,scope,"","磅单管理,磅单管理,补录,补录磅单:"+pon_no+","+user_id+","+opt_time_log+","+user_org_id



		case "get":		//获取合同
			sql_exc += "SELECT "+field+" FROM "+scope
			sql_count += scope
			return 1,sql_exc,sql_count,page,id,scope,"",""

		default:	// 无 act
			return 0,sql_exc,"缺少act",page,id,scope,"",""
		}

	default:
		return 0,sql_exc,"act参数不对",page,"0",scope,"",""

	}

	return 0,sql_exc,"参数不对",page,"0",scope,"",""
}

//保存数据
func PostSys(w http.ResponseWriter,api_params map[string]string,scope string,user_id string,org_type string)  {

	//对数据进行解析
	//code        1-get   2-add		3-edit		4-del
	//sys_log  操作日志
	res,sql_exl,msg,p,SqlID,scope_table,err_msg,sys_log :=GetFieldByTable(api_params,scope,user_id,org_type)

	if res == 0 || res == 158 {//查询类消息直接输出
		cnnJson := make(map[string]interface{})

		resuls := make([]interface{}, 0)

		if res == 158 {
			cnnJson["error"] = "158"
		}else {
			cnnJson["error"] = "159"
		}

		cnnJson["errorMsg"] = msg
		cnnJson["results"] = resuls

		b, _ := json.Marshal(cnnJson)
		cnnn := string(b)

		fmt.Fprintf(w,cnnn)
	}else {//更改类消息 返回消息 ／ 发送Kafka
		//string, string,interface{}
		var  code string
		var  errMsg string
		var  resuls interface{}

		//根据res执行sql 1 为查询类的  其他为更改数据 进行数据库操作

		if res ==1{
			code,errMsg,resuls = models.GetSys(sql_exl,msg,p)
			msg =""
		}else {
			code,errMsg,resuls = models.PostSys(sql_exl)
		}

		//打印sql语句
		fmt.Println(sql_exl)
		if code == "0" {//对于执行成功的消息发送Kafka
			cnnJson := make(map[string]interface{})
			cnnJson["error"] = code
			cnnJson["errorMsg"] = msg+errMsg
			cnnJson["results"] = resuls

			b, _ := json.Marshal(cnnJson)
			cnnn := string(b)

			//对于查询的数据直接输出，对于更改数据的发送Kafka消息
			if res ==1{
				//接口输出
				Out(w,cnnn)
			}else {
				//接口输出http
				go Out(w,cnnn)
				//如果是厂区表 org_factory 则再去给 sys_init 写库
				if scope_table == "org_factory" {
					sys_init_sql :="insert into `sys_init` set `id`="+SqlID+",`local_register_id`=(select `local_register_id` from `org_factory` where `id`="+SqlID+"),`factory_id`="+SqlID+",`init_flag`=0,init_date=now(),syn_flag=0,`syn_status`=0,`syn_time`=now(),`share_flag`=0,`user_id`=1,`org_id`=(select `org_id` from `org_factory` where `id`="+SqlID+"),`order_id`=0,`deleted`=0,`updated`=now(),`created`=now()"
					fmt.Println("新加入厂区>>>>>>>>",sys_init_sql)
					models.PostSys(sys_init_sql)
				}

				var handle  string
				switch res {
				case 2:
					handle="add"
				case 3:
					handle="edit"
				case 4:
					handle="edit"
				case 5:
					handle="edit"
				case 6:
					handle="edit"
					//删除缓存 cookie
				case 8:
					handle="edit"
					//合同结余
					if err_msg != "" {
						models.PostSys(err_msg)
						//下发
						p_id:=strconv.Itoa(p)
						queue.ActWebApiSendMessage(p_id ,scope_table ,handle ,"")
					}
				case 11:
					//针对 父类磅单码修改子类跟随修改
					handle="edit"

					sql_select :="SELECT `id`,`pon_code` FROM `org_info` where `parent_id` ="+SqlID
					resultRows,_ := models.SelectSqlToStringMap(sql_select)
					if len(resultRows) >0 {
						for _,v := range resultRows{
							b, _ := json.Marshal(v)
							js, _ := simplejson.NewJson(b)
							child_id,_ := js.Get("id").String()

							update_child_sql :="UPDATE `org_info` set `pon_code`=(select t.co FROM((select aa.`pon_code` as co FROM `org_info` as aa where aa.id="+SqlID+") t)) where `id`="+child_id
							models.PostSys(update_child_sql)

							fmt.Println("子类..>",update_child_sql)
							//发送同步数据
							queue.ActWebApiSendMessage(child_id ,"org_info" ,handle ,"")
						}
					}


				default:
					handle="edit"
				}

				queue.ActWebApiSendMessage(SqlID ,scope_table ,handle ,"")

			}

			//存储 日志信息
			SaveSysLog(sys_log)

		}else {//对于执行失败的消息 输出http
			cnnJson := make(map[string]interface{})
			cnnJson["error"] = code
			cnnJson["errorMsg"] = msg+errMsg+err_msg
			cnnJson["results"] = resuls
			b, _ := json.Marshal(cnnJson)
			cnnn := string(b)
			//返回http
			Out(w,cnnn)
		}

	}


}

//判断记录是否已删除
func GetDeleted(table string,field string,value string,target string) (string) {
	if target =="" {
		target = "deleted"
	}

	res := "0"
	if table=="" || field=="" ||  value==""{
		return res
	}
	sel_plan_id := "select "+target+" as a from `"+table+"` where  "+field+"="+value
	res = models.SelsetSqlBySqlMax(sel_plan_id)

	return res

}

//存储日志信息
func SaveSysLog(sys_log string)  {
	if sys_log ==""{
		//fmt.Println("空...")
		//对于空信息不作为处理
	}else {
		sys_log_arr :=strings.Split(sys_log, ",")

		// 磅单管理,磅单管理,磅单发布,发布了磅单:BK8686443,1,2018-04-09 16:38:14
		// 0-module_name 模块名称		1-sub_module_name 子模块时间
		// 2-act_name 操作名称		3-desc 描述
		// 4-user_id 操作人		5-opt_time 操作时间

		//拼接成 入库的sql
		b2 := time.Now().UnixNano()
		id := strconv.FormatInt(b2,10)
		log_sql := "INSERT INTO `sys_log`  SET `id` ="+id+","+
			"`module_name` ='"+sys_log_arr[0]+"',`sub_module_name` ='"+sys_log_arr[1]+
			"',`act_name`='"+sys_log_arr[2]+"',`desc` ='"+sys_log_arr[3]+
			"',`user_id` ="+sys_log_arr[4]+",`opt_time` ='"+sys_log_arr[5]+
			"',`org_id` ="+sys_log_arr[6]+","+

			"`init_id`= 0,`seq` =0,`syn_flag` =0,`syn_status` =0,`syn_time` =now(),`share_flag` =0,`factory_id` =0,`order_id` =0,`valid_flag` =1,`deleted` =0,`updated` = now(),`created` =now()"
		//在此入库
		fmt.Println("记录日志.....>",log_sql)
		models.PostSys(log_sql)

	}


}
func Out(w http.ResponseWriter,cnnn string)  {
	fmt.Fprintf(w,cnnn)
}

//用户登录
func GetSys(w http.ResponseWriter,api_params map[string]string) {
	if api_params["name"] != api_params["passwd"] {
		cnnJson := make(map[string]interface{})

		resuls := make(map[string]interface{})
		resuls["user_id"] = "1"
		resuls["user_name"] = "18611696067"
		resuls["token"] = "TOKEN_C4CA4238A0B923820DCC509A6F75849B"
		resuls["team_id"]="1"
		resuls["role_id"]="1"
		resuls["role_name"]="驼队物流"

		cnnJson["error"] = "0"
		cnnJson["errorMsg"] = ""
		cnnJson["results"] = resuls

		b, _ := json.Marshal(cnnJson)
		cnnn := string(b)

		fmt.Fprintf(w,cnnn)

	}else {
		fmt.Println("密码正确✅")

	}
}

func IsEqual(f1,f2 float64) bool  {
	return math.Dim(f1,f2) < MIN
}

func SysNoVerify(w http.ResponseWriter ,error string,errorMsg string) {
	cnnJson := make(map[string]interface{})

	resuls := make(map[string]interface{})

	cnnJson["error"] = error
	cnnJson["errorMsg"] = errorMsg
	cnnJson["results"] = resuls

	b, _ := json.Marshal(cnnJson)
	cnnn := string(b)

	fmt.Fprintf(w,cnnn)
}

func SysDefault(w http.ResponseWriter) {
		cnnJson := make(map[string]interface{})

		resuls := make(map[string]interface{})

		cnnJson["error"] = "404"
		cnnJson["errorMsg"] = "Please enter the correct method"
		cnnJson["results"] = resuls

		b, _ := json.Marshal(cnnJson)
		cnnn := string(b)

		fmt.Fprintf(w,cnnn)
}

func GetPasswd(str string) (string) {
	if str == "" {
		str = "bbc0c410396fafc86b677ba03eae0404"
	}

	//str md5加密后连接tuodui2017
	data := []byte(str)
	has := md5.Sum(data)
	str = fmt.Sprintf("%x", has) //将[]byte转成16进制
	str += "tuodui2017"

	//对str md5加密后连接0918
	data = []byte(str)
	has = md5.Sum(data)
	str = fmt.Sprintf("%x", has) //将[]byte转成16进制
	str += "0918"

	//对str md5 加密后转成大写返回
	data = []byte(str)
	has = md5.Sum(data)
	str = fmt.Sprintf("%x", has) //将[]byte转成16进制

	return strings.ToUpper(str)
}

func ChangePasswd(str string) (string) {
	if str == "" {
		str = "bbc0c410396fafc86b677ba03eae0404"
	}

	//str md5加密后连接tuodui2017
	data := []byte(str)
	has := md5.Sum(data)
	str = fmt.Sprintf("%x", has) //将[]byte转成16进制
	str += "tuodui2017"

	//对str md5加密后连接0918
	data = []byte(str)
	has = md5.Sum(data)
	str = fmt.Sprintf("%x", has) //将[]byte转成16进制
	str += "0918"

	//对str md5 加密后转成大写返回
	data = []byte(str)
	has = md5.Sum(data)
	str = fmt.Sprintf("%x", has) //将[]byte转成16进制

	return strings.ToUpper(str)
}