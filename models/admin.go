package models

import (
	"log"
	"os"
	"encoding/json"
	"fmt"
	"time"
)

const (
	Table = "admin_user"
)

func AddAdminUser(api_params map[string]string) ( string, string,interface{}) {
	//检测是否用户名是否存在
	act := api_params["act"]
	
	//新增用户
	if act =="add" {
		login_name := api_params["login_name"]
		sql := "select * from "+Table +" WHERE login_name='"+login_name+"'"

		sqlResult := SelsetSqlBySqlSliceOne(sql)

		if sqlResult != nil {
			return "159","该用户已经注册过",make([]interface{}, 0)
		}else {
			sql_insert := "INSERT INTO "+Table+" (login_name, password) VALUES (?, ?)";

			sqlParam := make([]interface{}, 0)
			sqlParam = append(sqlParam, api_params["login_name"])
			sqlParam = append(sqlParam, api_params["password"])

			rowsAffected, _, err := ExecSql(sql_insert, sqlParam...)

			if rowsAffected >0 {

				return "0","注册成功",make([]interface{}, 0)
			}else {
				//记录日志
				go WELog(9,sql_insert,err)
				return "159","注册失败",make([]interface{}, 0)

			}
		}
	}else if act =="del" {

		user_id := api_params["user_id"]
		sql_insert := "UPDATE "+Table+" set deleted=1 where user_id=?";

		sqlParam := make([]interface{}, 0)
		sqlParam = append(sqlParam, user_id)

		rowsAffected, _, err := ExecSql(sql_insert, sqlParam...)

		if rowsAffected >0 {

			return "0","删除成功",make([]interface{}, 0)
		}else {
			//记录日志
			go WELog(9,sql_insert,err)
			//查询是否已经被删除
			sql := "select * from "+Table +" WHERE deleted=1 and user_id="+user_id

			sqlResult := SelsetSqlBySqlSliceOne(sql)

			if sqlResult != nil {
				return "0","该用户已经被删除，请勿重复操作",make([]interface{}, 0)
			}

			return "159","删除失败",make([]interface{}, 0)

		}

	}else if act == "get" {
		//分页查询
		page := (api_params["page"])
		if page == "" {
			page ="1"
		}

		limit := (api_params["limit"])
		if limit == "" {
			limit ="1"
		}

		sql_count := "select count(1) a from "+Table
		//sqls_count := make([]string, 0)
		//sqls_count = append(sqls_count,sql_count)
		sqlResult_count := SelsetSqlBySqlCount(sql_count)

		fmt.Println(sqlResult_count)
		//jsonResult1, _ := json.Marshal(sqlResult_count)


		fmt.Println(page)

		sql := "select `user_name`,`login_name`,`mobile`,`org_id` from "+Table;
		sqls := make([]string, 0)
		sqls = append(sqls,sql)
		sqlResult := ExecSqlBySqlSlice(sqls)

		jsonResult, _ := json.Marshal(sqlResult)

		//fmt.Println(sqlResult[0])
		fmt.Println(string(jsonResult))

		Result := sqlResult[0].([]map[string]interface{})

		if Result == nil {
			return "1","无此数据",sqlResult[0]
		}else {
			return "0","",sqlResult[0]
		}


	}else{
		return "159","未检测到 act,请核对参数",make([]interface{}, 0)
	}
}

func WSLog(code int,sql_log string)  {

	var file_name string
	switch code {
	case 0:
		file_name = "sql_success.sql"
	case 1:
		file_name = "kafka.sql"
	case 2:
		file_name = "http.sql"

	default:
		file_name = "log.log"
	}
	file_path := "/mnt/data/cronatb.go/log/"
	file_path += time.Now().Local().Format("2006-01-02")

	file := file_path+"/"+file_name
	logfile,err:= os.OpenFile(file,os.O_RDWR|os.O_APPEND,0666)

	if err != nil {
		err_dir := os.MkdirAll(file_path, os.ModePerm)  //生成多级目录
		if err != nil {
			fmt.Println("创建目录报错<<><<<<<<<<>>>>>>",err_dir)
		}
		//创建文件
		_,err_file :=os.Create(file)
		if err_file != nil {
			fmt.Println("创建文件报错<<><<<<<<<<>>>>>>",err_file)
		}
		logfile,_=os.OpenFile(file,os.O_RDWR|os.O_APPEND,0666)

	}

	logger:=log.New(logfile,"",log.Ldate|log.Ltime)
	logger.Println(sql_log)
	defer logfile.Close()
	return
}
func WELog(code int,sql string,sql_log error)  {
	var file_name string
	switch code {
	case 0:
		file_name = "sql_error.sql"
	case 1:
		file_name = "kafka_err.sql"
	case 2:
		file_name = "http_err.sql"
	default:
		file_name = "err_log.log"
	}
	file_path := "/mnt/data/cronatb.go/errlog/"
	file_path += time.Now().Local().Format("2006-01-02")

	file := file_path+"/"+file_name
	logfile,err:= os.OpenFile(file,os.O_RDWR|os.O_APPEND,0666)

	if err != nil {
		err_dir := os.MkdirAll(file_path, os.ModePerm)  //生成多级目录
		if err != nil {
			fmt.Println("创建目录报错<<><<<<<<<<>>>>>>",err_dir)
		}
		//创建文件
		_,err_file :=os.Create(file)
		if err_file != nil {
			fmt.Println("创建文件报错<<><<<<<<<<>>>>>>",err_file)
		}
		logfile,_=os.OpenFile(file,os.O_RDWR|os.O_APPEND,0666)

	}

	logger:=log.New(logfile,"",log.Ldate|log.Ltime)
	logger.Println("########报错开始#####")
	logger.Println(sql)
	logger.Println(sql_log)
	logger.Println("########报错结束#####")
	defer logfile.Close()
	return
}