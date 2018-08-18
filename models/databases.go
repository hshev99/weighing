package models

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
	"time"
	"strings"
	"strconv"
	"fmt"
)

var db = &sql.DB{}
/****************************************************************
 *                      全局变量                                 *
 ****************************************************************/

/* 使用这个Db链接 */
//var Db = getDB()
var Db = db

/* 数据库相关配置 */
/*
var MySQLUsername string = "root"               // 用户名
var MySQLPassword string = "Root@123"              // 密码
var MySQLDatabase string = "gnssPlatform"               // 数据库
var MySQLHostPort string = "118.190.96.116:3306"     // 主机
*/

var MySQLUsername string = "root"               // 用户名
var MySQLPassword string = "Root@123++"              // 密码
var MySQLDatabase string = "weighing"               // 数据库
var MySQLHostPort string = "118.190.207.192:3306"     // 主机

/*
// 数据库 物流平台配置	caravans
var MySQLUsername string = "tuodui_root"               // 用户名
var MySQLPassword string = "Tdroot123"              // 密码
var MySQLDatabase string = "weighing"               // 数据库
var MySQLHostPort string = "rm-m5e67f8g4y49u063q.mysql.rds.aliyuncs.com:3306"     // 主机

/* 数据库链接资源,主要用来链接数据库 */
var DatabaseSource string = MySQLUsername + ":" + MySQLPassword +
	"@tcp(" + MySQLHostPort + ")/" +
	MySQLDatabase + "?charset=utf8&loc=Asia%2FShanghai"

/* 数据库相关配置 */

func init(){
	db,_ = sql.Open("mysql", DatabaseSource)
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(100)
	db.Ping()
	fmt.Println("1-5>>>>>>>mysql>>weighing>>连接👌👌👌👌👌")

	if db == nil {
		fmt.Println("数据库连接失败❌❌❌❌❌❌")
	}


}



/****************************************************************
 *                          相关函数                             *
 ****************************************************************/

/* 得到数据库链接 */
//func getDB() *sql.DB {
//	db, err := sql.Open("mysql", DatabaseSource)
//
//	if err != nil {
//		log.Println(err)
//		return nil
//	}
//
//	if db == nil {
//		log.Println("数据库连接失败")
//		return nil
//	}
//
//	return db
//}

/****************************************************************
 *                      核心方法                                 *
 ****************************************************************/

// 根据一条SQL语句执行SELECT操作,返回所有查询到的数据,以map的形式存储
// 全部数据是一个map slice形式
// sql = "SELECT * FROM users"
func SelectSqlToStringMap(sql string, args ...interface{}) (resultRows []map[string]interface{}, err error) {
	//db := getDB()
	db.Ping()

	//fmt.Println(sql)
	sqlPrepare, err := db.Prepare(sql)
	if err != nil {
		return nil, err
		go WELog(0,sql,err)
	}
	//记录sql
	go WSLog(0,sql)

	defer sqlPrepare.Close()

	res, err := sqlPrepare.Query(args...)
	if err != nil {
		return nil, err
	}

	defer res.Close()

	fields, err := res.Columns()
	if err != nil {
		return nil, err
	}

	for res.Next() {
		result := make(map[string] interface{})
		var scanResultContainers []interface{}
		for i := 0; i < len(fields); i++ {
			var scanResultContainer interface{}
			scanResultContainers = append(scanResultContainers, &scanResultContainer)
		}

		if err := res.Scan(scanResultContainers...); err != nil {
			return nil, err
		}

		for ii, key := range fields {
			rawValue := reflect.Indirect(reflect.ValueOf(scanResultContainers[ii]))
			//if row is null then ignore
			if rawValue.Interface() == nil {
				//continue
				result[key] = ""

			}else {
				result[key] = RawValueToString(rawValue.Interface())
			}


		}

		resultRows = append(resultRows, result)
	}

	return resultRows, nil
}

func SelectSqlToStringMap2(sql string, args ...interface{}) (resultRows []map[string]interface{}, err error) {
	//db := getDB()
	db.Ping()
	sqlPrepare, err := db.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer sqlPrepare.Close()

	res, err := sqlPrepare.Query(args...)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	fields, err := res.Columns()
	if err != nil {
		return nil, err
	}

	for res.Next() {
		result := make(map[string] interface{})
		var scanResultContainers []interface{}
		for i := 0; i < len(fields); i++ {
			var scanResultContainer interface{}
			scanResultContainers = append(scanResultContainers, &scanResultContainer)
		}

		if err := res.Scan(scanResultContainers...); err != nil {
			return nil, err
		}

		for ii, key := range fields {
			rawValue := reflect.Indirect(reflect.ValueOf(scanResultContainers[ii]))
			//if row is null then ignore
			if rawValue.Interface() == nil {
				continue
			}

			result[key] = RawValueToString(rawValue.Interface())
		}

		resultRows = append(resultRows, result)
	}

	return resultRows, nil
}

// 根据一条SQL语句执行UPDATE,INSERT,DELETE操作
func ExecSqlToStringMap(sqlString string, args ...interface{}) (resultRows []map[string]interface{}, err error) {
	db.Ping()

	resultRows = make([]map[string] interface{}, 0)
	resultMap := make(map[string] interface{})

	res, err := db.Exec(sqlString, args ...)
	if err != nil {
		resultMap["error"] = err.Error()
		//记录日志
		go WELog(0,sqlString,err)
	} else {
		resultMap["RowsAffected"], err = res.RowsAffected()
		resultMap["LastInsertId"], err = res.LastInsertId()
		//记录日志
		go WSLog(0,sqlString)
	}

	resultRows = append(resultRows, resultMap)

	return
}

// 根据一条SQL语句执行UPDATE,INSERT,DELETE操作
// rowsAffected:影响的条数
// lastInsertId:最后插入的id
func ExecSql(sql string, args ...interface{}) (rowsAffected int64, lastInsertId int64, err error) {
	//db := getDB()
	db.Ping()

	sqlPrepare, err := db.Prepare(sql)
	if err != nil {
		//记录日志
		go WELog(0,sql,err)
		return 0, 0, err
	}
	//记录日志
	go WSLog(0,sql)

	defer sqlPrepare.Close()

	res, err := sqlPrepare.Exec(args...)
	if err != nil {
		return 0, 0, err
	}

	rowsAffected, _ = res.RowsAffected()
	lastInsertId, _ = res.LastInsertId()

	return
}

// 无序,json对象
// 同时发送多个SQL查询语句给后台
func ExecSqlBySqlMap(sqlObject map[string]string) (map[string]interface{}) {

	sqlResults := make(map[string]interface{})

	var resultRows []map[string]interface{}

	for key, sql := range sqlObject {

		sql = strings.TrimLeft(sql, " \n")

		sqlCmd := strings.ToLower( strings.Split(sql, " ")[0] )

		switch sqlCmd {

		case "select", "desc":
			resultRows, _ = SelectSqlToStringMap(sql)

		case "insert", "delete", "update":
			resultRows, _ = ExecSqlToStringMap(sql)
		}

		sqlResults[key] = resultRows
	}

	return sqlResults
}

// 有序,json数组
// 同时发送多个SQL查询语句给后台
func ExecSqlBySqlSlice(sqlObject []string) ([]interface{}) {
	db.Ping()

	sqlResults := make([]interface{}, 0)

	var resultRows []map[string]interface{}

	for _, sql := range sqlObject {

		sql = strings.TrimLeft(sql, " ")

		sqlCmd := strings.ToLower( strings.Split(sql, " ")[0] )

		switch sqlCmd {

		case "select":
			resultRows, _ = SelectSqlToStringMap(sql)

		case "insert", "delete", "update":
			resultRows, _ = ExecSqlToStringMap(sql)
		}

		sqlResults = append(sqlResults, resultRows)
	}

	return sqlResults
}

func SelsetSqlBySqlSliceOne(sqlObject string) (interface{}) {
	resultRows, _ := SelectSqlToStringMap(sqlObject)

	if resultRows == nil {
		return nil
	}else {
		return resultRows[0]
	}



}

func SelsetSqlBySqlCount(sqlObject string) (int) {
	resultRows, _ := SelectSqlToStringMap(sqlObject)

	//fmt.Println(resultRows)
	if resultRows == nil {
		return 0
	}else {
		var aa  string
		for k,v := range resultRows[0]{
			if k=="a" {
				aa = RawValueToString(v)
			}
		}

		int,_:=strconv.Atoi(aa)

		return int
	}
}

func SelsetOrgLocal(sqlObject string) ([2]string ) {
	resultRows, _ := SelectSqlToStringMap(sqlObject)

	result := [2]string{}
	if resultRows == nil {
		return result
	}else {
		for k,v := range resultRows[0]{
			if k=="a" {
				result[0] = RawValueToString(v)
			}else if k =="b" {
				result[1] = RawValueToString(v)
			}
		}
		//int,_:=strconv.ParseInt(aa, 10, 64)
		return result
	}
}

func SelsetSqlBySqlOne64(sqlObject string) (int64) {
	resultRows, _ := SelectSqlToStringMap(sqlObject)

	//fmt.Println(resultRows)
	if resultRows == nil {
		return 0
	}else {
		var aa  string
		for k,v := range resultRows[0]{
			if k=="a" {
				aa = RawValueToString(v)
			}
		}

		int,_:=strconv.ParseInt(aa, 10, 64)

		return int
	}
}

func SelsetSqlBySqlMax(sqlObject string) (string) {
	resultRows, _ := SelectSqlToStringMap(sqlObject)

	if resultRows == nil {
		return "0"
	}else {
		var aa  string
		for k,v := range resultRows[0]{
			if k=="a" {
				aa = RawValueToString(v)
			}
		}
		return aa
	}
}

/****************************************************************
 *                      辅助方法                                 *
 ****************************************************************/

// 将不同类型的数据转换成string类型
func RawValueToString(rawValue interface{}) string {

	//fmt.Println("这里是转化的原始数据",rawValue)
	valueType := reflect.TypeOf(rawValue)
	value := reflect.ValueOf(rawValue)

	var str string
	switch reflect.TypeOf(rawValue).Kind() {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		//fmt.Println("Int8")
		str = strconv.FormatInt(value.Int(), 10)
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		//fmt.Println("Uint8")
		str = strconv.FormatUint(value.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		//fmt.Println("Float32")
		str = strconv.FormatFloat(value.Float(), 'f', -1, 64)
	case reflect.Slice:
		//fmt.Println("Slice")
		if valueType.Elem().Kind() == reflect.Uint8 {
			str = string(value.Bytes())
			break
		}
	case reflect.String:
		//fmt.Println("String")
		str = value.String()
		//时间类型
	case reflect.Struct:
		//fmt.Println("Struct")
		str = rawValue.(time.Time).Format("2006-01-02 15:04:05.000 -0700")
	case reflect.Bool:
		//fmt.Println("Bool")
		if value.Bool() {
			str = "1"
		} else {
			str = "0"
		}
	}

	return str
}