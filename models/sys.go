package models

import (
	"strconv"
)

func PostSys(sqlsql string) ( string, string,interface{}) {
	//检测是否用户名是否存在
		rowsAffected, _, err := ExecSql(sqlsql)

		if rowsAffected >0 {
			go WSLog(0,sqlsql)
			return "0","成功",make([]interface{}, 0)
		}else {
			//记录日志
			go WELog(0,sqlsql,err)

			return "159", "失败", make([]interface{}, 0)
		}
}

func GetSys(sqlsql string,sqlcount string,p int) ( string, string,interface{}) {
	//检测是否用户名是否存在
	results := make(map[string]interface{})
	page := make(map[string]interface{})

	count := SelsetSqlBySqlCount(sqlcount)

	totalCount := count

	page["curpage"] =strconv.Itoa(p)
	page["limit"] ="10"
	page["pageCount"] =strconv.Itoa((totalCount/10)+1)
	page["totalCount"] =strconv.Itoa(totalCount)

	limit := ""
	limit += " limit "
	limit += strconv.Itoa((p-1)*10)
	limit += " ,"
	limit += strconv.Itoa((p)*10)
	sqlsql +=limit

	var result interface{}
	if count ==0 {
		result =make([]interface{}, 0)
	}else {
		result,_ = SelectSqlToStringMap(sqlsql)
	}


	results["page"] = page
	results["result"] = result

	return "0","",results

}

func GetDataById(sqlsql string) ( interface{}) {
	//检测是否用户名是否存在

	var result interface{}
	result,_ = SelectSqlToStringMap(sqlsql)

	return result

}