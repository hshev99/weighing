package controllers

import (
	"net/http"
	"github.com/julienschmidt/httprouter"
	"fmt"
	"encoding/json"
	"io/ioutil"
	"weighing/models"
)

func KaAdmin(w http.ResponseWriter, r *http.Request, ps httprouter.Params)  {

	fmt.Println(ps.ByName("name"))

	roue := ps.ByName("name")
	//对接收到的 参数就行分析
	con, err := ioutil.ReadAll(r.Body) //获取post的数据
	if err != nil {
		fmt.Println(err)
	}

	api_params := make(map[string]string)
	err = json.Unmarshal(con, &api_params)


	switch roue {
	case "LoginAdminUser":
		LoginAdminUser(w ,api_params)
	case "PostAdminUser":
		PostAdminUser(w,api_params)

	default:
		Default(w)
	}
}

//保存用户
func KaPostAdminUser(w http.ResponseWriter,api_params map[string]string)  {

	code,errMsg,resuls := models.AddAdminUser(api_params)

	if code == "0" {
		cnnJson := make(map[string]interface{})

		//resuls := make(map[string]interface{})

		cnnJson["code"] =code
		cnnJson["errorMsg"] = "新增成功"
		cnnJson["results"] = resuls

		b, _ := json.Marshal(cnnJson)
		cnnn := string(b)

		fmt.Fprintf(w,cnnn)
	}else {
		cnnJson := make(map[string]interface{})

		//resuls := make(map[string]interface{})

		cnnJson["code"] = code
		cnnJson["errorMsg"] = errMsg
		cnnJson["results"] = resuls

		b, _ := json.Marshal(cnnJson)
		cnnn := string(b)

		fmt.Fprintf(w,cnnn)
	}

}

//用户登录
func KaLoginAdminUser(w http.ResponseWriter,api_params map[string]interface{}) {
	if api_params["name"] != api_params["passwd"] {
		cnnJson := make(map[string]interface{})

		resuls := make(map[string]interface{})
		resuls["user_id"] = "1"
		resuls["user_name"] = "18611696067"
		resuls["token"] = "TOKEN_C4CA4238A0B923820DCC509A6F75849B"
		resuls["team_id"]="1"
		resuls["role_id"]="1"
		resuls["role_name"]="驼队物流"

		cnnJson["code"] = "0"
		cnnJson["errorMsg"] = ""
		cnnJson["results"] = resuls

		b, _ := json.Marshal(cnnJson)
		cnnn := string(b)

		fmt.Fprintf(w,cnnn)

	}else {
		fmt.Println("密码正确✅")

	}
}

func KaDefault(w http.ResponseWriter) {
		cnnJson := make(map[string]interface{})

		resuls := make(map[string]interface{})

		cnnJson["code"] = "404"
		cnnJson["errorMsg"] = "Please enter the correct method"
		cnnJson["results"] = resuls

		b, _ := json.Marshal(cnnJson)
		cnnn := string(b)

		fmt.Fprintf(w,cnnn)
}

