package controllers

import (
	"net/http"
	"github.com/julienschmidt/httprouter"
	"fmt"
	"encoding/json"
)

func Index(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	//fmt.Fprint(w, "Welcome!\n")
	cnnJson := make(map[string]interface{})

	resuls := make(map[string]interface{})
	//resuls["user_id"] = "1"
	//resuls["user_name"] = "18611696067"
	//resuls["token"] = "TOKEN_C4CA4238A0B923820DCC509A6F75849B"
	//resuls["team_id"]="1"
	//resuls["role_id"]="1"
	//resuls["role_name"]="驼队物流"

	cnnJson["code"] = 404
	cnnJson["errorMsg"] = "Please enter a specific method"
	cnnJson["results"] = resuls

	//fmt.Println("密码错误❌")

	b, _ := json.Marshal(cnnJson)
	cnnn := string(b)

	fmt.Fprintf(w,cnnn)

}
