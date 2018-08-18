package routers

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func OutSuccessful(w http.ResponseWriter,api_params map[string]interface{})  {
	cnnJson := make(map[string]interface{})

	resuls := make(map[string]interface{})

	cnnJson["code"] = "0"
	cnnJson["errorMsg"] = api_params["errorMsg"]
	cnnJson["results"] = resuls

	b, _ := json.Marshal(cnnJson)
	cnnn := string(b)

	fmt.Fprintf(w,cnnn)
}

func OutFailure(w http.ResponseWriter,api_params map[string]interface{})  {
	cnnJson := make(map[string]interface{})

	resuls := make(map[string]interface{})

	cnnJson["code"] = api_params["code"]
	cnnJson["errorMsg"] = api_params["errorMsg"]
	cnnJson["results"] = resuls

	b, _ := json.Marshal(cnnJson)
	cnnn := string(b)

	fmt.Fprintf(w,cnnn)
}
