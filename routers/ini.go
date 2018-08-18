package routers

import (
	"github.com/widuu/goini"
)

//申明配置
var Config  = &goini.Config{}


func init(){
	Config = goini.SetConfig("./config/app.ini")

}

//初始化程序
func GetConfig(confVal string) (string) {
	//Config = goini.SetConfig("../config/app.ini")

	//fmt.Println(Config.GetValue("http", "host"))
	config_params := make(map[string]interface{})


	//http
	config_params["http_host"] = Config.GetValue("http", "host")
	config_params["http_port"] = Config.GetValue("http", "port")

	config_params["http_addr"] = Config.GetValue("http", "host") + ":" + Config.GetValue("http", "port")


	//database
	config_params["db_host"] = Config.GetValue("db", "host")
	config_params["db_port"] = Config.GetValue("db", "port")
	config_params["db_user"] = Config.GetValue("db", "user")
	config_params["db_passwd"] = Config.GetValue("db", "passwd")

	//kafka
	config_params["kafka_host"] = Config.GetValue("kafka", "host")
	config_params["kafka_port"] = Config.GetValue("kafka", "port")
	config_params["from_to_lan"] = Config.GetValue("kafka", "from_to_lan")
	config_params["reply_from_to_lan"] = Config.GetValue("kafka", "reply_from_to_lan")
	config_params["send_to_lan"] = Config.GetValue("kafka", "send_to_lan")

	//
	config_params["http_host"] = Config.GetValue("http", "host")


	return ""
}