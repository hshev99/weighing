package main

import (
	"fmt"
	"github.com/widuu/goini"
	"weighing/queue"
	"weighing/cron"
	"runtime"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"weighing/controllers"
)

//申明配置
var Config  = &goini.Config{}

//初始化程序
func init(){
	Config = goini.SetConfig("./config/app.ini")
}

func main() {
	// 利用cpu多核来处理http请求，这个没有用go默认就是单核处理http的，这个压测过了，请一定要相信我
	fmt.Println("cpu核心数🚀",runtime.NumCPU())
	runtime.GOMAXPROCS(runtime.NumCPU());

	//这里是数据重发机制
	fmt.Println("4-5>>>>>>>>数据重发机制启动中.....👌👌👌👌👌👌")
	go cron.RetryKafka()

	//制定路由规则
	router := httprouter.New()

	router.GET("/", controllers.Index)
	router.POST("/", controllers.Index)
	router.GET("/hello/:name", controllers.Admin)
	router.GET("/img/:name", controllers.Img)

	//用户模块
	router.POST("/admin/:name",controllers.Admin)
	router.POST("/Sys/:name/:scope",controllers.Sys)

	fmt.Println("WebServer api start...✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅")

	//log.Fatal(http.ListenAndServe(":9598", router))
	go http.ListenAndServe(":9598", router)

	//在这里监听Kafka
	fmt.Println("5-5>>>>>>>>数据处理服务开始启动.....👌👌👌👌👌👌 v2.9.4")
	fmt.Println("AllService startup success Is OK✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅ ✅")

	queue.KaConsume()

}