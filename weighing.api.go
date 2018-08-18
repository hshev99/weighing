package main

import (
	"github.com/julienschmidt/httprouter"
	"net/http"
	"weighing/controllers"
	"fmt"
	"runtime"
)

func main() {
	// 利用cpu多核来处理http请求，这个没有用go默认就是单核处理http的，这个压测过了，请一定要相信我
	runtime.GOMAXPROCS(runtime.NumCPU());

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
	http.ListenAndServe(":9598", router)

}
