package main

import (
	"weighing/queue"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU());
	//监听kafka消费着
	queue.KaProucer()
	//queue.ActWebApiSendMessage("1505704338121148537","admin_user","edit")
}