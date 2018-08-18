package cron

import (
	"github.com/robfig/cron"
	"fmt"
	"weighing/models"
	"time"
	"github.com/bitly/go-simplejson"
	"encoding/json"
	"strconv"
	"weighing/queue"
	"net/http"
)

func callWebGo()  {		//web-go 服务检测

	//执行sql
	resp,err := http.Get("http://monitor.weighing.tuodui.com:9598/hello/ping")

	if err != nil {
		// handle error
		fmt.Println("web-go程序❌❌❌❌")
	}else {
		defer resp.Body.Close()
		fmt.Println("web-go程序正常运行🐛🐛🐛🐛🐛")
		//body, _ := ioutil.ReadAll(resp.Body)
		//string(body)
		//"io/ioutil"

		sql_exl :="INSERT INTO `sys_monitor` SET "
		b2 := time.Now().UnixNano()
		ID := strconv.FormatInt(b2,10)
		sql_exl +=" `id`='"+ID+"',"
		sql_exl +=" `prog_type`='0',"
		sql_exl	+="`prog_seq`='1',"
		sql_exl	+=	"`run_tip`='正常运行',"
		sql_exl	+=	"`level`='0',"
		sql_exl	+=	"`syn_flag`='0',"
		sql_exl	+=	"`syn_status`='0',"
		sql_exl	+=	"`syn_time`=now(),"
		sql_exl	+=	"`share_flag`='0',"
		sql_exl	+=	"`user_id`='1',"
		sql_exl	+=	"`org_id`='0',"
		sql_exl	+=	"`deleted`='0',"
		sql_exl	+=	"`updated`=now(),"
		sql_exl	+=	"`created`=now()"

		//村信息
		models.ExecSql(sql_exl)

	}

}

func callMonitorWeb()  {		//web-php 服务程序检测
	//请求web服务
	resp,err := http.Get("http://monitor.weighing.tuodui.com:9595/hello/ping")

	if err != nil {
		// handle error
		fmt.Println("web-php程序❌❌❌❌❌❌")
	}else {
		defer resp.Body.Close()
		fmt.Println("web-php程序正常运行🐘🐘🐘🐘🐘")
		//body, _ := ioutil.ReadAll(resp.Body)
		//string(body)

		sql_exl :="INSERT INTO `sys_monitor` SET "
		b2 := time.Now().UnixNano()
		ID := strconv.FormatInt(b2,10)
		sql_exl +=" `id`='"+ID+"',"
		sql_exl +=" `prog_type`='1',"
		sql_exl	+="`prog_seq`='1',"
		sql_exl	+=	"`run_tip`='正常运行',"
		sql_exl	+=	"`level`='0',"
		sql_exl	+=	"`syn_flag`='0',"
		sql_exl	+=	"`syn_status`='0',"
		sql_exl	+=	"`syn_time`=now(),"
		sql_exl	+=	"`share_flag`='0',"
		sql_exl	+=	"`user_id`='1',"
		sql_exl	+=	"`org_id`='0',"
		sql_exl	+=	"`deleted`='0',"
		sql_exl	+=	"`updated`=now(),"
		sql_exl	+=	"`created`=now()"
		//存储信息
		models.ExecSql(sql_exl)
	}
}

func clearMonitor()  {		//web-php 服务程序检测
	//请求web服务
	fmt.Println("执行数据清理🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚🥚")
	sql_exl :="DELETE FROM `sys_monitor` WHERE (TO_DAYS(NOW())-TO_DAYS(`created`))  > 3"
	//存储信息
	models.ExecSql(sql_exl)
}

func RetryKafka()  {
	resend := "*/200, *, *, *, *, *" // Kafka消息重发机制
	web_go := "0, *, *, *, *, *" // web-go 监听机制
	web_php := "1, *, *, *, *, *" // web-go 监听机制
	clear_monitor := "0, 0, 0, *, *, *" // 明天检查一次 清理超过一周的监控
	c := cron.New()
	c.AddFunc(resend, callResend)
	c.AddFunc(web_go, callWebGo)
	c.AddFunc(web_php, callMonitorWeb)
	c.AddFunc(clear_monitor, clearMonitor)
	c.Start()

	select {}
}

//重发消息机制
func callResend() {
	//查看redis中的  kafka 发送id
	r,weighSend := models.GetKafkaSend()
	if r {
		for send_id,val := range weighSend{
			//类型断言
			b := val.([]byte)
			js, _ := simplejson.NewJson(b)

			//更新重发次数
			re_send,_ := js.Get("re_send").String()
			re_send_int,_:=strconv.Atoi(re_send)
			re_send_int ++
			re_send_str:=strconv.Itoa(re_send_int)
			//对重发次数大于1000次的进行清楚
			if re_send_int >5000 {
				fmt.Print("重发次数大于5K")
				models.ClearKafkaSend(send_id)
				return
			}

			js.Set("re_send",re_send_str)

			// ->json -> string
			cnnn, _ := json.Marshal(js)

			fmt.Println("这里进行重发....>")

			//发送Kafka
			queue.AproduceStdin(string(cnnn))

			//再此更新redis
			models.SettKafkaSend(send_id,cnnn)
		}

	}

}
