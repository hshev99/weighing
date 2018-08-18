package main

import (
	"fmt"
	"time"
	"reflect"
	"net/http"
	"io/ioutil"
	"strconv"
	"net/url"
	"github.com/bitly/go-simplejson"
	"crypto/md5"
	"strings"
	"encoding/base64"
	"math"
)
const MIN = 0.001

func IsEqual(f1,f2 float64) bool  {
	return math.Dim(f1,f2) < MIN
}

func main()  {
	carryout_amount := 1.21
	sel_contract_amount_float := 1.20
	aa:=carryout_amount==sel_contract_amount_float

	fmt.Println("比较结果为:",aa)
	//format Time, string type
	newFormat := time.Now().Local().Format("2006-01-02 15:04:05")
	fmt.Println(newFormat)
	fmt.Println(reflect.TypeOf(newFormat))
	/*
	runtime.GOMAXPROCS(runtime.NumCPU());
	for ll:=15110001403;ll<15910001403;ll++ {
		phone := strconv.Itoa(ll)
		SaveUser(phone)
	}
	*/
	// 如果要用在url中，需要使用URLEncoding
	//b64 := "%7B%22user_id%22%3A%221%22%2C%22user_name%22%3A%2218612729182%22%2C%22token%22%3A%22TOKEN_C4CA4238A0B923820DCC509A6F75849B%22%2C%22team_id%22%3A%221%22%2C%22role_id%22%3A%221%22%2C%22role_name%22%3A%22%5Cu9a7c%5Cu961f%5Cu7269%5Cu6d41%22%7D"

	input := []byte("hello golang base64 快乐编程http://www.01happy.com +~")

	// 演示base64编码
	encodeString := base64.StdEncoding.EncodeToString(input)
	fmt.Println(encodeString)

	uEnc := base64.URLEncoding.EncodeToString([]byte(input))
	fmt.Println(uEnc)

}

func GetPasswd(str string) (string) {

	data := []byte(str)
	has := md5.Sum(data)
	md5str1 := fmt.Sprintf("%x", has) //将[]byte转成16进制
	md5str1 += "tuodui-0908"

	data2 := []byte(md5str1)
	has2 := md5.Sum(data2)
	md5str2 := fmt.Sprintf("%x", has2) //将[]byte转成16进制

	result := strings.ToUpper(md5str2)
	return result
}

func SaveOrgInfo()  {

	resp, err := http.PostForm("http://api.caradmin.com:9598/Sys/PostSys/org_info",
		url.Values{"act": {"add"}, "parent_id": {"0"},"full_name":{"菜鸟集团"},"short_name":{"菜鸟"}})

	if err != nil {
		// handle error
		return
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		// handle error
		fmt.Println("请求错误")
		return
	}

	bs, err2 := simplejson.NewJson(body)

	if bs == nil {
		//fmt.Println("数据为空")
		//SaveUser(phone)
		return
	}

	if err2 != nil {
		fmt.Println("数据错误")
		return
	}

	fmt.Println(bs)

	return
}

func SaveUser(phone string)  {
	resp, err := http.PostForm("http://123.57.56.133:88/Login/login_save_web",
		url.Values{"phone": {phone}, "password": {"SB OR DAFT :Do you work without pay"+phone},"type":{"4"}})

	if err != nil {
		// handle error
		return
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		// handle error
		fmt.Println("请求错误",err)

	}

	bs, err2 := simplejson.NewJson(body)

	if bs == nil {
		fmt.Println("数据为空")
		//SaveUser(phone)

	}

	if err2 != nil {
		fmt.Println("数据错误",err2)

	}

	fmt.Println(phone)
	fmt.Println(bs)

	return
}
func Message(i int)  {
	i_str :=strconv.Itoa(i)
	resp,err := http.Get("http://api-huole.51huole.cn/Login/index?phone=186"+i_str+"&check_registered=1")

	if err != nil {
		// handle error
		//fmt.Println("2这里是发送失败....")
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println(string(body))
}