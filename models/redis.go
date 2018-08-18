package models
import (
	"github.com/garyburd/redigo/redis"
	"time"
	"fmt"
	"strings"
	"strconv"
	"crypto/md5"
	"github.com/bitly/go-simplejson"
)

var (
	// 定义常量
	RedisClient     *redis.Pool
	REDIS_HOST string
	REDIS_DB   int
)

func init() {
	// 从配置文件获取redis的ip以及db
	fmt.Println("2-5>>>>redis>>>连接👌👌👌👌👌👌👌")
	REDIS_HOST ="127.0.0.1:6379"
	REDIS_DB = 1
	// 建立连接池
	RedisClient = &redis.Pool{
		// 从配置文件获取maxidle以及maxactive，取不到则用后面的默认值
		MaxIdle:    4,
		MaxActive:   20,
		IdleTimeout: 180 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", REDIS_HOST)
			if err != nil {
				return nil, err
			}
			// 选择db
			c.Do("SELECT", REDIS_DB)
			return c, nil
		},
	}
}

func SettKafkaSend(key string,val []byte)  {
	rc := RedisClient.Get()

	//删除同类型ID
	keys, _ := redis.Strings(rc.Do("KEYS", "kafkaweighSend_"+key+"_*"))
	for _, k := range keys {
		//删除该键
		rc.Do("DEL", k)
	}

	k := "kafkaweighSend_"+key+"_"+strconv.FormatInt(time.Now().UnixNano(),10)
	//fmt.Println(k)

	//fmt.Println(">>>>>>>>>>>>>>>>>>>",val)
	_, err := redis.String(rc.Do("SET", k,val))

	defer rc.Close()


	if err != nil {
		fmt.Println(".......>",err)
		return
	}
}

func SettKafkaSendString(key string,val string)  {
	rc := RedisClient.Get()

	//删除同类型ID
	keys, _ := redis.Strings(rc.Do("KEYS", "kafkaweighSend_"+key+"_*"))
	for _, k := range keys {
		//删除该键
		rc.Do("DEL", k)
	}

	k := "kafkaweighSend_"+key+"_"+strconv.FormatInt(time.Now().UnixNano(),10)
	//fmt.Println(k)

	fmt.Println("将存入...>>>>>>>>>>>>>>>>>>>",val)
	_, err := rc.Do("SET", k,val)

	defer rc.Close()


	if err != nil {
		fmt.Println(".......>",err)
		return
	}
}

func ClearKafkaSend(k string) () {
	fmt.Println("这里是执行清除",k)
	rc := RedisClient.Get()
	keys, _ := redis.Strings(rc.Do("KEYS", "kafkaweighSend_"+k+"*"))

	var result []byte

	if len(keys) ==0 {
		fmt.Println("没有刚发送的kafka信息")
	}else {
		//redis 有值
		for _, key := range keys {
			if len(result) ==0{
				fmt.Println("这里是 redis key",key)
				rc.Do("DEL", key)
			}
		}
	}

	defer rc.Close()

	return
}

func GetAltDifference() (bool,map[string]interface{}) {
	rc := RedisClient.Get()
	keys, _ := redis.Strings(rc.Do("KEYS", "kafkaweighSend_*"))

	array5 := make(map[string]interface{})

	if len(keys) ==0 {
		//fmt.Println("没有刚发送的kafka信息")
	}else {
		//redis 有值
		for _, key := range keys {
			//fmt.Println(key)
			//如果存在，比对时间去重发
			//分离时间
			key_time := strings.Split(key, "_")
			if len(key_time) ==3 {
				time_last := key_time[2]
				kk,_ := strconv.ParseInt(time_last,10,64)
				timeout := (int64(time.Now().UnixNano())-kk )/1000000000

				if timeout > 3 {
					//array5[key_time[1]]=val
					v, _ := redis.Bytes(rc.Do("GET", key))
					array5[key_time[1]] = v
				}
			}

		}

	}

	defer rc.Close()

	return true,array5
}

func GetKafkaSend() (bool,map[string]interface{}) {
	rc := RedisClient.Get()
	//keys, _ := redis.Strings(rc.Do("KEYS", "alt*"))
	keys, _ := redis.Strings(rc.Do("KEYS", "kafkaweighSend_*"))

	array5 := make(map[string]interface{})

	if len(keys) ==0 {
		//fmt.Println("没有刚发送的kafka信息")
	}else {
		//redis 有值
		for _, key := range keys {
			//fmt.Println(key)
			//如果存在，比对时间去重发
			//分离时间
			//查看已发送列表中是否存在改
			//send_key := "send_alt"+key
			send_key := key
			send_key_toush, _ := redis.Strings(rc.Do("KEYS", send_key))
			//fmt.Println("1%%%",send_key)
			//fmt.Println("2%%%",send_key_toush)
			//fmt.Println("3%%%",len(send_key_toush))

			if len(send_key_toush) == 1 {
				//发送 websocket 并将次信息存入
				//rc.Do("SET", send_key, "1")
				rc.Do("EXPIRE", send_key, 300)

				//取出内容并返回
				v, _ := redis.Bytes(rc.Do("GET", key))

				key_time := strings.Split(key, "_")

				fmt.Println("1%%%",key_time[1])

				array5[key_time[1]]=v
				return true,array5

			}else {
				rc.Do("EXPIRE", send_key, 300)
			}
		}

	}

	defer rc.Close()

	return true,array5
}

func GetUserTooken(str string,atom string) (bool,string) {
	rc := RedisClient.Get()

	if str == "" {
		return false,"登录过期，请重新登录"
	}
	data := []byte(str)
	has := md5.Sum(data)
	str = fmt.Sprintf("%x", has) //将[]byte转成16进制

	str = strings.ToUpper(str)
	keys := "TOKEN_"+str
	//v, err := redis.Bytes(rc.Do("GET", key))



	keys_value, _ := redis.Strings(rc.Do("KEYS", keys+"_*"))

	if len(keys_value) ==0 {
		//未查询到相关结果
		return false,"登录过期,请重新登录"
	}else {
		//redis 有值
		for _, key := range keys_value {
			//fmt.Println(key)
			//如果存在，比对时间去重发
			//分离时间
			status := strings.Split(key, "_")
			if len(status) == 4 {
				switch status[3] {
				case "0": //正常账户
					v, _ := redis.Bytes(rc.Do("GET", key))
					//b := val.([]byte)
					js, _ := simplejson.NewJson(v)
					uid,_ := js.Get("user_id").String()
					return true,uid
				case "1"://已被其他地方登录
					return false,"您的账户已在其他地方登录，如非本人操作请尽快修改密码"
				case "2"://删除
					return false,"您的账户已被删除，如有疑问请联系管理员"
				case "3"://禁用
					return false,"您的账户已被禁用，如有疑问请联系管理员"
				default://过期
					return false,"登录过期，请重新登录"
				}
			}else {
				return false,"登录过期，请重新登录"
			}

		}

	}




	defer rc.Close()

	//if err == nil || len(v)==0 {
	//	fmt.Println("redis>>>>>>>>",key)
	//	return false,[]byte{}
	//
	//}




	return true,"已经登录"
}
func SET(i int)  {
	rc := RedisClient.Get()

	v, err := rc.Do("SET", "name", i)


	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(v)

	rc.Do("SET",time.Now().UnixNano(),time.Now().UnixNano())
	defer rc.Close()
}
func G()  {
	rc := RedisClient.Get()
	// 用完后将连接放回连接池
	fmt.Println(rc.Do("keys","*"))
	defer rc.Close()
}