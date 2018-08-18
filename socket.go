package main

import (
	"runtime"
	"fmt"
)

func test(c chan bool, n int) {

	x := 0
	for i := 0; i < 1000000000; i++ {
		x += i
	}

	println(n, x)

	if n == 9 {
		c <- true
	}
}

func main() {
	runtime.GOMAXPROCS(1) //设置cpu的核的数量，从而实现高并发
	c := make(chan bool)

	for i := 0; i < 10; i++ {
		go test(c, i)
}

<-c

fmt.Println("main ok")

}