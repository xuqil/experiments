package registry

import (
	"github.com/go-zookeeper/zk"
	"log"
	"testing"
	"time"
)

func getConn() *zk.Conn {
	conn, _, err := zk.Connect([]string{"192.168.122.20:2181", "192.168.122.21:2181", "192.168.122.22:2181"},
		5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	return conn
}

var registry = NewZkRegistry("servers", getConn())

// TestClientWatch 测试客户端的 watch
func TestClientWatch(t *testing.T) {
	// 1. 初始化 Client ，连接 zookeeper
	client := NewClient(registry)
	//	2. 获取 servers 的子节点信息，从中获取服务器信息列表
	go func() {
		for server := range client.GetServerList() {
			log.Println(server)
		}
	}()

	// 3. 业务进程启动
	client.Business(func() {
		for {
			time.Sleep(1 * time.Second)
		}
	})
}

// TestServerRegister 测试服务端上线
func TestServerRegister(t *testing.T) {
	// 依次启动5个服务
	for i := 0; i < 5; i++ {
		// 1. 初始化 server，连接 zk
		server := NewServer("server1", registry)
		// 2. 注册服务器
		server.Register()
		// 3. 业务进程启动
		server.Business(func() {
			for {
				time.Sleep(10 * time.Second)
			}
		})
		time.Sleep(5 * time.Second)
	}
}
