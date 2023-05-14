package zookeeper_client

import (
	"github.com/go-zookeeper/zk"
	"log"
	"testing"
	"time"
)

/*
1.如果即设置了全局监听又设置了部分监听，那么最终是都会触发的，并且全局监听会先执行
2.如果设置了监听子节点，那么事件的触发是先子节点后父节点
*/

// TestWatchGlobal 全局 watch
func TestWatchGlobal(t *testing.T) {
	// 创建监听的option，用于初始化zk
	eventCallbackOption := zk.WithEventCallback(callback)
	// 连接zk
	conn, _, err := zk.Connect([]string{"192.168.122.20:2181", "192.168.122.21:2181", "192.168.122.22:2181"}, time.Second*5, eventCallbackOption)
	defer conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("hello")
	var flags int32 = zk.FlagEphemeral
	path := "/watchGlobal"

	// 开始监听path
	_, _, _, err = conn.ExistsW(path)
	if err != nil {
		t.Fatal(err)
	}

	// 触发创建操作
	createPath(t, conn, path, data, flags)

	//再次监听path
	_, _, _, err = conn.ExistsW(path)
	if err != nil {
		t.Fatal(err)
	}

	// 触发删除操作
	deletePath(t, conn, path)
}

// callback 回调函数
func callback(event zk.Event) {
	if event.Err != nil {
		log.Fatal(event.Err)
	}
	log.Println("发生 global watch 回调：")
	log.Println("path:", event.Path)
	log.Println("type:", event.Type.String())
	log.Println("state:", event.State.String())
	log.Println("---------------------------")
}

// TestWatchPathData watch 节点的值
// Cmd: get -w /path
func TestWatchPathData(t *testing.T) {
	conn := initConn(t)

	path := "/watchData"
	data := []byte("data")
	var flags int32 = zk.FlagEphemeral

	_ = createPath(t, conn, path, data, flags)
	t.Log("before data:", string(data))

	// 监听 path 的数据
	_, _, event, err := conn.GetW(path)
	if err != nil {
		t.Error(err)
		return
	}

	go watchData(event)

	modifyData(t, conn, path, []byte("new data"))
	afterData, _ := getPath(t, conn, path)
	t.Log("after data:", string(afterData))
}

func watchData(e <-chan zk.Event) {
	event := <-e
	log.Println("发生 data watch 回调：")
	log.Println("path:", event.Path)
	log.Println("type:", event.Type.String())
	log.Println("state:", event.State.String())
	log.Println("---------------------------")
}

// TestWatchPathChildren watch 节点的子节点
// Cmd: ls -w /path
func TestWatchPathChildren(t *testing.T) {
	conn, _, err := zk.Connect([]string{"192.168.122.20:2181", "192.168.122.21:2181", "192.168.122.22:2181"},
		5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	path := "/watchData"
	data := []byte("data")
	var flags int32 = 0

	_ = createPath(t, conn, path, data, flags)

	// 监听 path 的数据
	_, _, event, err := conn.ChildrenW(path)
	if err != nil {
		t.Error(err)
		return
	}
	_ = createPath(t, conn, path+"/ch1", data, flags)

	// 监听 path 的数据
	_, _, event, err = conn.ChildrenW(path)
	if err != nil {
		t.Error(err)
		return
	}
	_ = createPath(t, conn, path+"/ch2", data, flags)

	go watchData(event)

	modifyData(t, conn, path, []byte("new data"))
	children, _, err := conn.Children(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, child := range children {
		deletePath(t, conn, path+"/"+child)
	}

	deletePath(t, conn, path)
}
