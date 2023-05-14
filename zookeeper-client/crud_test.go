package zookeeper_client

import (
	"github.com/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

// 初始化连接
func initConn(t *testing.T) *zk.Conn {
	conn, _, err := zk.Connect([]string{"192.168.122.20:2181", "192.168.122.21:2181", "192.168.122.22:2181"},
		5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

// TestCreatePersistentPath 创建一个持久化节点：/path1
// Cmd: create /path1
func TestCreatePersistentPath(t *testing.T) {
	conn := initConn(t)
	defer conn.Close()

	// flags有4种取值：
	// 0: 永久，除非手动删除
	// zk.FlagEphemeral = 1: 短暂，session断开则该节点也被删除
	// zk.FlagSequence  = 2: 会自动在节点后面添加序号
	// 3: Ephemeral和Sequence，即，短暂且自动添加序号
	var flags int32 = 0
	path := "/path1"
	res := createPath(t, conn, path, []byte("data"), flags)
	assert.Equal(t, path, res)

	// 持久化的 path 需要显式删除
	deletePath(t, conn, path)
}

// TestCreateEphemeralPath 创建一个临时节点：/path2
// Cmd: create -e /path2
func TestCreateEphemeralPath(t *testing.T) {
	conn := initConn(t)
	defer conn.Close()

	var flags int32 = zk.FlagEphemeral
	// 创建临时节点，连接断开后会自动删除
	path := "/path2"
	data := []byte("ephemeral")
	res := createPath(t, conn, path, []byte("ephemeral"), flags)
	assert.Equal(t, path, res)

	// 获取刚刚创建的临时节点
	resData, stat := getPath(t, conn, path)
	assert.Equal(t, data, resData)
	t.Logf("stat:%+v\n", *stat)
}

// TestCreatePersistentPathWithSequence 创建一个带序号的持久化节点：/path3
// Cmd: create -s /path3
func TestCreatePersistentPathWithSequence(t *testing.T) {
	conn := initConn(t)
	defer conn.Close()

	// flags有4种取值：
	// 0: 永久，除非手动删除
	// zk.FlagEphemeral = 1: 短暂，session断开则该节点也被删除
	// zk.FlagSequence  = 2: 会自动在节点后面添加序号
	// 3: Ephemeral和Sequence，即，短暂且自动添加序号
	var flags int32 = zk.FlagSequence
	path := "/path3"
	data := []byte("data")

	// 第一次创建
	res := createPath(t, conn, path, data, flags)
	assert.True(t, strings.HasPrefix(res, path))
	// 持久化的 path 需要显式删除
	deletePath(t, conn, res)

	// 第二次创建
	res = createPath(t, conn, path, data, flags)
	assert.True(t, strings.HasPrefix(res, path))
	// 持久化的 path 需要显式删除
	deletePath(t, conn, res)
}

// TestCreateEphemeralPathWithSequence 创建一个临时节点：/path4
// Cmd: create -e -s /path4
func TestCreateEphemeralPathWithSequence(t *testing.T) {
	conn := initConn(t)
	defer conn.Close()

	// flags有4种取值：
	// 0: 永久，除非手动删除
	// zk.FlagEphemeral = 1: 短暂，session断开则该节点也被删除
	// zk.FlagSequence  = 2: 会自动在节点后面添加序号
	// 3: Ephemeral和Sequence，即，短暂且自动添加序号
	var flags int32 = 3
	// 创建临时节点，连接断开后会自动删除
	path := "/path4"
	data := []byte("ephemeral")

	// 第一次创建
	res := createPath(t, conn, path, data, flags)
	assert.True(t, strings.HasPrefix(res, path))

	// 第二次创建
	res = createPath(t, conn, path, data, flags)
	assert.True(t, strings.HasPrefix(res, path))
}

// TestModifyPathData 修改指定 path 的值
// Cmd: set /path "new data"
func TestModifyPathData(t *testing.T) {
	conn := initConn(t)
	defer conn.Close()

	// flags有4种取值：
	// 0: 永久，除非手动删除
	// zk.FlagEphemeral = 1: 短暂，session断开则该节点也被删除
	// zk.FlagSequence  = 2: 会自动在节点后面添加序号
	// 3: Ephemeral和Sequence，即，短暂且自动添加序号
	var flags int32 = 0
	path := "/path5"
	data := []byte("data")

	_ = createPath(t, conn, path, data, flags)

	// 将 path5 的值修改为 new data
	modifyData(t, conn, path, []byte("new data"))
	afterData, _ := getPath(t, conn, path)
	assert.Equal(t, []byte("new data"), afterData)

	deletePath(t, conn, path)
}

// TestGetPathChildren 获取 path 下的所有 children
// Cmd: ls -s /path
func TestGetPathChildren(t *testing.T) {
	conn := initConn(t)
	defer conn.Close()

	var flags int32 = 0

	path := "/path6"
	data := []byte("ephemeral")
	_ = createPath(t, conn, path, data, flags)
	_ = createPath(t, conn, path+"/ch1", data, flags)
	_ = createPath(t, conn, path+"/ch2", data, flags)

	children, _, err := conn.Children(path)
	if err != nil {
		t.Fatal(err)
	}
	assert.ElementsMatch(t, []string{"ch1", "ch2"}, children)

	for _, child := range children {
		deletePath(t, conn, path+"/"+child)
	}

	deletePath(t, conn, path)
}

// TestExists 判断节点是否存在
func TestExists(t *testing.T) {
	conn := initConn(t)
	defer conn.Close()

	exists, _, err := conn.Exists("/noExist")
	if err != nil {
		t.Fatal(err)
	}
	assert.False(t, exists)
}

// getPath 获取 path 的 data 和 stat
// Cmd: get -s /path
func getPath(t *testing.T, conn *zk.Conn, path string) ([]byte, *zk.Stat) {
	data, stat, err := conn.Get(path)
	if err != nil {
		t.Fatal(err)
	}
	return data, stat
}

// deletePath 删除指定的 path，如果 path 下还有 children path，会删除失败
// Cmd: delete  /path
func deletePath(t *testing.T, conn *zk.Conn, path string) {
	_, stat, err := conn.Get(path)
	if err != nil {
		t.Fatal(err)
	}

	// version是用于 CAS支持，可以通过此种方式保证原子性
	if err := conn.Delete(path, stat.Version); err != nil {
		t.Fatal(err)
	}
}

// modifyData 修改节点的值
func modifyData(t *testing.T, conn *zk.Conn, path string, data []byte) {
	// 获取 path 的属性
	_, stat, err := conn.Get(path)
	if err != nil {
		t.Fatal(err)
	}
	stat, err = conn.Set(path, data, stat.Version)
	if err != nil {
		t.Fatal(err)
	}
}

// createPath 创建节点
func createPath(t *testing.T, conn *zk.Conn, path string, data []byte, flags int32) string {
	// 获取访问控制权限
	acl := zk.WorldACL(zk.PermAll)
	res, err := conn.Create(path, data, flags, acl)
	if err != nil {
		t.Fatal(err)
	}
	return res
}
