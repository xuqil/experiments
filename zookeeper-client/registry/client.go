package registry

import (
	"fmt"
	"github.com/go-zookeeper/zk"
	"log"
)

// Client 客户端
type Client struct {
	zk         *ZkRegistry
	serverChan chan []string
}

func NewClient(registry *ZkRegistry) *Client {
	return &Client{
		zk:         registry,
		serverChan: make(chan []string),
	}
}

// GetServerList 获取服务列表
func (c *Client) GetServerList() <-chan []string {
	go c.Watch()
	return c.serverChan
}

func (c *Client) Business(fn func()) {
	fn()
}

// Watch 监听一个节点的子节点，获取所有子节点的数据
func (c *Client) Watch() {
	children, _, event, err := c.zk.Conn.ChildrenW(c.zk.RootPath)
	if err != nil {
		return
	}

	data := make([]string, 0)
	for _, child := range children {
		res, _, er := c.zk.Conn.Get(fmt.Sprintf("%s/%s", c.zk.RootPath, child))
		if er != nil {
			log.Println(er)
			continue
		}
		data = append(data, string(res))
	}
	go c.watchChildren(event)

	c.serverChan <- data
}

func (c *Client) watchChildren(event <-chan zk.Event) {
	<-event
	// 再次监听
	c.Watch()
}
