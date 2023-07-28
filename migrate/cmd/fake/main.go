package main

import (
	"github.com/xuqil/experiments/migrate/dwrite"
	"github.com/xuqil/experiments/migrate/generate"
	"github.com/xuqil/experiments/migrate/models"
	"log"
	"time"
)

// 生成测试数据
func main() {
	db := dwrite.InitSourceDB()
	models.Migrate(db)
	g := generate.NewGenerate(db, 10)
	for i := 0; i < 100; i++ {
		err := g.InsertBatch(1000)
		if err != nil {
			log.Println("批量插入失败:", err)
		}
		time.Sleep(time.Millisecond * 50)
	}
}
