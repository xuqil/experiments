package main

import (
	"github.com/xuqil/experiments/migrate"
	"github.com/xuqil/experiments/migrate/generate"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
	"time"
)

func main() {
	db := InitDB()
	err := db.AutoMigrate(&migrate.User{})
	if err != nil {
		log.Fatalln(err)
	}
	g := generate.NewGenerate(db, 10)
	for {
		err := g.Insert()
		if err != nil {
			log.Println("插入失败:", err)
		}
		err = g.InsertBatch(10)
		if err != nil {
			log.Println("批量插入失败:", err)
		}
		err = g.Update()
		if err != nil {
			log.Println("批量更新失败:", err)
		}
		err = g.Delete()
		if err != nil {
			log.Println("删除更新失败:", err)
		}
		time.Sleep(time.Second * 5)
	}
}

func InitDB() *gorm.DB {
	// 源库
	sDsn := "root:Mysql_p1234@tcp(192.168.122.23:3306)/test?charset=utf8mb4&parseTime=True&loc=Local"
	dia := mysql.Open(sDsn)
	db, err := gorm.Open(dia)
	if err != nil {
		log.Fatalln(err)
	}

	return db
}
