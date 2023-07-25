package main

import (
	"database/sql"
	"github.com/xuqil/experiments/migrate"
	"github.com/xuqil/experiments/migrate/generate"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
)

func main() {
	db := InitDB()
	err := db.AutoMigrate(&migrate.User{})
	if err != nil {
		log.Fatalln(err)
	}
	//user := migrate.User{Name: "Jinzhu", Birthday: time.Now()}
	//err = db.Create(&user).Error
	//if err != nil {
	//	log.Fatalln(err)
	//}
	//log.Println(user.ID)

	batch := 10
	users := make([]*migrate.User, 0, batch)
	for i := 0; i < batch; i++ {
		user := generate.FakeUser()
		users = append(users, user)
	}
	result := db.Create(users)
	log.Println("InsertBatch ID:", result.RowsAffected)

	//user := migrate.User{}
	//log.Println("hello")
	//err := db.WithContext(context.Background()).Find(&user).Where("id=?", 200).Error
	//if err != nil {
	//	log.Fatalln(err)
	//}
}

func InitDB() *gorm.DB {
	// 源库
	sDsn := "root:Mysql_p1234@tcp(192.168.122.23:3306)/test?charset=utf8mb4&parseTime=True&loc=Local"
	// 目标库
	tDsn := "root:Mysql_p1234@tcp(192.168.122.22:3306)/test?charset=utf8mb4&parseTime=True&loc=Local"
	sdb, err := sql.Open("mysql", sDsn)
	if err != nil {
		log.Fatalln(err)
	}
	sdb.SetMaxIdleConns(20)
	sdb.SetMaxOpenConns(100)

	tdb, err := sql.Open("mysql", tDsn)
	if err != nil {
		log.Fatalln(err)
	}
	tdb.SetMaxIdleConns(20)
	tdb.SetMaxOpenConns(100)

	pool := migrate.NewDoubleWritePool(sdb, tdb)
	pool.SetMode("double-write")
	//pool.SetMode("target-write")
	dial := mysql.New(mysql.Config{
		Conn: pool,
	})

	db, err := gorm.Open(dial, &gorm.Config{})
	if err != nil {
		log.Fatalln(err)
	}

	return db
}
