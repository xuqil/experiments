package models

import (
	"gorm.io/gorm"
	"log"
)

// Migrate 数据库表构造
func Migrate(db *gorm.DB) {
	if err := db.AutoMigrate(&User{}); err != nil {
		log.Fatalln(err)
	}
}
