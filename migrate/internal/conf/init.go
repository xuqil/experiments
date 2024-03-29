package conf

import (
	"database/sql"
	"github.com/xuqil/experiments/migrate/pkg/dwrite"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"os"
	"time"
)

var (
	sDsn = "root:Mysql_1234@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local"
	tDsn = "root:Mysql_1234@tcp(127.0.0.1:3307)/test?charset=utf8mb4&parseTime=True&loc=Local"
)

var l = logger.New(
	log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
	logger.Config{
		SlowThreshold:             time.Second,   // Slow SQL threshold
		LogLevel:                  logger.Silent, // Log level
		IgnoreRecordNotFoundError: true,          // Ignore ErrRecordNotFound error for logger
		ParameterizedQueries:      true,          // Don't include params in the SQL log
		Colorful:                  false,         // Disable color
	},
)

// InitSourceDB 初始化源库 *gorm.DB
func InitSourceDB() *gorm.DB {
	dia := mysql.Open(sDsn)
	db, err := gorm.Open(dia, &gorm.Config{
		Logger: l,
	})
	if err != nil {
		log.Fatalln(err)
	}
	return db
}

// InitTargetDB 初始化目标库 *gorm.DB
func InitTargetDB() *gorm.DB {
	dia := mysql.Open(tDsn)
	db, err := gorm.Open(dia, &gorm.Config{
		Logger: l,
	})
	if err != nil {
		log.Fatalln(err)
	}
	return db
}

// InitDoubleWriteDB 初始化双写 *gorm.DB 和 *DoubleWritePool
func InitDoubleWriteDB() (*gorm.DB, *dwrite.DoubleWritePool) {
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

	pool := dwrite.NewDoubleWritePool(sdb, tdb)
	pool.SetMode(dwrite.SourceWrite)
	dial := mysql.New(mysql.Config{
		Conn: pool,
	})

	db, err := gorm.Open(dial, &gorm.Config{
		Logger: l,
	})
	if err != nil {
		log.Fatalln(err)
	}

	return db, pool
}
