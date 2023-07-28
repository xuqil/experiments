package main

import (
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/xuqil/experiments/migrate/dwrite"
	"github.com/xuqil/experiments/migrate/generate"
	"github.com/xuqil/experiments/migrate/models"
	"gorm.io/gorm"
	"log"
	"net/http"
	"strconv"
	"time"
)

var db *gorm.DB
var pool *dwrite.DoubleWritePool

func main() {
	Init()
	go CrudTask(db)

	s := gin.Default()
	f := NewFakeServer(db, s, pool)
	f.Register()

	if err := s.Run(":8080"); err != nil {
		log.Fatalln(err)
	}
}

type FakeServer struct {
	db     *gorm.DB
	pool   *dwrite.DoubleWritePool
	server *gin.Engine
}

func NewFakeServer(db *gorm.DB, server *gin.Engine, pool *dwrite.DoubleWritePool) *FakeServer {
	return &FakeServer{
		db:     db,
		server: server,
		pool:   pool,
	}
}

// ChangeModel 修改双写模式
func (f *FakeServer) ChangeModel() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		modelStr := ctx.Query("model")
		m, err := strconv.Atoi(modelStr)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"msg": "错误的 model", "code": 1})
			return
		}
		model := dwrite.Mode(m)
		f.pool.SetMode(model)
		log.Println("model change to:", model)
		ctx.JSON(http.StatusOK, gin.H{"msg": "success", "code": 0})
	}
}

// GetUser 根据 ID 获取用户
func (f *FakeServer) GetUser() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		idStr := ctx.Param("id")
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"msg": "错误的 id", "code": 1})
			return
		}
		user := models.User{}
		err = f.db.WithContext(ctx.Request.Context()).Where("id=?", id).First(&user).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				ctx.JSON(http.StatusBadRequest, gin.H{"msg": "not found", "code": 1, "data": nil})
				return
			}
			log.Println("error:", err)
			ctx.JSON(http.StatusBadRequest, gin.H{"msg": "内部错误", "code": 1})
			return
		}
		ctx.JSON(http.StatusOK, gin.H{"msg": "success", "code": 0, "data": user})
	}
}

// Register 注册路由
func (f *FakeServer) Register() {
	f.server.GET("/model", f.ChangeModel())
	f.server.GET("/users/:id", f.GetUser())
}

// CrudTask 模拟业务的增删改操作
func CrudTask(db *gorm.DB) {
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

func Init() {
	db, pool = dwrite.InitDoubleWriteDB()
	pool.SetMode(dwrite.SourceWrite)
}
