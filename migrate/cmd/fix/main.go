package main

import (
	"context"
	"fmt"
	"github.com/xuqil/experiments/migrate/dwrite"
	"github.com/xuqil/experiments/migrate/models"
	"gorm.io/gorm"
	"log"
	"time"
)

func main() {
	sdb := dwrite.InitSourceDB() // 源库
	tdb := dwrite.InitTargetDB() // 目的库
	//if err := tdb.AutoMigrate(&models.User{}); err != nil {
	//	log.Fatalln(err)
	//}

	fix := NewFixData(sdb, tdb)
	err := fix.FullFix(context.Background(), 1000)
	if err != nil {
		log.Fatalln(err)
	}
}

type FixOptional func(f *FixData)

func WithSleep(d time.Duration) FixOptional {
	return func(f *FixData) {
		f.d = d
	}
}

type FixData struct {
	d   time.Duration // 休眠时长
	sdb *gorm.DB      // 源库
	tdb *gorm.DB      // 目标库
}

func NewFixData(sdb *gorm.DB, tdb *gorm.DB, opts ...FixOptional) *FixData {
	f := &FixData{
		d:   time.Millisecond * 50,
		sdb: sdb,
		tdb: tdb,
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// FullFix 全量比对 fix
func (f *FixData) FullFix(ctx context.Context, batch int) error {
	var (
		first  = true
		prevID uint64
		sUsers []models.User
		tUsers []models.User
		err    error
	)

	for first || (len(sUsers) != 0 && len(tUsers) != 0) {
		//log.Println("source:", len(sUsers), "target:", len(tUsers))
		first = false
		// 从源库获取 User
		sUsers, err = models.FetchUserInterval(ctx, f.sdb, prevID, batch)
		if err != nil {
			return err
		}

		// 从目标库获取 User
		tUsers, err = models.FetchUserInterval(ctx, f.tdb, prevID, batch)
		if err != nil {
			return err
		}

		// 转为 map 类型
		sum := ParseUsers(sUsers)
		tum := ParseUsers(tUsers)
		deleteIDList := make([]uint64, 0)
		createIDList := make([]models.User, 0)
		for id := range sum {
			if tu, exist := tum[id]; !exist { // 源库新建的
				//log.Println("从目的库中新建 ID:", id)
				createIDList = append(createIDList, *sum[id])
			} else { // 源库和目标库都存在
				su := sum[id]
				if su.Checksum() == tu.Checksum() { // 记录相同
					continue
				} else {
					log.Println("从目的库中更新 ID:", tu.ID)
					if er := su.Update(ctx, f.tdb); er != nil {
						log.Println(fmt.Errorf("更新目的库失败，ID: %d err:%w", tu.ID, er))
					}
				}
				delete(tum, tu.ID) // 移除已经记录的条目
			}
		}
		for id := range tum {
			if _, exist := sum[id]; !exist { // 源库已经删除
				//log.Println("从目的库中删除 ID:", id)
				deleteIDList = append(deleteIDList, id)
			}
		}

		if len(createIDList) > 0 {
			log.Println("从目标库中批量创建的数量:", len(createIDList))
			if er := models.CreateUserBatch(ctx, f.tdb, createIDList); er != nil {
				log.Println(fmt.Errorf("创建目的库失败， err:%w", er))
			}
		}
		if len(deleteIDList) > 0 {
			log.Println("1-从目标库中批量删除的数量:", len(deleteIDList))
			if er := models.DeleteUserBatch(ctx, f.tdb, deleteIDList); er != nil {
				log.Println(fmt.Errorf("删除目的库失败， err:%w", er))
			}
		}

		time.Sleep(f.d)
		prevID += uint64(batch)
	}

	// 处理剩下的记录
	for len(sUsers) != 0 {
		log.Println("source 处理剩下的记录")
		newUsers := make([]models.User, 0, len(sUsers))
		for i := range sUsers {
			//log.Println("从目的库中创建 ID:", sUsers[i].ID)
			newUsers = append(newUsers, sUsers[i])
		}
		if len(newUsers) > 0 {
			log.Println("批量插入目的库的数量:", len(newUsers))
			if er := models.CreateUserBatch(ctx, f.sdb, newUsers); er != nil {
				log.Println(fmt.Errorf("插入目的库失败 err:%w", er))
			}
		}
		// 从源库获取 User
		sUsers, err = models.FetchUserInterval(ctx, f.sdb, prevID, batch)
		if err != nil {
			return err
		}
		prevID += uint64(batch)
		time.Sleep(f.d)
	}

	for len(tUsers) != 0 {
		log.Println("target 处理剩下的记录")
		deleteIDList := make([]uint64, 0)
		for i := range tUsers {
			u := &tUsers[i]
			//log.Println("从目的库中删除 ID:", u.ID)
			deleteIDList = append(deleteIDList, u.ID)
		}
		if len(deleteIDList) > 0 {
			log.Println("2-从目标库中批量删除的数量:", len(deleteIDList))
			if er := models.DeleteUserBatch(ctx, f.tdb, deleteIDList); er != nil {
				log.Println(fmt.Errorf("删除目的库失败 err:%w", er))
			}
		}
		// 从目的库获取 User
		tUsers, err = models.FetchUserInterval(ctx, f.tdb, prevID, batch)
		if err != nil {
			return err
		}
		prevID += uint64(batch)
		time.Sleep(f.d)
	}

	return nil
}

// IncFix 增量修复
func (f *FixData) IncFix() error {
	panic("implement me")
}

// ParseUsers 将 []models.User 转为 map[uint64]*models.User 类型
func ParseUsers(users []models.User) map[uint64]*models.User {
	um := make(map[uint64]*models.User, len(users))
	for i := range users {
		user := &users[i]
		um[user.ID] = user
	}
	return um
}
