package main

import (
	"context"
	"fmt"
	"github.com/xuqil/experiments/migrate/dwrite"
	"github.com/xuqil/experiments/migrate/models"
	"gorm.io/gorm"
	"log"
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

type FixData struct {
	sdb *gorm.DB
	tdb *gorm.DB
}

func NewFixData(sdb *gorm.DB, tdb *gorm.DB) *FixData {
	return &FixData{
		sdb: sdb,
		tdb: tdb,
	}
}

// FullFix 全量比对 fix
func (f *FixData) FullFix(ctx context.Context, batch int) error {
	var (
		first   = true
		sPrevID uint64
		tPrevID uint64
		sUsers  []models.User
		tUsers  []models.User
		err     error
	)

	// TODO: 两者的 prevID 可能不一致，会导致查询的数据区间不同
	for first || (len(sUsers) != 0 && len(tUsers) != 0) {
		log.Println("source:", len(sUsers), "target:", len(tUsers))
		first = false
		// 从源库获取 User
		sUsers, err = models.FetchUserBatch(ctx, f.sdb, sPrevID, batch)
		if err != nil {
			return err
		}
		// 更新 prevID
		if len(sUsers) > 0 {
			sPrevID = sUsers[len(sUsers)-1].ID
		}

		// 从目标库获取 User
		tUsers, err = models.FetchUserBatch(ctx, f.tdb, tPrevID, batch)
		if err != nil {
			return err
		}
		// 更新 prevID
		if len(tUsers) > 0 {
			tPrevID = tUsers[len(tUsers)-1].ID
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
					//log.Println("从目的库中更新 ID:", tu.ID)
					if er := su.Save(ctx, f.tdb); er != nil {
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
			//if er := models.DeleteUserBatch(ctx, f.tdb, deleteIDList); er != nil {
			//	log.Println(fmt.Errorf("删除目的库失败， err:%w", er))
			//}
		}
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
		sUsers, err = models.FetchUserBatch(ctx, f.sdb, sPrevID, batch)
		if err != nil {
			return err
		}
		if len(sUsers) > 0 {
			sPrevID = sUsers[len(sUsers)-1].ID
		}
	}

	//for len(tUsers) != 0 {
	//	log.Println("target 处理剩下的记录")
	//	deleteIDList := make([]uint64, 0)
	//	for i := range tUsers {
	//		u := &tUsers[i]
	//		//log.Println("从目的库中删除 ID:", u.ID)
	//		deleteIDList = append(deleteIDList, u.ID)
	//	}
	//	if len(deleteIDList) > 0 {
	//		log.Println("2-从目标库中批量删除的数量:", len(deleteIDList))
	//		if er := models.DeleteUserBatch(ctx, f.tdb, deleteIDList); er != nil {
	//			log.Println(fmt.Errorf("删除目的库失败 err:%w", er))
	//		}
	//	}
	//	// 从目的库获取 User
	//	tUsers, err = models.FetchUserBatch(ctx, f.tdb, tPrevID, batch)
	//	if err != nil {
	//		return err
	//	}
	//	if len(tUsers) > 0 {
	//		tPrevID = tUsers[len(tUsers)-1].ID
	//	}
	//}

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
