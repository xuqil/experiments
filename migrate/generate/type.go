package generate

import (
	"fmt"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/xuqil/experiments/migrate"
	"gorm.io/gorm"
	"log"
)

type Generate struct {
	db         *gorm.DB
	updateList []uint
	deleteList []uint
	capacity   int
}

func NewGenerate(db *gorm.DB, capacity int) *Generate {
	if capacity < 10 {
		capacity = 10
	}
	return &Generate{
		db:         db,
		updateList: make([]uint, 0, capacity),
		deleteList: make([]uint, 0, capacity),
		capacity:   capacity,
	}
}

// InsertBatch 批量新建数据
func (g *Generate) InsertBatch(batch int) error {
	users := make([]*migrate.User, 0, batch)
	for i := 0; i < batch; i++ {
		user := FakeUser()
		users = append(users, user)
	}
	result := g.db.Create(users)
	log.Println("InsertBatch ID:", result.RowsAffected)
	return result.Error
}

// Insert 新建数据
func (g *Generate) Insert() error {
	user := FakeUser()
	err := g.db.Create(&user).Error
	if err != nil {
		return err
	}
	if len(g.updateList) >= g.capacity {
		if g.capacity < 5 {
			return err
		}
		g.updateList = append(g.updateList[:3], g.updateList[5:]...)
		g.deleteList = append(g.deleteList[:1], g.deleteList[2:]...)
	}
	g.updateList = append(g.updateList, user.ID)
	g.deleteList = append(g.deleteList, user.ID)
	log.Println("Inserted ID:", user.ID)
	return nil
}

// Update 更新
func (g *Generate) Update() error {
	for i := 0; i < len(g.updateList); i++ {
		user := FakeUser()
		user.ID = g.updateList[i]
		user.Name = fmt.Sprintf("update-%d", user.ID)
		err := g.db.Where("id=?", user.ID).Model(&migrate.User{}).Updates(user).Error
		if err != nil {
			return err
		}
		log.Println("Update ID:", user.ID)
	}
	return nil
}

func (g *Generate) Delete() error {
	for i := 0; i < len(g.deleteList); i++ {
		id := g.deleteList[i]
		err := g.db.Where("id=?", id).Delete(&migrate.User{}).Error
		if err != nil {
			return err
		}
		log.Println("Delete ID:", id)
	}
	return nil
}

func FakeUser() *migrate.User {
	return &migrate.User{
		Name:     gofakeit.Name(),
		Email:    gofakeit.Email(),
		Birthday: gofakeit.Date(),
	}
}
