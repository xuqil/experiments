package generate

import (
	"fmt"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/xuqil/experiments/migrate/internal/models"
	"gorm.io/gorm"
	"log"
	"sync"
	"time"
)

// Generate 生成测试数据
type Generate struct {
	db         *gorm.DB
	updateList []uint64
	deleteList []uint64
	capacity   int
	lock       sync.RWMutex
}

func NewGenerate(db *gorm.DB, capacity int) *Generate {
	if capacity < 10 {
		capacity = 10
	}
	return &Generate{
		db:         db,
		updateList: make([]uint64, 0),
		deleteList: make([]uint64, 0),
		capacity:   capacity,
	}
}

// InsertBatch 批量新建数据
func (g *Generate) InsertBatch(batch int) error {
	users := make([]*models.User, 0, batch)
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
	g.lock.Lock()
	defer g.lock.Unlock()

	err := g.db.Create(&user).Error
	if err != nil {
		return err
	}
	g.updateList = append(g.updateList, user.ID)
	g.deleteList = append(g.deleteList, user.ID)

	if len(g.updateList) >= g.capacity && g.capacity > 5 {
		g.updateList = append(g.updateList[:3], g.updateList[5:]...)
		if len(g.deleteList) > 2 {
			g.deleteList = g.deleteList[:1]
		}
	}

	log.Println("Inserted ID:", user.ID)
	return nil
}

// Update 更新
func (g *Generate) Update() error {
	g.lock.RLock()
	defer g.lock.RUnlock()

	now := time.Now()
	for i := 0; i < len(g.updateList); i++ {
		user := FakeUser()
		user.ID = g.updateList[i]
		user.Name = fmt.Sprintf("update-%d", user.ID)
		user.UpdatedAt = now
		err := g.db.Where("id=?", user.ID).Model(&models.User{}).Updates(user).Error
		if err != nil {
			return err
		}
		log.Println("Update ID:", user.ID)
	}
	return nil
}

func (g *Generate) Delete() error {
	g.lock.RLock()
	defer g.lock.RUnlock()

	for i := 0; i < len(g.deleteList); i++ {
		id := g.deleteList[i]
		err := g.db.Where("id=?", id).Delete(&models.User{}).Error
		if err != nil {
			return err
		}
		log.Println("Delete ID:", id)
	}
	return nil
}

func FakeUser() *models.User {
	return &models.User{
		Name:     gofakeit.Name(),
		Email:    gofakeit.Email(),
		Birthday: gofakeit.Date(),
	}
}
