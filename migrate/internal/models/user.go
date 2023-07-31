package models

import (
	"context"
	"fmt"
	"github.com/xuqil/experiments/migrate/pkg/util"
	"gorm.io/gorm"
	"time"
)

type User struct {
	ID        uint64
	Name      string
	Email     string
	Birthday  time.Time
	CreatedAt time.Time
	UpdatedAt time.Time `gorm:"index"`
}

// Checksum 校验和
func (u *User) Checksum() string {
	s := fmt.Sprintf("%s%s%s%s%s", u.Name, u.Email, u.Birthday, u.CreatedAt, u.UpdatedAt)
	return util.MD5Checksum(s)
}

// Create 创建用户
func (u *User) Create(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Create(u).Error
}

// Update 更新用户，注意不触发 UpdatedAt 自动更新
func (u *User) Update(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).UpdateColumns(u).Error
}

// Delete 删除用户
func (u *User) Delete(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Delete(u).Error
}

// CreateUserBatch 批量创建用户
func CreateUserBatch(ctx context.Context, db *gorm.DB, users []User) error {
	return db.WithContext(ctx).Create(&users).Error
}

// DeleteUserBatch 批量删除用户
func DeleteUserBatch(ctx context.Context, db *gorm.DB, idList []uint64) error {
	return db.WithContext(ctx).Delete(&User{}, idList).Error
}

// FetchUserBatch 批量获取用户
func FetchUserBatch(ctx context.Context, db *gorm.DB, prevID uint64, limit int) (users []User, err error) {
	err = db.WithContext(ctx).Where("id>?", prevID).Order("id").Limit(limit).Find(&users).Error
	return
}

// FetchUserInterval 按区间批量获取用户
func FetchUserInterval(ctx context.Context, db *gorm.DB, startID uint64, limit int) (users []User, err error) {
	err = db.WithContext(ctx).Where("id>=?", startID).Where("id<?", startID+uint64(limit)).
		Order("id").Find(&users).Error
	return
}

// FetchUserByUpdatedAt 按更新时间获取用户
func FetchUserByUpdatedAt(ctx context.Context, db *gorm.DB, updateAt time.Time) (users []User, err error) {
	err = db.WithContext(ctx).Where("updated_at>?", updateAt).Order("id").Find(&users).Error
	return
}

// FetchUserByIDList 按 ID 获取用户
func FetchUserByIDList(ctx context.Context, db *gorm.DB, idList []uint64) (users []User, err error) {
	err = db.WithContext(ctx).Find(&users, idList).Error
	return
}

// FetchUserByID 按 ID 获取用户
func FetchUserByID(ctx context.Context, db *gorm.DB, id uint64) (user User, err error) {
	err = db.WithContext(ctx).Where("id=?", id).First(&user).Error
	return
}
