package models

import (
	"context"
	"crypto/md5"
	"fmt"
	"gorm.io/gorm"
	"io"
	"log"
	"time"
)

type User struct {
	ID        uint64
	Name      string
	Email     string
	Birthday  time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Checksum 校验和
func (u *User) Checksum() string {
	s := fmt.Sprintf("%s%s%s%s%s", u.Name, u.Email, u.Birthday, u.CreatedAt, u.UpdatedAt)
	return MD5Checksum(s)
}

// Create 创建用户
func (u *User) Create(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Create(u).Error
}

// Save 更新用户
func (u *User) Save(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Save(u).Error
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

// Migrate 数据库表构造
func Migrate(db *gorm.DB) {
	if err := db.AutoMigrate(&User{}); err != nil {
		log.Fatalln(err)
	}
}

// MD5Checksum MD5 实现的校验和
func MD5Checksum(val string) string {
	h := md5.New()
	_, _ = io.WriteString(h, val)
	return string(h.Sum(nil))
}