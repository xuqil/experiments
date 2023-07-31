package fix

import (
	"context"
	"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/withlin/canal-go/client"
	pbe "github.com/withlin/canal-go/protocol/entry"
	"github.com/xuqil/experiments/migrate/internal/models"
	"gorm.io/gorm"
	"log"
	"strconv"
	"time"
)

type Optional func(f *User)

func WithSleep(d time.Duration) Optional {
	return func(f *User) {
		f.d = d
	}
}

func WithUpdatedAt(u time.Time) Optional {
	return func(f *User) {
		f.updatedAt = u
	}
}

func WithCanal(c *client.SimpleCanalConnector) Optional {
	return func(f *User) {
		f.conn = c
	}
}

// User 用于校验和修目标数据库的表 user
// FixFull 和 FixIncByUpdatedAt 会对数据库造成压力，
// 可以考虑从“从库“（目标库和源库的从库，或其中之一的从库）批量获取数据
// 要是有数据不一致的情况，要从源库再次获取该数据，如何再更新目标库
type User struct {
	d         time.Duration                // 休眠时长
	updatedAt time.Time                    // 上次更新时间
	quit      chan struct{}                // 用于关闭增量更新
	sdb       *gorm.DB                     // 源库
	tdb       *gorm.DB                     // 目标库
	conn      *client.SimpleCanalConnector // canal
}

func NewFixUser(sdb *gorm.DB, tdb *gorm.DB, opts ...Optional) *User {
	f := &User{
		d:         time.Millisecond * 50,
		updatedAt: time.Now(),
		quit:      make(chan struct{}, 1),
		sdb:       sdb,
		tdb:       tdb,
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

func (f *User) Close() {
	f.quit <- struct{}{}
}

// FixFull 全量比对 fix
func (f *User) FixFull(ctx context.Context, batchSize int) error {
	var (
		prevID uint64
		sUsers []models.User
		tUsers []models.User
		err    error
	)

	// 从源库获取 User
	sUsers, err = models.FetchUserInterval(ctx, f.sdb, prevID, batchSize)
	if err != nil {
		return err
	}

	// 从目标库获取 User
	tUsers, err = models.FetchUserInterval(ctx, f.tdb, prevID, batchSize)
	if err != nil {
		return err
	}

	for len(sUsers) != 0 && len(tUsers) != 0 {
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
			log.Println("1-从目标库中批量创建的数量:", len(createIDList))
			if er := models.CreateUserBatch(ctx, f.tdb, createIDList); er != nil {
				log.Println(fmt.Errorf("1-插入目的库失败， err:%w", er))
			}
		}
		if len(deleteIDList) > 0 {
			log.Println("1-从目标库中批量删除的数量:", len(deleteIDList))
			if er := models.DeleteUserBatch(ctx, f.tdb, deleteIDList); er != nil {
				log.Println(fmt.Errorf("删除目的库失败， err:%w", er))
			}
		}

		time.Sleep(f.d)
		prevID += uint64(batchSize)
		// 从源库获取 User
		sUsers, err = models.FetchUserInterval(ctx, f.sdb, prevID, batchSize)
		if err != nil {
			return err
		}

		// 从目标库获取 User
		tUsers, err = models.FetchUserInterval(ctx, f.tdb, prevID, batchSize)
		if err != nil {
			return err
		}
	}

	prevID += uint64(batchSize)

	// 处理剩下的记录
	for len(sUsers) != 0 {
		log.Println("source 处理剩下的记录")
		newUsers := make([]models.User, 0, len(sUsers))
		for i := range sUsers {
			//log.Println("从目的库中创建 ID:", sUsers[i].ID)
			newUsers = append(newUsers, sUsers[i])
		}
		if len(newUsers) > 0 {
			log.Println("2-批量插入目的库的数量:", len(newUsers))
			if er := models.CreateUserBatch(ctx, f.tdb, newUsers); er != nil {
				log.Println(fmt.Errorf("2-插入目的库失败 err:%w", er))
			}
		}
		// 从源库获取 User
		sUsers, err = models.FetchUserInterval(ctx, f.sdb, prevID, batchSize)
		if err != nil {
			return err
		}
		prevID += uint64(batchSize)
		time.Sleep(f.d)
		log.Println("s:", len(sUsers), "t:", len(tUsers))
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
				log.Println(fmt.Errorf("2-删除目的库失败 err:%w", er))
			}
		}
		// 从目的库获取 User
		tUsers, err = models.FetchUserInterval(ctx, f.tdb, prevID, batchSize)
		if err != nil {
			return err
		}
		prevID += uint64(batchSize)
		time.Sleep(f.d)
	}

	return nil
}

// FixIncByUpdatedAt 根据 UpdatedAt 字段增量校验
func (f *User) FixIncByUpdatedAt(ctx context.Context) error {
	for {
		select {
		case <-f.quit:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := f.fixByUpdatedAt(ctx); err != nil {
				return err
			}
			time.Sleep(f.d)
		}
	}
}

// fixByUpdatedAt 根据 UpdatedAt 字段增量校验
// 注意要给字段 UpdatedAt 添加索引，如果根据 UpdatedAt 条件获取的数据量很大，可以分批处理
func (f *User) fixByUpdatedAt(ctx context.Context) error {
	log.Println("增量校验，updatedAt:", f.updatedAt)
	// 从源库获取 User
	sUsers, err := models.FetchUserByUpdatedAt(ctx, f.sdb, f.updatedAt)
	if err != nil {
		return err
	}
	IDList := make([]uint64, 0, len(sUsers))
	for i, _ := range sUsers {
		IDList = append(IDList, sUsers[i].ID)
	}
	// 从目标库获取 User
	tUsers := make([]models.User, 0)
	if len(IDList) > 0 {
		tUsers, err = models.FetchUserByIDList(ctx, f.tdb, IDList)
		if err != nil {
			return err
		}
	}

	// 转为 map 类型
	sum := ParseUsers(sUsers)
	tum := ParseUsers(tUsers)
	deleteIDList := make([]uint64, 0)
	createIDList := make([]models.User, 0)
	prevTime := f.updatedAt
	for id := range sum {
		su := sum[id]
		if su.UpdatedAt.After(prevTime) {
			prevTime = su.UpdatedAt
		}
		if tu, exist := tum[id]; !exist { // 源库新建的
			//log.Println("从目的库中新建 ID:", id)
			createIDList = append(createIDList, *sum[id])
		} else { // 源库和目标库都存在
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
	// 更新时间记录
	f.updatedAt = prevTime

	for id := range tum {
		if _, exist := sum[id]; !exist { // 源库已经删除
			//log.Println("从目的库中删除 ID:", id)
			deleteIDList = append(deleteIDList, id)
		}
	}

	if len(createIDList) > 0 {
		log.Println("1-从目标库中批量创建的数量:", len(createIDList))
		if er := models.CreateUserBatch(ctx, f.tdb, createIDList); er != nil {
			log.Println(fmt.Errorf("1-插入目的库失败， err:%w", er))
		}
	}
	if len(deleteIDList) > 0 {
		log.Println("1-从目标库中批量删除的数量:", len(deleteIDList))
		if er := models.DeleteUserBatch(ctx, f.tdb, deleteIDList); er != nil {
			log.Println(fmt.Errorf("删除目的库失败， err:%w", er))
		}
	}
	return nil
}

// FixIncByCDC 由 binlog 触发增量修复
func (f *User) FixIncByCDC(ctx context.Context, batchSize int) error {
	err := f.conn.Subscribe("test.users")
	if err != nil {
		return err
	}

	for {
		select {
		case <-f.quit:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			message, er := f.conn.Get(int32(batchSize), nil, nil)
			if er != nil {
				return er
			}
			batchId := message.Id
			if batchId == -1 || len(message.Entries) <= 0 {
				time.Sleep(f.d)
				continue
			}
			if er = f.fixByBinlog(ctx, message.Entries); er != nil {
				return err
			}
		}
	}
}

func (f *User) fixByBinlog(ctx context.Context, entries []pbe.Entry) error {
	for _, entry := range entries {
		if entry.GetEntryType() == pbe.EntryType_TRANSACTIONBEGIN || entry.GetEntryType() == pbe.EntryType_TRANSACTIONEND {
			continue
		}
		rowChange := new(pbe.RowChange)

		if err := proto.Unmarshal(entry.GetStoreValue(), rowChange); err != nil {
			return err
		}
		if rowChange == nil {
			continue
		}
		eventType := rowChange.GetEventType()
		header := entry.GetHeader()
		//db := header.GetSchemaName()
		//tb := header.GetTableName()
		fmt.Println(fmt.Sprintf("================> binlog[%s : %d],name[%s,%s], eventType: %s", header.GetLogfileName(), header.GetLogfileOffset(), header.GetSchemaName(), header.GetTableName(), header.GetEventType()))
		for _, rowData := range rowChange.GetRowDatas() {
			if eventType == pbe.EventType_DELETE { // 源表删除的数据
				id, er := parseID(rowData.GetBeforeColumns())
				if er != nil {
					log.Println(fmt.Errorf("获取 ID 失败 error:%w", er))
					continue
				}
				user := &models.User{ID: id}
				if er = user.Delete(ctx, f.tdb); er != nil {
					log.Println(fmt.Errorf("从目标库删除失败 ID:%d error:%w", id, er))
				} else {
					log.Println(fmt.Sprintf("从目标库删除成功 ID:%d", id))
				}
			} else if eventType == pbe.EventType_INSERT { // 源表新插入的数据
				id, er := parseID(rowData.GetAfterColumns())
				if er != nil {
					log.Println(fmt.Errorf("获取 ID 失败 error:%w", er))
					continue
				}
				// 先从源库获取插入数据
				sUser, er := models.FetchUserByID(ctx, f.sdb, id)
				if er != nil {
					log.Println("从源库获取数据失败 ID:", id)
					continue
				}
				// 然后从目标库中获取数据，如果没有或者不一致，则插入或者更新
				tUser, er := models.FetchUserByID(ctx, f.tdb, id)
				if er != nil {
					if errors.Is(er, gorm.ErrRecordNotFound) { // 目标库数据不存在
						if er = sUser.Create(ctx, f.tdb); er != nil {
							log.Println(fmt.Errorf("插入目标库失败 ID:%d error:%w", id, er))
						} else {
							log.Println(fmt.Sprintf("从目标库创建成功 ID:%d", id))
						}
					} else {
						log.Println(fmt.Errorf("从目标库获取数据失败 ID:%d error:%w", id, er))
					}
					continue
				}
				if tUser.Checksum() == sUser.Checksum() { // 如果两者数据相同，则不做更新
					log.Println("数据相同, ID:", id)
					continue
				}
				if er = sUser.Update(ctx, f.tdb); er != nil {
					log.Println(fmt.Errorf("更新目标库失败 ID:%d error:%w", id, er))
				} else {
					log.Println(fmt.Sprintf("从目标库更新成功 ID:%d", id))
				}
			} else { // 源表更新的数据
				id, er := parseID(rowData.GetAfterColumns())
				if er != nil {
					log.Println(fmt.Errorf("获取 ID 失败 error:%w", er))
					continue
				}
				// 先从源库获取插入数据
				sUser, er := models.FetchUserByID(ctx, f.sdb, id)
				if er != nil {
					log.Println("从源库获取数据失败 ID:", id)
					continue
				}
				// 然后从目标库中获取数据，如果没有或者不一致，则插入或者更新
				tUser, er := models.FetchUserByID(ctx, f.tdb, id)
				if er != nil {
					if errors.Is(er, gorm.ErrRecordNotFound) { // 目标库数据不存在
						if er = sUser.Create(ctx, f.tdb); er != nil {
							log.Println(fmt.Errorf("插入目标库失败 ID:%d error:%w", id, er))
						} else {
							log.Println(fmt.Sprintf("从目标库创建成功 ID:%d", id))
						}
					} else {
						log.Println(fmt.Errorf("从目标库获取数据失败 ID:%d error:%w", id, er))
					}
					continue
				}
				if tUser.Checksum() == sUser.Checksum() { // 如果两者数据相同，则不做更新
					continue
				}
				if er = sUser.Update(ctx, f.tdb); er != nil {
					log.Println(fmt.Errorf("更新目标库失败 ID:%d error:%w", id, er))
				} else {
					log.Println(fmt.Sprintf("从目标库更新成功 ID:%d", id))
				}
			}
		}
	}
	return nil
}

func parseID(columns []*pbe.Column) (uint64, error) {
	_, idStr := getID(columns)
	return strconv.ParseUint(idStr, 10, 64)
}

func getID(columns []*pbe.Column) (idName string, value string) {
	for _, col := range columns {
		if col.IsKey {
			return col.GetName(), col.GetValue()
		}
	}

	return
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
