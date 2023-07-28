package fix

import (
	"context"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/withlin/canal-go/client"
	pbe "github.com/withlin/canal-go/protocol/entry"
	"github.com/xuqil/experiments/migrate/internal/models"
	"gorm.io/gorm"
	"log"
	"time"
)

type Optional func(f *FixData)

func WithSleep(d time.Duration) Optional {
	return func(f *FixData) {
		f.d = d
	}
}

func WithUpdatedAt(u time.Time) Optional {
	return func(f *FixData) {
		f.updatedAt = u
	}
}

func WithCanal(c *client.SimpleCanalConnector) Optional {
	return func(f *FixData) {
		f.conn = c
	}
}

// FixData 用于校验和修目标数据库
// FixFull 和 FixIncByUpdatedAt 会对数据库造成压力，
// 可以考虑从“从库“（目标库和源库的从库，或其中之一的从库）批量获取数据
// 要是有数据不一致的情况，要从源库再次获取该数据，如何再更新目标库
type FixData struct {
	d         time.Duration                // 休眠时长
	updatedAt time.Time                    // 上次更新时间
	quit      chan struct{}                // 用于关闭增量更新
	sdb       *gorm.DB                     // 源库
	tdb       *gorm.DB                     // 目标库
	conn      *client.SimpleCanalConnector // canal
}

func NewFixData(sdb *gorm.DB, tdb *gorm.DB, opts ...Optional) *FixData {
	f := &FixData{
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

func (f *FixData) Close() {
	f.quit <- struct{}{}
}

// FixFull 全量比对 fix
func (f *FixData) FixFull(ctx context.Context, batchSize int) error {
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
func (f *FixData) FixIncByUpdatedAt(ctx context.Context) error {
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
func (f *FixData) fixByUpdatedAt(ctx context.Context) error {
	log.Println("增量校验，updatedAt:", f.updatedAt)
	// 从源库获取 User
	sUsers, err := models.FetchUserByUpdatedAt(ctx, f.sdb, f.updatedAt)
	if err != nil {
		return err
	}
	// 从目标库获取 User
	tUsers, err := models.FetchUserByUpdatedAt(ctx, f.tdb, f.updatedAt)
	if err != nil {
		return err
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
		} else {                                // 源库和目标库都存在
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
func (f *FixData) FixIncByCDC(ctx context.Context, batchSize int) error {
	err := f.conn.Subscribe("test.users")
	if err != nil {
		return err
	}

	for {
		message, er := f.conn.Get(int32(batchSize), nil, nil)
		if er != nil {
			return er
		}
		batchId := message.Id
		if batchId == -1 || len(message.Entries) <= 0 {
			time.Sleep(300 * time.Millisecond)
			fmt.Println("===没有数据了===")
			continue
		}
		printEntry(message.Entries)
	}
}

func printEntry(entrys []pbe.Entry) {

	for _, entry := range entrys {
		if entry.GetEntryType() == pbe.EntryType_TRANSACTIONBEGIN || entry.GetEntryType() == pbe.EntryType_TRANSACTIONEND {
			continue
		}
		rowChange := new(pbe.RowChange)

		err := proto.Unmarshal(entry.GetStoreValue(), rowChange)
		checkError(err)
		if rowChange != nil {
			eventType := rowChange.GetEventType()
			header := entry.GetHeader()
			fmt.Println(fmt.Sprintf("================> binlog[%s : %d],name[%s,%s], eventType: %s", header.GetLogfileName(), header.GetLogfileOffset(), header.GetSchemaName(), header.GetTableName(), header.GetEventType()))

			for _, rowData := range rowChange.GetRowDatas() {
				if eventType == pbe.EventType_DELETE {
					printColumn(rowData.GetBeforeColumns())
				} else if eventType == pbe.EventType_INSERT {
					printColumn(rowData.GetAfterColumns())
				} else {
					fmt.Println("-------> before")
					printColumn(rowData.GetBeforeColumns())
					fmt.Println("-------> after")
					printColumn(rowData.GetAfterColumns())
				}
			}
		}
	}
}
func printColumn(columns []*pbe.Column) {
	for _, col := range columns {
		fmt.Println(fmt.Sprintf("%s : %s  update= %t", col.GetName(), col.GetValue(), col.GetUpdated()))
	}
}
func checkError(err error) {
	if err != nil {
		log.Println("Fatal error:", err)
	}
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
