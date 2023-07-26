package dwrite

import (
	"context"
	"database/sql"
	"fmt"
	"gorm.io/gorm"
	"log"
	"strings"
)

type Model int

const (
	SourceWrite Model = iota // 只写源库
	DoubleWrite              // 双写，先写和读源库，再写目标库
	Transition               // 双写，先写和读目标库，再写源库
	TargetWrite              //切换至目标库
)

// DoubleWritePool 实现双写
type DoubleWritePool struct {
	// model 的几种模式:
	// 1. source-write: 只写源库
	// 2. double-write: 双写，先写和读源库，再写目标库
	// 3. transition: 双写，先写和读目标库，再写源库
	// 4. target-write: 切换至目标库
	model  Model
	source gorm.ConnPool
	target gorm.ConnPool
}

func NewDoubleWritePool(source gorm.ConnPool, target gorm.ConnPool) *DoubleWritePool {
	return &DoubleWritePool{
		model:  SourceWrite,
		source: source,
		target: target,
	}
}

func (d *DoubleWritePool) SetMode(mode Model) {
	d.model = mode
}

func (d *DoubleWritePool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	log.Println("prepare")
	if d.model == TargetWrite {
		return d.target.PrepareContext(ctx, query)
	}
	return d.source.PrepareContext(ctx, query)
}

func (d *DoubleWritePool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	switch d.model {
	case SourceWrite: // 写源库
		log.Println("source-write")
		return d.source.ExecContext(ctx, query, args...)
	case DoubleWrite: // 双写，先写和读源库，再写目标库
		log.Println("double-write", query)
		result, err := d.source.ExecContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(query, "INSERT") { // 插入数据
			rows, er := result.RowsAffected()
			if er != nil {
				return result, er
			}
			lastInsertId, er := result.LastInsertId()
			if er != nil {
				return result, er
			}
			log.Println("插入操作 lastID:", lastInsertId, "rows:", rows)
			if rows > 1 { // 批量操作
				idList := make([]int64, 0, rows)
				for i := lastInsertId; i < lastInsertId+rows; i++ {
					idList = append(idList, i)
				}
				// INSERT INTO `users` (`name`,`email`,`birthday`,`created_at`,`updated_at`) VALUES (?,?,?,?,?),(?,?,?,?,?)
				s := strings.Split(query, " ")
				fields := s[3]      // 插入的字段
				placeholder := s[5] // 占位符
				log.Println("批量插入的ID:", idList)
				qBuffer := strings.Builder{}
				qBuffer.WriteString(s[0]) // INSERT
				qBuffer.WriteByte(' ')
				qBuffer.WriteString(s[1]) // INTO
				qBuffer.WriteByte(' ')
				qBuffer.WriteString(s[2]) // `user`
				qBuffer.WriteByte(' ')
				if !strings.Contains(fields, "`ID`") { // 创建时没有指定 ID
					// 插入 ID 字段
					qBuffer.WriteByte(fields[0])
					qBuffer.WriteString("`id`,")
					qBuffer.WriteString(fields[1:])
					qBuffer.WriteByte(' ')
					qBuffer.WriteString(s[4])
					qBuffer.WriteByte(' ')
					// 新增占位符
					fieldCount := 0
					for i, ph := range strings.Split(placeholder, ",(") {
						if i == 0 {
							fieldCount = len(strings.Split(ph, ","))
							qBuffer.WriteByte(ph[0])
							qBuffer.WriteString("?,")
							qBuffer.WriteString(ph[1:])
							continue
						}
						qBuffer.WriteString(",(?,")
						qBuffer.WriteString(ph)
					}
					newQuery := qBuffer.String()
					//log.Println("newQuery:", newQuery, "fieldCount:", fieldCount)
					//log.Println("args;", args)
					newArgs := make([]any, 0, len(args)+int(rows))
					for i := 0; i < len(args); i++ {
						if i%fieldCount == 0 {
							newArgs = append(newArgs, idList[i/fieldCount])
						}
						newArgs = append(newArgs, args[i])
					}
					_, er = d.target.ExecContext(ctx, newQuery, newArgs...)
					if er != nil {
						log.Println("插入目标库失败:", er)
					}
					//log.Println("newArgs:", newArgs)
					//newArgs[0] = idList[0]
					//newArgs[1] = args[0]
					//newArgs[2] = args[1]
					//newArgs[3] = args[2]
					//newArgs[4] = args[3]
					//newArgs[5] = args[4]
					//newArgs[6] = idList[1]
					//newArgs[7] = args[5]
					//for i := 0; i < len(args); i++ {
					//	if i%fieldCount == 0 {
					//		newArgs[i] = idList[len]
					//	}
					//}
				}
			} else {
				//	插入单条记录
				//  INSERT INTO `users` (`name`,`email`,`birthday`,`created_at`,`updated_at`) VALUES (?,?,?,?,?)
				s := strings.Split(query, " ")
				fields := s[3]                         // 插入的字段
				placeholder := s[5]                    // 占位符
				if !strings.Contains(fields, "`ID`") { // 创建时没有指定 ID
					// 插入 ID 字段
					fields = fmt.Sprintf("%c%s%s", fields[0], "`id`,", fields[1:])
					// 新增占位符
					placeholder = fmt.Sprintf("%c%s%s", placeholder[0], "?,", placeholder[1:])
				}
				s[3] = fields
				s[5] = placeholder
				newQuery := strings.Join(s, " ")
				newArgs := make([]any, len(args)+1)
				newArgs[0] = lastInsertId
				for i := range args {
					newArgs[i+1] = args[i]
				}
				log.Println("newQuery:", newQuery)
				log.Println("newArgs:", newArgs)
				_, er = d.target.ExecContext(ctx, newQuery, newArgs...)
				if er != nil {
					log.Println("插入目标库失败:", er)
				}
			}
		} else if strings.HasPrefix(query, "UPDATE") { // 更新数据
			var res sql.Result
			var er error
			res, er = d.source.ExecContext(ctx, query, args...)
			if er != nil {
				return res, er
			}
			_, er = d.target.ExecContext(ctx, query, args...)
			if er != nil {
				log.Println("更新目标库失败:", er)
			}
		}
		//go func() {
		//	// 拿到最后插入的 ID
		//	lastInsertId, er := r.LastInsertId()
		//	if er != nil {
		//		log.Println("写目标库发生错误：", er)
		//	}
		//	log.Println("ID:", lastInsertId)
		//	// TODO:写目标库
		//	log.Println("ID:", lastInsertId)
		//}()
		return result, err
	case Transition: // 双写，先写和读目标库，再写源库
		log.Println("transition")
		r, err := d.target.ExecContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		go func() {
			// 拿到最后插入的 ID
			id, er := r.LastInsertId()
			if er != nil {
				log.Println("写目标库发生错误：", er)
			}
			// TODO:写源库
			log.Println("ID:", id)
		}()
	case TargetWrite: // 写目标库
		log.Println("target-write")
		return d.target.ExecContext(ctx, query, args...)
	}
	log.Println("source-write")
	return d.source.ExecContext(ctx, query, args...)
}

func (d *DoubleWritePool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	switch d.model {
	case SourceWrite, DoubleWrite:
		return d.source.QueryContext(ctx, query, args...)
	case Transition:
		return d.target.QueryContext(ctx, query, args...)
	case TargetWrite:
		return d.target.QueryContext(ctx, query, args...)
	default:
		return d.source.QueryContext(ctx, query, args...)
	}
}

func (d *DoubleWritePool) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	switch d.model {
	case SourceWrite, DoubleWrite:
		return d.source.QueryRowContext(ctx, query, args...)
	case Transition:
		return d.target.QueryRowContext(ctx, query, args...)
	case TargetWrite:
		return d.target.QueryRowContext(ctx, query, args...)
	default:
		return d.source.QueryRowContext(ctx, query, args...)
	}
}

//// ExecContext 填充`ID`，写目标库或者源库
//func ExecContext(c gorm.ConnPool, ctx context.Context, query string, args ...any) (sql.Result, error) {
//
//}
