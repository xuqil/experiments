package main

import (
	"context"
	"github.com/withlin/canal-go/client"
	"github.com/xuqil/experiments/migrate/internal/conf"
	"github.com/xuqil/experiments/migrate/internal/fix"
	"log"
	"time"
)

func main() {
	sdb := conf.InitSourceDB() // 源库
	tdb := conf.InitTargetDB() // 目标库
	//models.Migrate(tdb)

	connector := client.NewSimpleCanalConnector("127.0.0.1", 11111, "", "",
		"example", 60000, 60*60*1000)
	err := connector.Connect()
	if err != nil {
		log.Fatalln(err)
	}

	// 切换到目标库前，以源库为准
	f := fix.NewFixData(sdb, tdb, fix.WithSleep(time.Second*1), fix.WithCanal(connector))
	//err := f.FixFull(context.Background(), 1000)
	//if err != nil {
	//	log.Fatalln(err)
	//}

	//err = f.FixIncByUpdatedAt(context.Background())
	//if err != nil {
	//	log.Fatalln(err)
	//}

	err = f.FixIncByCDC(context.Background(), 100)
	if err != nil {
		log.Fatalln(err)
	}
}
