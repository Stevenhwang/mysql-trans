package main

import (
	"fmt"
	"log"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/huandu/go-sqlbuilder"
)

type MyEventHandler struct {
	canal.DummyEventHandler
}

func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {
	fields := []string{}
	values := []interface{}{}
	table := fmt.Sprintf("%s.%s", e.Table.Schema, e.Table.Name)

	for ci, cc := range e.Table.Columns {
		fields = append(fields, cc.Name)
		values = append(values, e.Rows[len(e.Rows)-1][ci])
	}

	switch e.Action {
	case canal.UpdateAction:
		log.Printf("update")
	case canal.InsertAction:
		log.Println("insert")
		ib := sqlbuilder.NewInsertBuilder()
		ib.InsertInto(table)
		ib.Cols(fields...)
		ib.Values(values...)
		sql, args := ib.Build()
		query, err := sqlbuilder.MySQL.Interpolate(sql, args)
		fmt.Println(query)
		fmt.Println(err)
		// dest mysql conn
		conn, err := client.Connect("172.31.30.220:3307", "root", "test123456", "game_backend")
		defer conn.Close()
		if err != nil {
			log.Fatal(err)
		}
		_, err = conn.Execute(sql, args...)
		if err != nil {
			log.Fatal(err)
		}
	case canal.DeleteAction:
		log.Println("delete")
	}

	return nil
}

func (h *MyEventHandler) String() string {
	return "MyEventHandler"
}

func main() {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = "127.0.0.1:3306"
	cfg.User = "root"
	cfg.Password = "jenkins"

	cfg.Dump.TableDB = "game_backend"

	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Register a handler to handle RowsEvent
	c.SetEventHandler(&MyEventHandler{})

	startPos := mysql.Position{Name: "binlog.000114", Pos: 398967830}

	// Start canal
	c.RunFrom(startPos)
}
