package main

import (
	"fmt"
	"log"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
)

type MyEventHandler struct {
	canal.DummyEventHandler
}

func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {
	// log.Printf("%s, %s, %s, %v", e.Table.Schema, e.Table.Name, e.Action, e.Rows)
	for ci, cc := range e.Table.Columns {
		row := fmt.Sprintf("%s, %d, %v", cc.Name, ci, e.Rows[len(e.Rows)-1][ci])
		log.Println("row info: ", row)
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
	cfg.Dump.Tables = []string{"Sp_GameRecord", "Sp_PlayerGameHistory"}

	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Register a handler to handle RowsEvent
	c.SetEventHandler(&MyEventHandler{})

	startPos := mysql.Position{Name: "binlog.000114", Pos: 67061836}

	// Start canal
	c.RunFrom(startPos)
}
