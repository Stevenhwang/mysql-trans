package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
)

type MyEventHandler struct {
	canal.DummyEventHandler
}

func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {
	// log.Printf("%s, %s, %s, %v", e.Table.Schema, e.Table.Name, e.Action, e.Rows)
	// conn, _ := client.Connect("172.31.30.220:3307", "root", "test123456", "game_backend")
	// r, _ := conn.Execute(`insert into table (id, name) values (1, "abc")`)

	fields := ""
	values := ""
	fv := ""
	key := ""
	val := ""
	for ci, cc := range e.Table.Columns {
		// row := fmt.Sprintf("%s, %s, %d, %v", e.Table.Name, cc.Name, ci, e.Rows[len(e.Rows)-1][ci])
		// log.Println("row info: ", row)
		if ci == 0 {
			key = cc.Name
			val = fmt.Sprintf("%v", e.Rows[len(e.Rows)-1][ci])
		}
		if ci > 0 {
			fv += fmt.Sprintf("%s=%s, ", cc.Name, e.Rows[len(e.Rows)-1][ci])
		}
		fields += cc.Name + ", "
		values += fmt.Sprintf("%v, ", e.Rows[len(e.Rows)-1][ci])
	}

	switch e.Action {
	case canal.UpdateAction:
		log.Printf("update %v %v", key, val)
		// tfv := strings.TrimRight(fv, ", ")
		// update := fmt.Sprintf(`UPDATE %s SET %s WHERE %s=%s`, e.Table.Name, tfv, key, val)
		// log.Println(update)
	case canal.InsertAction:
		log.Println("insert")
		fs := strings.TrimRight(fields, ", ")
		vs := strings.TrimRight(values, ", ")
		insert := fmt.Sprintf(`insert into %s (%v) values (%v)`, e.Table.Name, fs, vs)
		log.Println(insert)
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
	// cfg.Dump.Tables = []string{"Sp_GameRecord", "Sp_PlayerGameHistory"}

	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Register a handler to handle RowsEvent
	c.SetEventHandler(&MyEventHandler{})

	startPos := mysql.Position{Name: "binlog.000114", Pos: 98570471}

	// Start canal
	c.RunFrom(startPos)
}
