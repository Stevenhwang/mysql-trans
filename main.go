package main

import (
	"fmt"
	"log"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/huandu/go-sqlbuilder"
)

// get dest mysql conn
func getConn() (*client.Conn, error) {
	conn, err := client.Connect("172.31.30.220:3307", "root", "test123456", "game_backend")
	return conn, err
}

type MyEventHandler struct {
	canal.DummyEventHandler
}

func (h *MyEventHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	record := fmt.Sprintf("%s %d", pos.Name, pos.Pos)
	log.Println("OnPosSynced: ", record)
	return nil
}

func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {
	table := fmt.Sprintf("%s.%s", e.Table.Schema, e.Table.Name)

	switch e.Action {
	case canal.UpdateAction:
		log.Printf("update")
		key := ""
		var val interface{}
		ub := sqlbuilder.NewUpdateBuilder()
		ub.Update(table)
		for ci, cc := range e.Table.Columns {
			if ci == 0 {
				key = cc.Name
				val = e.Rows[len(e.Rows)-1][ci]
			} else {
				ub.SetMore(ub.Assign(cc.Name, e.Rows[len(e.Rows)-1][ci]))
			}
		}
		ub.Where(ub.Equal(key, val))
		sql, args := ub.Build()
		query, err := sqlbuilder.MySQL.Interpolate(sql, args)
		fmt.Println(query)
		fmt.Println(err)
		// dest mysql conn
		conn, err := getConn()
		defer conn.Close()
		if err != nil {
			log.Fatal(err)
		}
		_, err = conn.Execute(sql, args...)
		if err != nil {
			log.Println(err)
		}
	case canal.InsertAction:
		log.Println("insert")
		fields := []string{}
		values := []interface{}{}
		for ci, cc := range e.Table.Columns {
			fields = append(fields, cc.Name)
			values = append(values, e.Rows[len(e.Rows)-1][ci])
		}
		ib := sqlbuilder.NewInsertBuilder()
		ib.InsertInto(table)
		ib.Cols(fields...)
		ib.Values(values...)
		sql, args := ib.Build()
		query, err := sqlbuilder.MySQL.Interpolate(sql, args)
		fmt.Println(query)
		fmt.Println(err)
		// dest mysql conn
		conn, err := getConn()
		defer conn.Close()
		if err != nil {
			log.Fatal(err)
		}
		_, err = conn.Execute(sql, args...)
		if err != nil {
			log.Println(err)
		}
	case canal.DeleteAction:
		log.Println("delete")
		db := sqlbuilder.NewDeleteBuilder()
		db.DeleteFrom(table)
		key := ""
		var val interface{}
		for ci, cc := range e.Table.Columns {
			if ci == 0 {
				key = cc.Name
				val = e.Rows[len(e.Rows)-1][ci]
			}
		}
		db.Where(db.Equal(key, val))
		sql, args := db.Build()
		query, err := sqlbuilder.MySQL.Interpolate(sql, args)
		fmt.Println(query)
		fmt.Println(err)
		// dest mysql conn
		conn, err := getConn()
		defer conn.Close()
		if err != nil {
			log.Fatal(err)
		}
		_, err = conn.Execute(sql, args...)
		if err != nil {
			log.Println(err)
		}
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
