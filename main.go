package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/nats-io/nats.go"
)

type Config struct {
	Dsn     string
	Table   string
	Js      string
	Subject string
}

var startFrom int64

func init() {
	flag.Int64Var(&startFrom, "startfrom", -1, "Starting Id to stream")
}
func main() {
	flag.Parse()
	if startFrom == -1 {
		log.Fatalf("Need startfrom ")
	}
	filename, err := os.Open("config.json")
	if err != nil {
		panic(err)
	}
	conf := &Config{}
	err = json.NewDecoder(filename).Decode(conf)
	if err != nil {
		panic(err)
	}
	db, err := sql.Open("mysql", conf.Dsn)
	if err != nil {
		log.Printf("%q", err)
	}
	nc, _ := nats.Connect(conf.Js)
	js, _ := nc.JetStream()

	var id int64 = startFrom
	for {
		rows, err := db.Query(fmt.Sprintf("select * from %s where id=?", conf.Table), id)
		if err != nil {
			panic(err)
		}
		val, types := scanRow(rows)
		b := jsonize(val, types)
		puback, err := js.Publish(conf.Subject, []byte(b.String()))
		if err != nil {
			panic(err)
		}
		fmt.Printf("Last sequence %d\n", puback.Sequence)
		id = id + 1
	}
}

func scanRow(rows *sql.Rows) ([]interface{}, []*sql.ColumnType) {
	rows.Next()
	columnTypes, _ := rows.ColumnTypes()
	count := len(columnTypes)
	scanArgs := make([]interface{}, count)
	for i, v := range columnTypes {

		switch v.DatabaseTypeName() {
		case "VARCHAR", "TEXT", "CHAR":
			scanArgs[i] = new(sql.NullString)
		case "BOOL":
			scanArgs[i] = new(sql.NullBool)
		case "INTEGER", "INT", "SMALLINT", "TINYINT", "MEDIUMINT", "BIGINT":
			scanArgs[i] = new(sql.NullInt64)
		case "DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR":
			scanArgs[i] = new(sql.NullString)
		default:
			scanArgs[i] = new(sql.NullString)
		}
	}
	err := rows.Scan(scanArgs...)
	if err != nil {
		panic(err)
	}
	return scanArgs, columnTypes
}

func jsonize(val []interface{}, ct []*sql.ColumnType) strings.Builder {
	var b strings.Builder
	b.WriteString("{")
	for i, v := range ct {
		b.WriteString(fmt.Sprintf("\"%s\":", v.Name()))
		if val[i] == nil {
			b.WriteString("null")
			continue
		}
		switch v.DatabaseTypeName() {
		case "VARCHAR", "TEXT", "CHAR":
			b.WriteString(nullString(val[i].(*sql.NullString)))
		case "BOOL":
			b.WriteString(nullBool(val[i].(*sql.NullBool)))
		case "INTEGER", "INT", "SMALLINT", "TINYINT", "MEDIUMINT", "BIGINT":
			b.WriteString(nullInt64(val[i].(*sql.NullInt64)))
		case "DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR":
			b.WriteString((nullString(val[i].(*sql.NullString))))
		default:
			b.WriteString(nullString(val[i].(*sql.NullString)))
		}
		if i+1 != len(val) {
			b.WriteString(",")
		}
	}
	b.WriteString("}")
	return b
}

func nullString(n *sql.NullString) string {
	if !n.Valid {
		return "null"
	} else {
		return fmt.Sprintf("\"%s\"", n.String)
	}
}

func nullBool(n *sql.NullBool) string {
	if !n.Valid {
		return "null"
	} else {
		return strconv.FormatBool(n.Bool)
	}
}

func nullInt64(n *sql.NullInt64) string {
	if !n.Valid {
		return "null"
	} else {
		return strconv.FormatInt(n.Int64, 10)
	}
}

func nullTime(n *sql.NullTime) string {
	if !n.Valid {
		return "null"
	} else {
		return n.Time.String()
	}
}
