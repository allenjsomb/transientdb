package main

import (
	"database/sql"
	"strconv"
	"sync"

	log "github.com/sirupsen/logrus"
)

type Database struct {
	dsn   string
	link  *sql.DB
	mutex *sync.Mutex
}

type Response struct {
	Success bool        `json:"success"`
	Result  interface{} `json:"result"`
}

type SqlObject struct {
	Sql   string
	Param []interface{}
}

func NewDatabase(dsn string) *Database {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		log.Fatal(err)
	}
	return &Database{dsn, db, &sync.Mutex{}}
}
func (db *Database) Exec(q SqlObject) Response {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	var rtn Response
	rtn.Success = false

	stmt, err := db.link.Prepare(q.Sql)
	if err != nil {
		log.Error("Prepare(...) ", err.Error())
		rtn.Result = err.Error()
		return rtn
	}
	defer stmt.Close()

	result, err := stmt.Exec(q.Param...)
	if err != nil {
		log.Error("Prepare(...) ", err.Error())
		rtn.Result = err.Error()
		return rtn
	}

	cnt, err := result.RowsAffected()
	if err != nil {
		log.Error("result.RowsAffected() ", err.Error())
		rtn.Result = err.Error()
		return rtn
	} else {
		rtn.Success = true
		rtn.Result = []interface{}{cnt}
	}

	return rtn
}

func (db *Database) Query(sqlString string) Response {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	var rtn Response
	rtn.Success = false
	rows, err := db.link.Query(sqlString)

	if err != nil {
		log.Error("Query(...) ", err.Error())
		rtn.Result = err.Error()
		return rtn
	}

	colTypes, err := rows.ColumnTypes()

	columns, err := rows.Columns()
	if err != nil {
		log.Error("rows.Columns() ", err.Error())
		rtn.Result = err.Error()
		return rtn
	}

	values := make([]sql.RawBytes, len(columns))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var results []map[string]interface{}

	rowIdx := 0
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			log.Error("rows.Scan(...) ", err.Error())
		}

		obj := make(map[string]interface{})
		var value interface{}
		for i, col := range values {
			if col == nil {
				value = nil
			} else {
				switch colTypes[i].DatabaseTypeName() {
				case "INTEGER":
					value, err = strconv.Atoi(string(col))
					if err != nil {
						value = nil
					}
				case "BOOLEAN":
					value, err = strconv.ParseBool(string(col))
					if err != nil {
						value = false
					}
				default:
					value = string(col)
				}
			}

			obj[columns[i]] = value
		}
		results = append(results, obj)
		rowIdx += 1
	}

	if err = rows.Err(); err != nil {
		log.Error("rows.Err() ", err.Error())
	}

	rtn.Success = true
	rtn.Result = results

	err = rows.Close()
	if err != nil {
		log.Error("rows.Close() ", err.Error())
	}

	return rtn
}

func (db *Database) Close() {
	db.link.Close()
}
