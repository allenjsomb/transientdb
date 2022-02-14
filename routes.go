package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

func Execute(db *Database) gin.HandlerFunc {
	return func(c *gin.Context) {
		body, err := ioutil.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusInternalServerError, err)
			return
		}

		sql := strings.TrimSpace(string(body))
		if sql == "" {
			c.JSON(http.StatusBadRequest, "No Body")
			return
		}

		res := db.Exec(SqlObject{sql, nil})
		c.JSON(http.StatusOK, res)
	}
}

func Query(db *Database) gin.HandlerFunc {
	return func(c *gin.Context) {
		body, err := ioutil.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusInternalServerError, err)
			return
		}

		sql := strings.TrimSpace(string(body))
		if sql == "" {
			c.JSON(http.StatusBadRequest, "No Body")
			return
		}

		res := db.Query(sql)
		c.JSON(http.StatusOK, res)
	}
}

func CountTableRecords(db *Database) gin.HandlerFunc {
	return func(c *gin.Context) {
		table := c.Params.ByName("table")
		res := db.Query(fmt.Sprintf("SELECT COUNT(*) as records FROM %s", table))
		c.JSON(http.StatusOK, res)
	}
}

func GetTables(db *Database) gin.HandlerFunc {
	return func(c *gin.Context) {
		res := db.Query(`SELECT name FROM sqlite_schema WHERE type ="table" AND name NOT LIKE "sqlite_%";`)
		c.JSON(http.StatusOK, res)
	}
}

func Vacuum(db *Database) gin.HandlerFunc {
	return func(c *gin.Context) {
		res := db.Exec(SqlObject{"VACUUM", nil})
		c.JSON(http.StatusOK, res)
	}
}

func GetAllTableRecords(db *Database) gin.HandlerFunc {
	return func(c *gin.Context) {
		table := c.Params.ByName("table")

		limit, err := strconv.Atoi(c.Query("limit"))
		if err != nil {
			limit = 100
		}

		offset, err := strconv.Atoi(c.Query("offset"))
		if err != nil {
			offset = 0
		}

		res := db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT %d, %d", table, offset, limit))
		c.JSON(http.StatusOK, res)
	}
}

func GetValueByField(db *Database) gin.HandlerFunc {
	return func(c *gin.Context) {
		table := c.Params.ByName("table")
		field := c.Params.ByName("field")
		value := c.Params.ByName("value")

		limit, err := strconv.Atoi(c.Query("limit"))
		if err != nil {
			limit = 100
		}

		offset, err := strconv.Atoi(c.Query("offset"))
		if err != nil {
			offset = 0
		}
		res := db.Query(fmt.Sprintf("SELECT * FROM %s WHERE %s = '%s' LIMIT %d, %d", table, field, value, offset, limit))
		c.JSON(http.StatusOK, res)
	}
}
