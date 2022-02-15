package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/ini.v1"

	"github.com/easonlin404/limit"
	"github.com/erikdubbelboer/gspt"
	"github.com/gin-gonic/gin"

	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

var db *Database
var cfg *ini.File

func configLogger(level string) {
	switch level {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "trace":
		log.SetLevel(log.TraceLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	default:
		log.SetLevel(log.InfoLevel)
	}

	formatter := &log.TextFormatter{}
	formatter.DisableLevelTruncation = true
	formatter.FullTimestamp = true
	formatter.PadLevelText = true
	formatter.DisableLevelTruncation = true
	formatter.TimestampFormat = "Mon Jan 02 2006 15:04:05.00000 MST"
	log.SetFormatter(formatter)
}

func initDatabase(dsn string) {
	log.Infof("Initializing db (%s)", dsn)
	db = NewDatabase(dsn)
}

func loadSchemas(schemaFolder string) {
	files, err := ioutil.ReadDir(schemaFolder)
	if err != nil {
		log.Error(err)
		return
	}

	for _, file := range files {
		log.Infof("Executing content of %s", file.Name())
		content, err := ioutil.ReadFile(fmt.Sprintf("%s/%s", schemaFolder, file.Name()))
		if err != nil {
			log.Error(err)
			continue
		}

		text := string(content)
		statements := strings.Split(text, ";")
		for _, statement := range statements {
			statement = strings.TrimSpace(statement)
			if statement == "" {
				continue
			}
			res := db.Exec(SqlObject{statement, nil})
			if !res.Success {
				log.Error(res.Result)
				continue
			}
		}
	}
}

func loadData(dataFolder string) {
	files, err := ioutil.ReadDir(dataFolder)
	if err != nil {
		log.Error(err)
		return
	}

	for _, file := range files {
		go func(file fs.FileInfo) {
			log.Infof("Loading data from %s", file.Name())
			fp, err := os.Open(fmt.Sprintf("%s/%s", dataFolder, file.Name()))
			if err != nil {
				log.Error(err)
				return
			}

			defer fp.Close()

			start := time.Now()

			var maxSize int64 = 500
			var count int64 = 0
			var headers []string
			var params [][]interface{}
			var sql string = ""
			var current int64 = 0
			csvReader := csv.NewReader(fp)
			for {
				rec, err := csvReader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Error(err)
					continue
				}

				if headers == nil {
					headers = rec
					log.Debugf("%s headers=%+v", file.Name(), headers)
					table := strings.TrimSuffix(filepath.Base(file.Name()), filepath.Ext(file.Name()))
					places := make([]string, len(headers))
					for i, _ := range headers {
						places[i] = "?"
					}
					sql = "INSERT INTO " + table + " (" + strings.Join(headers, ",") + ") VALUES (" + strings.Join(places, ",") + ")"
					params = make([][]interface{}, maxSize)
					for i := range params {
						params[i] = make([]interface{}, len(headers))
					}
					current = 0
					continue
				}

				for i, v := range rec {
					params[current][i] = v
				}

				current += 1

				if current%maxSize == 0 {
					res := db.ExecBatch(sql, params, current)
					if !res.Success {
						log.Error(res.Result)
						continue
					}
					count += res.Result.(int64)
					params = make([][]interface{}, maxSize)
					for i := range params {
						params[i] = make([]interface{}, len(headers))
					}
					current = 0
					continue
				}
			}

			if 0 < current {
				res := db.ExecBatch(sql, params, current)
				if !res.Success {
					log.Error(res.Result)
				}
				count += res.Result.(int64)
			}

			duration := time.Since(start)
			log.Infof("Loading of %s completed with %d records in %s", file.Name(), count, duration.String())
		}(file)
	}
}

func catchSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	s := <-c
	log.Info("Caught signal:", s, " - Shutting down.")
	db.Close()
	os.Exit(0)
}

func maintenance() {
	log.Info("Maintenance routine started")
	for {
		time.Sleep(60 * time.Second)
		//runtime.GC()
	}
}

func setupRouter(authToken string) *gin.Engine {
	r := gin.New()

	r.Use(TokenCheck(authToken))
	r.Use(gin.Recovery())
	r.Use(limit.Limit(200))

	r.GET("/table/:table", GetAllTableRecords(db))
	r.GET("/table/:table/:field/:value", GetValueByField(db))
	r.POST("/execute", Execute(db))
	r.POST("/query", Query(db))
	r.GET("/tables", GetTables(db))
	r.GET("/count/:table", CountTableRecords(db))
	r.GET("/vacuum", Vacuum(db))

	return r
}

func main() {
	app := &cli.App{
		Name:  "transientdb",
		Usage: "The Transient Database",
	}

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:     "cfg",
			Value:    "./config.ini",
			Usage:    "Configuration file for TransientDB",
			Required: false,
		},
	}

	app.Action = func(c *cli.Context) error {
		cfgFile := c.String("cfg")
		lcfg, err := ini.Load(cfgFile)
		if err != nil {
			return err
		}

		cfg = lcfg

		serverCfg := cfg.Section("server")
		configLogger(serverCfg.Key("log_level").MustString("info"))
		initDatabase(serverCfg.Key("dsn").MustString(":memory:"))

		log.Infof("Using %s file for configuration.", cfgFile)

		go catchSignal()
		go maintenance()

		tag := serverCfg.Key("tag").MustString("default")
		ip := serverCfg.Key("listen").MustString("0.0.0.0")
		port := serverCfg.Key("port").MustInt(8000)
		read_timeout := serverCfg.Key("read_timeout").MustInt(60)
		write_timeout := serverCfg.Key("write_timeout").MustInt(60)
		schemasFolder := serverCfg.Key("schemas_folder").MustString("")
		dataFolder := serverCfg.Key("data_folder").MustString("")
		if schemasFolder != "" {
			loadSchemas(schemasFolder)
			if dataFolder != "" {
				go loadData(dataFolder)
			}
		}

		gspt.SetProcTitle(fmt.Sprintf("TransientDB [%s] %s:%d", tag, ip, port))

		router := setupRouter(cfg.Section("auth").Key("token").String())
		server := &http.Server{
			Addr:           fmt.Sprintf("%s:%d", ip, port),
			Handler:        router,
			ReadTimeout:    time.Duration(read_timeout) * time.Second,
			WriteTimeout:   time.Duration(write_timeout) * time.Second,
			MaxHeaderBytes: 1 << 20,
		}
		server.ListenAndServe()

		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err.Error())
	}

}
