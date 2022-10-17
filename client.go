package clickhouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/elastic/elastic-agent-libs/logp"
)

type client struct {
	log      *logp.Logger
	config   Config
	Conn     clickhouse.Conn
	observer outputs.Observer
	codec    codec.Codec
	index    string
}

func newClient(cfg Config, observer outputs.Observer, codec codec.Codec, index string) (*client, error) {
	c := &client{
		log:      logp.NewLogger("clickhouse"),
		config:   cfg,
		codec:    codec,
		observer: observer,
		index:    index,
	}
	return c, nil
}

func (c *client) Connect() error {

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: c.config.Host,
		Auth: clickhouse.Auth{
			Database: c.config.Db,
			Username: c.config.UserName,
			Password: c.config.PassWord,
		},
		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		},
		Debug: true,
		Debugf: func(format string, v ...interface{}) {
			fmt.Printf(format, v)
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:      time.Duration(10) * time.Second,
		MaxOpenConns:     5,
		MaxIdleConns:     5,
		ConnMaxLifetime:  time.Duration(10) * time.Minute,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
	})
	if err != nil {
		c.log.Errorw("can not connect ck server", "host", c.config.Host)
		return err
	}
	if err := conn.Ping(context.Background()); err != nil {
		c.log.Errorf("connect ck server fail, err: %v", err)
		return err
	} else {
		c.log.Info("connect ck server successful")
	}

	c.Conn = conn
	return nil
}

func (c *client) Close() error { return nil }

func (c *client) Publish(_ context.Context, batch publisher.Batch) error {
	st := c.observer
	events := batch.Events()

	batchData := c.getBatchRows(events)

	for _, v := range batchData {
		st.NewBatch(len(v.Rows))
		droped := 0
		if err := c.sendToTables(v); err != nil {
			c.log.Errorf("send table err: %v", err)
			droped += len(v.Rows)
		}
		st.Dropped(droped)
		st.Acked(len(v.Rows) - droped)
	}

	batch.ACK()
	return nil
}

func (c *client) String() string {
	return "clickhouse"
}

func (c *client) getTableConf() tableConf {

	tc := make(tableConf)
	for _, v := range c.config.Tables {
		tc[v.Table] = v.Columns
	}

	return tc
}

func (c *client) getBatchRows(events []publisher.Event) batchRows {
	conf := c.getTableConf()
	// fmt.Printf("========>  %+v", conf)

	batchs := make(batchRows)
	for _, ev := range events {
		fields, _ := ev.Content.GetValue("fields")
		fstr := fmt.Sprintf("%s", fields)
		tableT := ftField{}
		if err := json.Unmarshal([]byte(fstr), &tableT); err != nil {
			c.log.Errorf("parse field json fail, err: %v", err)
			continue
		}
		tableName := tableT.Table

		message, _ := ev.Content.GetValue("message")
		mstr := fmt.Sprintf("%s", message)
		messageT := make(map[string]interface{})
		if err := json.Unmarshal([]byte(mstr), &messageT); err != nil {
			c.log.Errorf("parse message json fail, err: %v", err)
			continue
		}
		// fmt.Printf("==message=> %v \n\n", messageT)
		row, err := filterRow(conf[tableName], messageT)
		if err != nil {
			c.log.Errorf("filter message column fail, err: %v", err)
			continue
		}

		// fmt.Printf("=row==> %v \n\n", row)

		lineRow := make([][]interface{}, 0)
		if _, ok := batchs[tableName]; !ok {
			lineRow = append(lineRow, row)
		} else {
			lineRow = batchs[tableName].Rows
			lineRow = append(lineRow, row)
		}
		batchs[tableName] = tableData{
			Table:   tableName,
			Columns: conf[tableName],
			Rows:    lineRow,
		}

	}
	return batchs
}

// 匹配指定的字段
func filterRow(column []string, row map[string]interface{}) (line []interface{}, err error) {
	for _, v := range column {
		if data, ok := row[v]; ok {
			line = append(line, data)
		} else {
			return nil, errors.New("filter column fail, column: " + v)
		}
	}
	return
}

func (c *client) sendToTables(v tableData) error {

	tableName := v.Table
	columnStr := strings.Join(v.Columns, ",")
	sql := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES ", c.config.Db, tableName, columnStr)

	num := 0
	for _, line := range v.Rows {
		valueStr := "("
		for _, column := range line {
			if reflect.TypeOf(column).String() == "[]interface {}" {
				valueStr += "[" + generateQuotaStr(column.([]interface{})) + "],"
			} else {
				valueStr += fmt.Sprintf("'%s',", column)
			}
		}
		sql += strings.TrimRight(valueStr, ",") + "),"
		num++
	}

	sql = strings.TrimRight(sql, ",")
	fmt.Printf("\n num: %d,  sql: %s \n", num, sql)

	return c.Conn.Exec(context.Background(), sql)
}

func generateQuotaStr(data []interface{}) string {
	var str string
	// fmt.Printf("%+v \n", data)
	for _, v := range data {
		if reflect.TypeOf(v).String() == "float64" {
			str += fmt.Sprintf("'%d',", v)
		} else {
			str += fmt.Sprintf("'%s',", v)
		}
	}
	str = strings.TrimRight(str, ",")
	// fmt.Printf("%+v \n", str)
	return str
}
