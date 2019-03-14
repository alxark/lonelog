package outputs

import (
	"database/sql"
	_ "github.com/kshvakov/clickhouse"
	"log"
	"github.com/alxark/lonelog/structs"
	"time"
	"strings"
	"errors"
	"strconv"
	"fmt"
)

const ClickhouseTimeThreshold = 60

type ClickhouseOutput struct {
	log log.Logger

	Fields      []string
	FieldsTypes []string

	Dsn   string
	Batch int
	Threshold int64
	Table string
}

func NewClickhouseOutput(options map[string]string, logger log.Logger) (c *ClickhouseOutput, err error) {
	c = &ClickhouseOutput{}
	c.log = logger
	if fieldsList, ok := options["fields"]; ok {
		for _, fieldName := range strings.Split(fieldsList, ",") {
			info := strings.Split(fieldName, ":")
			c.Fields = append(c.Fields, info[0])

			if len(info) > 1 {
				c.FieldsTypes = append(c.FieldsTypes, info[1])
			} else {
				c.FieldsTypes = append(c.FieldsTypes, "string")
			}
		}
	} else {
		return nil, errors.New("no fields available")
	}

	if dsn, ok := options["dsn"]; ok {
		c.Dsn = dsn
	} else {
		return nil, errors.New("no dsn available")
	}

	if table, ok := options["table"]; ok {
		c.Table = table
	} else {
		return nil, errors.New("no table specified")
	}

	if batch, ok := options["batch"]; ok {
		c.Batch, err = strconv.Atoi(batch)
		if err != nil {
			return nil, errors.New("incorrect clickhouse batch size: " + batch)
		}
	} else {
		c.Batch = 100
	}

	if threshold, ok := options["threshold"]; ok {
		threshold, err := strconv.Atoi(threshold)
		if err != nil {
			c.Threshold = int64(ClickhouseTimeThreshold)
		}
		c.Threshold = int64(threshold)
	} else {
		c.Threshold = int64(ClickhouseTimeThreshold)
	}

	c.log.Printf("Initialized ClickHouse to %s, batch: %d", c.Dsn, c.Batch)

	return c, nil
}

func pingDsn(dsn string) (err error) {
	connect, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return
	}

	err = connect.Ping()
	if err != nil {
		return
	}

	return
}

func (c *ClickhouseOutput) delay(try int) {
	time.Sleep(5 * time.Second)
}

func (c *ClickhouseOutput) flushBuffer(buffer []structs.Message) error {
	try := 0

	for {
		try += 1
		var tx *sql.Tx
		var stmt *sql.Stmt

		connect, err := sql.Open("clickhouse", c.Dsn)
		if err != nil {
			c.log.Printf("try %d, failed to connect with server: %s", try, err.Error())
			c.delay(try)
			continue
		}

		err = connect.Ping()
		if err != nil {
			c.log.Printf("try %d, server is not responding, got: %s", try, err.Error())
			c.delay(try)
			continue
		}

		var placeholders []string
		for range c.Fields {
			placeholders = append(placeholders, "?")
		}

		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			c.Table, strings.Join(c.Fields, ","), strings.Join(placeholders, ","))

		c.log.Print("Using query: " + query)

		var arguments []interface{}

		tx, err = connect.Begin()
		if err != nil {
			c.log.Printf("try %d, failed to initialize transaction: %s", try, err.Error())
			c.delay(try)
			continue
		}

		stmt, err = tx.Prepare(query)
		if err != nil {
			c.log.Printf("try %d, failed to prepare statement: %s", try, err.Error())
			c.delay(try)
			continue
		}

		for _, msg := range buffer {
			arguments, err = c.PrepareArguments(msg.Payload)
			if err != nil {
				c.log.Printf("failed to proceed row. error: %s", err.Error())
				continue
			}

			if _, err := stmt.Exec(arguments...); err != nil {
				c.log.Printf("failed to proceed row: %s, got: %s", arguments, err.Error())
				continue
			}
		}

		err = tx.Commit()
		if err == nil {
			c.log.Printf("inserted %d rows, try: %d", len(buffer), try)
			return nil
		}

		c.log.Printf("failed to commit clickhouse transaction, try: %d, got: %s", try, err.Error())
		c.delay(try)
	}
}

/**
 * Read data to local buffer and flush it when it's filled or when
 * time threshold is reached
 */
func (c *ClickhouseOutput) ReadFrom(input chan structs.Message,  runtimeOptions map[string]string, counter chan int) (err error) {
	i := 0

	lastFill := time.Now().Unix()
	currentTime := time.Now().Unix()

	buffer := make([]structs.Message, c.Batch)

	for msg := range input {
		buffer[i] = msg
		i += 1

		// time function is too heavy, we don't want to run it on each message
		if i % 128 == 0 {
			currentTime = time.Now().Unix()
		}

		if i >= c.Batch || currentTime - c.Threshold > lastFill {
			currentTime = time.Now().Unix()
			diff := currentTime - lastFill

			c.log.Printf("Batch filled in %d seconds. Inserting %d items to database", diff, i)
			lastFill = currentTime
			c.flushBuffer(buffer[:i])

			counter <- i

			i = 0
		}
	}

	c.log.Print("ClickHouse: channel finished. Exiting...")
	return
}

/**
 * Format arguments for clickhouse driver
 */
func (c *ClickhouseOutput) PrepareArguments(payload map[string]string) (arguments []interface{}, err error) {
	for i, fieldName := range c.Fields {
		var value string
		if _, ok := payload[fieldName]; !ok {
			value = ""
		}

		value = payload[fieldName]

		switch c.FieldsTypes[i] {
		case "int":
			convert, err := strconv.Atoi(value)
			if err != nil {
				convert = 0
			}
			arguments = append(arguments, convert)
			break
		case "float":
			convert, err := strconv.ParseFloat(value, 32)
			if err != nil {
				convert = 0.0
			}
			arguments = append(arguments, convert)
		case "datetime":
			convert, err := time.Parse("2006-01-02 15:04:05", value)
			if err != nil {
				convert = time.Now()
			}
			arguments = append(arguments, convert)
		case "string":
			arguments = append(arguments, value)
			break
		default:
			arguments = append(arguments, value)
		}
	}

	return arguments, err
}