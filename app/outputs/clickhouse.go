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

type ClickhouseOutput struct {
	log log.Logger

	Fields      []string
	FieldsTypes []string

	Dsn   string
	Batch int
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

	if err = pingDsn(c.Dsn); err != nil {
		return nil, errors.New("invalid clickhouse dsn: " + err.Error())
	}

	if batch, ok := options["batch"]; ok {
		c.Batch, err = strconv.Atoi(batch)
		if err != nil {
			return nil, errors.New("incorrect clickhouse batch size: " + batch)
		}
	} else {
		c.Batch = 100
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

func (c *ClickhouseOutput) ReadFrom(input chan structs.Message) (err error) {

	connect, err := sql.Open("clickhouse", c.Dsn)
	if err != nil {
		c.log.Fatal(err.Error())
	}
	err = connect.Ping()
	if err != nil {
		c.log.Fatal(err.Error())
	}

	var placeholders []string
	for range c.Fields {
		placeholders = append(placeholders, "?")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		c.Table, strings.Join(c.Fields, ","), strings.Join(placeholders, ","))

	c.log.Print("Using query: " + query)

	i := 0

	var tx *sql.Tx
	var stmt *sql.Stmt

	for msg := range input {
		// c.log.Print(msg)

		if i == 0 {
			tx, err = connect.Begin()
			if err != nil {
				c.log.Print("failed to initialize transaction: " + err.Error())
				time.Sleep(5 * time.Second)
				continue
			}

			stmt, err = tx.Prepare(query)
			if err != nil {
				c.log.Fatal(err.Error())
			}
		}

		var arguments []interface{}
		for i, fieldName := range c.Fields {
			var value string
			if _, ok := msg.Payload[fieldName]; !ok {
				value = ""
			}

			value = msg.Payload[fieldName]

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

		if _, err := stmt.Exec(arguments...); err != nil {
			c.log.Fatal(err)
		}

		i += 1
		if i == c.Batch {
			c.log.Printf("Batch is full. Inserting %d items to database", c.Batch)
			err = tx.Commit()
			if err != nil {
				c.log.Fatal(err.Error())
			}

			i = 0
		}

		//c.log.Print(msg)
	}

	c.log.Print("ClickHouse: channel finished. Exiting...")
	return
}
