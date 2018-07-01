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

func (c *ClickhouseOutput) ReadFrom(input chan structs.Message,  runtimeOptions map[string]string, counter chan int) (err error) {

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
	runningErrors := 0

	var tx *sql.Tx
	var stmt *sql.Stmt

	lastFill := time.Now().Unix()
	currentTime := time.Now().Unix()

	var arguments []interface{}

	for msg := range input {
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


		arguments, err = c.PrepareArguments(msg.Payload)
		if err != nil {
			c.log.Printf("failed to proceed row. error: %s", err.Error())
			continue
		}

		if _, err := stmt.Exec(arguments...); err != nil {
			c.log.Printf("failed to proceed row: %s, got: %s", arguments, err.Error())
			runningErrors += 1
			continue
		}

		i += 1
		runningErrors = 0

		if i % 100 == 0 {
			currentTime = time.Now().Unix()
		}

		if i >= c.Batch || currentTime - c.Threshold > lastFill {
			currentTime = time.Now().Unix()
			diff := currentTime - lastFill
			c.log.Printf("Batch filled in %d seconds. Inserting %d items to database", diff, i)
			lastFill = currentTime

			err = tx.Commit()
			if err != nil {
				c.log.Printf("failed to complete transaction: %s", err.Error())

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

				continue
			}

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