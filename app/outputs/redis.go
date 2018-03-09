package outputs

import (
	"github.com/go-redis/redis"
	"log"
	"github.com/alxark/lonelog/structs"
	"encoding/json"
	"strconv"
	"errors"
	"strings"
	"time"
)

const (
	redisDefaultBatchSize = 1000
	redisDefaultKey = "logs"
)

type RedisOutput struct {
	BasicOutput

	log log.Logger

	OutputMode string // available modes - sharding or replica
	Outputs    []Connections
	Key        string
	Batch      int
}

type Connections struct {
	Addr string
}

func NewRedisOutput(options map[string]string, logger log.Logger) (o *RedisOutput, err error) {
	logger.Printf("Initializing stat output")

	o = &RedisOutput{}
	o.log = logger

	if batchSize, ok := options["batch"]; ok {
		o.Batch, err = strconv.Atoi(batchSize)
		if err != nil {
			o.Batch = redisDefaultBatchSize
		}
	} else {
		o.Batch = redisDefaultBatchSize
	}

	if _, ok := options["servers"]; !ok {
		return nil, errors.New("no servers address")
	}

	if key, ok := options["key"]; ok {
		o.Key = key
	} else {
		o.Key = redisDefaultKey
	}

	for _, v := range strings.Split(options["servers"], ",") {
		c := Connections{Addr: v}
		o.Outputs = append(o.Outputs, c)
		o.log.Printf("Adding new connection: %s", v)
	}

	return o, nil
}

func (o *RedisOutput) ReadFrom(input chan structs.Message, runtimeOptions map[string]string, counter chan int) (err error) {
	keyName := o.PrepareStringVariable(o.Key, runtimeOptions)

	o.log.Printf("Started redis output, batch: %d, output to %s", o.Batch, keyName)

	cache := make([]interface{}, o.Batch)

	client := redis.NewClient(&redis.Options{
		Addr:     o.Outputs[0].Addr,
		Password: "",
		DB:       0,
	})

	_, err = client.Ping().Result()
	if err != nil {
		o.log.Printf("Failed to ping server. Got: " + err.Error())
	}

	cachePos := 0

messageCycle:
	for msg := range input {
		info, err := json.Marshal(msg)
		if err != nil {
			o.log.Print("Failed to encode to JSON")
			continue
		}

		cache[cachePos] = info
		cachePos += 1
		if cachePos == o.Batch {
			cachePos = 0

			for retry := 0; retry < 32; retry += 1 {
				cmdResult := client.RPush(keyName, cache...)

				if cmdResult.Err() == nil {
					o.log.Printf("Inserted data to redis. Size: %d. Try: %d", o.Batch, retry)

					counter <- o.Batch

					continue messageCycle
				}

				o.log.Printf("failed to push resolve. got: %s", cmdResult.Err())
				client = redis.NewClient(&redis.Options{
					Addr:     o.Outputs[0].Addr,
					Password: "",
					DB:       0,
				})

				_, err = client.Ping().Result()
				if err != nil {
					o.log.Printf("failed to ping server => " + err.Error())
				}

				time.Sleep(time.Duration(2*retry) * time.Second)
			}

			o.log.Print("failed to insert data batch")
		}
	}

	return
}
