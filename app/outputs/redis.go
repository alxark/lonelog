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

const DEFAULTBATCHSIZE = 100
const DEFAULTKEY = "logs"

type RedisOutput struct {
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
			o.Batch = DEFAULTBATCHSIZE
		}
	} else {
		o.Batch = DEFAULTBATCHSIZE
	}

	if _, ok := options["servers"]; !ok {
		return nil, errors.New("no servers address")
	}

	if key, ok := options["key"]; ok {
		o.Key = key
	} else {
		o.Key = DEFAULTKEY
	}

	for _, v := range strings.Split(options["servers"], ",") {
		c := Connections{Addr: v}
		o.Outputs = append(o.Outputs, c)
		o.log.Printf("Adding new connection: %s", v)
	}

	return o, nil
}

func (o *RedisOutput) ReadFrom(input chan structs.Message) (err error) {
	o.log.Printf("Started redis output, batch: %d", o.Batch)

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
				cmdResult := client.LPush(o.Key, cache...)

				if cmdResult.Err() == nil {
					o.log.Printf("Inserted data to redis. Size: %d. Try: %d", o.Batch, retry)
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
