package outputs

import (
	"github.com/go-redis/redis"
	"log"
	"github.com/alxark/lonelog/internal/structs"
	"encoding/json"
	"strconv"
	"errors"
	"strings"
	"time"
	"bytes"
	"compress/gzip"
)

const (
	redisDefaultBatchSize = 1000
	redisDefaultKey       = "logs"
)

type RedisOutput struct {
	BasicOutput

	log log.Logger

	OutputMode    string // available modes - sharding or replica
	Outputs       []Connections
	Key           string
	Batch         int
	CompressBatch int
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

	if compressBatch, ok := options["compress_batch"]; ok {
		o.CompressBatch, err = strconv.Atoi(compressBatch)
		if err != nil {
			return nil, errors.New("incorrect compress_batch value: " + compressBatch)
		}

		o.log.Printf("Initialized compress batch: %s", compressBatch)
	} else {
		o.CompressBatch = 0
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

	o.log.Printf("Started redis output, batch: %d, output to %s, compress batch: %d", o.Batch, keyName, o.CompressBatch)

	cache := make([]interface{}, o.Batch)
	var compressCache []string

	if o.CompressBatch > 0 {
		compressCache = make([]string, o.CompressBatch)
	}

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
	compressPos := 0

messageCycle:
	for msg := range input {
		info, err := json.Marshal(msg)
		if err != nil {
			o.log.Print("failed to encode to JSON: " + err.Error())
			continue
		}

		if o.CompressBatch > 0 {
			compressCache[compressPos] = string(info)
			compressPos += 1


			if compressPos == o.CompressBatch {
				compressPos = 0

				encodedData, err := json.Marshal(compressCache)
				if err != nil {
					o.log.Printf("failed to encode compress data: " + err.Error())

					compressCache = make([]string, o.CompressBatch)
					continue
				}

				var buf bytes.Buffer
				gz, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)

				_, err = gz.Write(encodedData)
				if err != nil {
					o.log.Printf("failed to compress: " + err.Error())
					compressCache = make([]string, o.CompressBatch)
				}
				gz.Flush()
				gz.Close()

				if o.Debug {
					compression := 100.0 * float64(buf.Len()) / float64(len(encodedData))
					o.log.Printf("data compressed %d => %d, compression %0.2f%%", len(encodedData), buf.Len(), compression)
				}

				cache[cachePos] = buf.Bytes()
				cachePos += 1
			}
		} else {
			cache[cachePos] = info
			cachePos += 1
		}

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
