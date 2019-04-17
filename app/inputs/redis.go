package inputs

import (
	"github.com/go-redis/redis"
	"log"
	"github.com/alxark/lonelog/structs"
	//"encoding/json"
	"errors"
	"strings"
	"time"
	"encoding/json"
	"strconv"
	"bytes"
	"compress/gzip"
	"io"
)

const DEFAULTKEY = "logs"

const (
	fetchModeRangeTrim = 1
	fetchModePop       = 2

	defaultBatchSize = 10000
)

type RedisInput struct {
	BasicInput

	log log.Logger

	Inputs    []Connections
	Key       string
	Batch     int
	FetchMode int
	Trim      bool
	Compression bool
}

type Connections struct {
	Addr string
}

func NewRedisInput(options map[string]string, logger log.Logger) (o *RedisInput, err error) {
	logger.Printf("Initializing redis input")

	o = &RedisInput{}
	o.log = logger

	if _, ok := options["servers"]; !ok {
		return nil, errors.New("no servers address")
	}

	if key, ok := options["key"]; ok {
		o.Key = key
	} else {
		o.Key = DEFAULTKEY
	}

	if fetchMode, ok := options["mode"]; ok {
		if fetchMode == "range" {
			o.FetchMode = fetchModeRangeTrim
			o.log.Print("Attention! Fetch mode RANGE-TRIM activated. Make sure there is no additional threads for this key")
		} else {
			o.FetchMode = fetchModePop
		}
	} else {
		o.FetchMode = fetchModePop
	}

	if trimOrig, ok := options["trim"]; ok {
		o.Trim = trimOrig == "0" || trimOrig == "no" || trimOrig == "false"
	} else {
		o.Trim = true
	}

	if batchOrig, ok := options["batch"]; ok {
		batch, err := strconv.Atoi(batchOrig)
		if err != nil {
			o.Batch = defaultBatchSize
		}
		o.Batch = batch
	} else {
		o.Batch = defaultBatchSize
	}

	if compression, ok := options["compression"]; ok {
		o.Compression = o.IsActive(compression)
	} else {
		o.Compression = false
	}

	for _, v := range strings.Split(options["servers"], ",") {
		c := Connections{Addr: v}
		o.Inputs = append(o.Inputs, c)
		o.log.Printf("Adding new connection: %s", v)
	}

	return o, nil
}

func (o *RedisInput) GetRedisConnection() (client *redis.Client, err error) {
	for {
		client := redis.NewClient(&redis.Options{
			Addr:     o.Inputs[0].Addr,
			Password: "",
			DB:       0,
		})

		_, err = client.Ping().Result()
		if err != nil {
			o.log.Printf("Failed to ping server. Got: " + err.Error())
			time.Sleep(1 * time.Second)
			continue
		}

		o.log.Printf("Redis connection established to %s", o.Inputs[0].Addr)
		return client, err
	}
}

func (o *RedisInput) AcceptTo(output chan structs.Message, counter chan int) (err error) {
	o.log.Printf("Started redis input. Input key: %s", o.Key)

	if o.FetchMode == fetchModeRangeTrim {
		return o.ProcessRangeTrim(output, counter)
	} else {
		return o.ProcessPop(output, counter)
	}
}

func (o *RedisInput) ProcessRangeTrim(output chan structs.Message, counter chan int) (err error) {
	o.log.Print("Started redis reader. Fetch mode LRANGE-LTRIM. Batch: %d, trim: %b", o.Batch, o.Trim)
	client, err := o.GetRedisConnection()

	for {
		res := client.LRange(o.Key, 0, int64(o.Batch - 1))
		if err != nil {
			o.log.Print("failed to connect. got: " + err.Error() + ", going to reconnect")
			client, err = o.GetRedisConnection()
			if err != nil {
				time.Sleep(10 * time.Second)
			}

			continue
		}

		items, err := res.Result()
		if err != nil {
			o.log.Print("Failed to fetch result. Got: " + err.Error())
			continue
		}

		if o.Trim {
			cmdRes := client.LTrim(o.Key, int64(len(items)), -1)
			if cmdRes.Err() != nil {
				o.log.Print("failed to run LTRIM command. Got: " + cmdRes.Err().Error())
				continue
			}
		}

		if len(items) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		chunkProcessed := 0
		for _, item := range items {
			if o.Compression {
				gz, err := gzip.NewReader(bytes.NewBuffer([]byte(item)))
				if err != nil {
					o.log.Print("failed to parse gzipped data: "  + err.Error())
					continue
				}

				var buf bytes.Buffer
				io.Copy(&buf, gz)

				var compressedMessages []string
				err = json.Unmarshal(buf.Bytes(), &compressedMessages)
				if err != nil {
					o.log.Print("failed to decode compressed messages: " + err.Error())
				}

				for _, encodedMsg := range compressedMessages {
					var msg structs.Message
					err = json.Unmarshal([]byte(encodedMsg), &msg)
					if err != nil {
						o.log.Printf("failed to decode compressed message: %s", err.Error())
						continue
					}

					chunkProcessed += 1
					output <- msg
				}
			} else {
				msg := structs.Message{}
				err = json.Unmarshal([]byte(item), &msg)

				if err != nil {
					o.log.Print("failed to decode message: " + err.Error())
					continue
				}

				chunkProcessed += 1

				output <- msg
			}
		}

		counter <- chunkProcessed
	}
}

func (o *RedisInput) ProcessPop(output chan structs.Message, counter chan int) (err error) {
	o.log.Print("Started redis reader. Fetch mode BLPOP")

	/**
	for {
		i += 1


		sliceCmd := client.LPop(o.Key)
		res, err := sliceCmd.Result()
		if err != nil {
			client = redis.NewClient(&redis.Options{
				Addr:     o.Inputs[0].Addr,
				Password: "",
				DB:       0,
			})
			time.Sleep(1 * time.Second)
			o.log.Printf("re-established connection to %s", o.Inputs[0].Addr)

			continue
		}
		msg := structs.Message{}
		err = json.Unmarshal([]byte(res), &msg)

		if err != nil {
			o.log.Print(err.Error())
			continue
		}

		o.log.Printf("new msg: %d", i)
		output <- msg
	}

	return*/

	return err
}
