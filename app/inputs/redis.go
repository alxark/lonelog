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
	"math"
	"strconv"
)

const DEFAULTKEY = "logs"

type RedisInput struct {
	BasicInput

	log log.Logger

	Inputs    []Connections
	Key       string
	Benchmark int
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

	if benchmark, ok := options["benchmark"]; ok {
		o.Benchmark, err = strconv.Atoi(benchmark)
		if err != nil {
			o.Benchmark = 100000
		}
	} else {
		o.Benchmark = 100000
	}

	for _, v := range strings.Split(options["servers"], ",") {
		c := Connections{Addr: v}
		o.Inputs = append(o.Inputs, c)
		o.log.Printf("Adding new connection: %s", v)
	}

	return o, nil
}

func (o *RedisInput) AcceptTo(output chan structs.Message) (err error) {
	o.log.Printf("Started redis output, benchmark %d", o.Benchmark)

	client := redis.NewClient(&redis.Options{
		Addr:     o.Inputs[0].Addr,
		Password: "",
		DB:       0,
	})

	_, err = client.Ping().Result()
	if err != nil {
		o.log.Printf("Failed to ping server. Got: " + err.Error())
	}

	i := 0
	benchmarkStart := time.Now().Unix()

	for {
		sliceCmd := client.BLPop(10*time.Second, o.Key)
		res, err := sliceCmd.Result()
		if err != nil {
			o.log.Print(err.Error())
			continue
		}
		msg := structs.Message{}
		err = json.Unmarshal([]byte(res[1]), &msg)

		if err != nil {
			o.log.Print(err.Error())
			continue
		}

		output <- msg

		if i >= o.Benchmark {
			benchmarkNow := time.Now().Unix()

			rps := math.Floor(float64(o.Benchmark) / float64(benchmarkNow-benchmarkStart))
			o.log.Printf("Processed %d, processing speed: %f RPS", o.Benchmark, rps)

			benchmarkStart = benchmarkNow
			i = 0
		}
	}

	return
}
