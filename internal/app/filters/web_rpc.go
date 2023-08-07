package filters

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/alxark/lonelog/internal/structs"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const OnFailRetry = 1
const OnFailSkip = 2

type RpcReply struct {
	Status     bool
	ReadTime   time.Time
	CreateTime time.Time
	Fields     map[string]string
}

type WebRpcFilter struct {
	BasicFilter

	Url    string
	Fields []string
	OnFail int
	Size   int
	Cache  map[string]RpcReply
	Mutex  sync.RWMutex

	log log.Logger
}

func NewWebRpcFilter(options map[string]string, logger log.Logger) (f *WebRpcFilter, err error) {
	f = &WebRpcFilter{}

	if _, ok := options["url"]; !ok {
		return f, errors.New("no RPC url provided")
	}

	f.Url = options["url"]
	if _, ok := options["fields"]; !ok {
		return f, errors.New("no fields specified")
	}

	if _, ok := options["size"]; ok {
		f.Size, err = strconv.Atoi(options["size"])
		if err != nil {
			f.Size = 2048
		}
	} else {
		f.Size = 2048
	}

	if _, ok := options["on_fail"]; ok {
		if options["on_fail"] == "retry" {
			f.OnFail = OnFailRetry
		} else if options["on_fail"] == "skip" {
			f.OnFail = OnFailSkip
		} else {
			return nil, errors.New("unknown on_fail mode: " + options["on_fail"] + ", should be skip or retry")
		}
	} else {
		f.OnFail = OnFailRetry
	}

	f.Fields = strings.Split(options["fields"], ",")
	f.Mutex = sync.RWMutex{}

	f.log = logger

	return f, nil
}

// Proceed - split content field by delimiter
func (f *WebRpcFilter) Proceed(ctx context.Context, input chan structs.Message, output chan structs.Message) (err error) {
	f.Cache = make(map[string]RpcReply)

	i := 0
	for ctx.Err() == nil {
		msg, _ := f.ReadMessage(input)

		if i == f.ServiceInterval {
			f.log.Printf("doing service works. Total cache size: %d", len(f.Cache))
			i = 0

			f.Mutex.Lock()
			var removeList []string
			for keyName, value := range f.Cache {
				if value.CreateTime.Unix() < time.Now().Unix()-3600 {
					removeList = append(removeList, keyName)
				}
			}

			f.log.Printf("Total items for removal: %d", len(removeList))
			for _, keyName := range removeList {
				delete(f.Cache, keyName)
			}
			f.log.Printf("Total items after cleanup: %d", len(f.Cache))
			f.Mutex.Unlock()
		}

		i += 1

		msgHash, err := f.HashKey(msg.Payload)

		if err != nil {
			_ = f.WriteMessage(output, msg)
			f.log.Printf("[ERROR] Failed to generate hash: %s", err.Error())
			continue
		}

		var updateData map[string]string

		f.Mutex.RLock()
		if cachedData, ok := f.Cache[msgHash]; ok {
			updateData = cachedData.Fields
			f.Mutex.RUnlock()
		} else {
			f.Mutex.RUnlock()

			result, err := f.Call(msg.Payload)

			if err != nil {
				if f.OnFail == OnFailSkip {
					f.log.Printf("failed for load information from RPC: %s", err.Error())
					_ = f.WriteMessage(output, msg)
					continue
					// oh shit, we need to retry and wait for result
				} else if f.OnFail == OnFailRetry {
					f.log.Printf(f.GetName()+": failed for load information from RPC: %s, starting retry process", err.Error())

					i := 0
					for {
						i++
						result, err = f.Call(msg.Payload)
						if err != nil {
							time.Sleep(time.Duration(i) * time.Second)
							f.log.Printf(f.GetName()+": request error, retry %d, got: %s", i, err.Error())
							continue
						}

						f.log.Printf("got RPC result after %d retries", i)
						break
					}
				}
			}

			result.Status = true
			result.CreateTime = time.Now()

			f.Mutex.Lock()
			f.Cache[msgHash] = result
			f.log.Printf("Added new cached item: %d", len(f.Cache))
			f.Mutex.Unlock()

			updateData = result.Fields
		}

		for key, value := range updateData {
			payload := msg.Payload
			payload[key] = value
			msg.Payload = payload
		}

		_ = f.WriteMessage(output, msg)
	}

	return
}

func (f *WebRpcFilter) HashKey(data map[string]string) (result string, err error) {
	hashedData := make(map[string]string)
	for _, fieldName := range f.Fields {
		if _, ok := data[fieldName]; ok {
			hashedData[fieldName] = data[fieldName]
		} else {
			hashedData[fieldName] = "-"
		}
	}

	jsonData, err := json.Marshal(hashedData)
	if err != nil {
		return
	}

	hashObject := sha256.New()
	hashObject.Write(jsonData)

	return base64.URLEncoding.EncodeToString(hashObject.Sum(nil)), nil
}

// call to remote RPC
func (f *WebRpcFilter) Call(payload map[string]string) (reply RpcReply, err error) {
	data := url.Values{}

	for _, fieldName := range f.Fields {
		if val, ok := payload[fieldName]; ok {
			data.Set(fieldName, val)
		} else {
			data.Set(fieldName, "-")
		}
	}

	client := &http.Client{}
	r, _ := http.NewRequest("POST", f.Url, strings.NewReader(data.Encode())) // URL-encoded payload
	r.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))

	res, err := client.Do(r)
	if err != nil {
		return
	}

	content, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}

	var serviceReply map[string]string
	err = json.Unmarshal(content, &serviceReply)
	res.Body.Close()

	reply.Fields = serviceReply

	return
}
