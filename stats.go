package workers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

type stats struct {
	Processed int         `json:"processed"`
	Failed    int         `json:"failed"`
	Jobs      interface{} `json:"jobs"`
	Enqueued  interface{} `json:"enqueued"`
	Retries   int64       `json:"retries"`
}

func Stats(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	jobs := make(map[string][]*map[string]interface{})
	enqueued := make(map[string]string)

	for _, m := range managers {
		queue := m.queueName()
		jobs[queue] = make([]*map[string]interface{}, 0)
		enqueued[queue] = ""
		for _, worker := range m.workers {
			message := worker.currentMsg
			startedAt := worker.startedAt

			if message != nil && startedAt > 0 {
				jobs[queue] = append(jobs[queue], &map[string]interface{}{
					"message":    message,
					"started_at": startedAt,
				})
			}
		}
	}

	stats := stats{
		0,
		0,
		jobs,
		enqueued,
		0,
	}

	conn := Config.Pool.Get()
	defer conn.Close()

	conn.Send("multi")
	conn.Send("get", Config.Namespace+"stat:processed")
	conn.Send("get", Config.Namespace+"stat:failed")
	conn.Send("zcard", Config.Namespace+RETRY_KEY)

	for key, _ := range enqueued {
		conn.Send("llen", fmt.Sprintf("%squeue:%s", Config.Namespace, key))
	}

	r, err := conn.Do("exec")

	if err != nil {
		Logger.Println("couldn't retrieve stats:", err)
	}

	results := r.([]interface{})
	if len(results) == (3 + len(enqueued)) {
		for index, result := range results {
			if index == 0 && result != nil {
				stats.Processed, _ = strconv.Atoi(string(result.([]byte)))
				continue
			}
			if index == 1 && result != nil {
				stats.Failed, _ = strconv.Atoi(string(result.([]byte)))
				continue
			}

			if index == 2 && result != nil {
				stats.Retries = result.(int64)
				continue
			}

			queueIndex := 0
			for key, _ := range enqueued {
				if queueIndex == (index - 3) {
					enqueued[key] = fmt.Sprintf("%d", result.(int64))
				}
				queueIndex++
			}
		}
	}

	body, _ := json.MarshalIndent(stats, "", "  ")
	fmt.Fprintln(w, string(body))
}

type queueMessage struct {
	Queue      string `json:"queue"`
	Identifier string `json:"identifier"`
}

type identifierStatus struct {
	Status  bool                   `json:"status"`
	Error   string                 `json:"error"`
	Details map[string]interface{} `json:"details"`
}

// CheckQueueData checks whether identifier is part of args in messages in queue
func CheckQueueData(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	var payload queueMessage
	response := identifierStatus{Status: false}

	decoder := json.NewDecoder(req.Body)
	err := decoder.Decode(&payload)
	if err != nil {
		response.Error = err.Error()
		return
	}
	response.Status, response.Details = IdentifierInQueue(payload.Queue, payload.Identifier)
	body, _ := json.Marshal(response)
	fmt.Fprintln(w, string(body))
}

// IdentifierInQueue checks whether identifier is present in message of queue
func IdentifierInQueue(srcQueue, identifier string) (bool, map[string]interface{}) {
	for _, m := range managers {
		queue := m.queueName()
		if queue == srcQueue {
			for _, worker := range m.workers {
				message := worker.currentMsg
				startedAt := worker.startedAt
				if message != nil && startedAt > 0 {
					args, _ := message.Args().Array() // nolint: gosec
					for _, arg := range args {
						if arg == identifier {
							return true, map[string]interface{}{
								"message":    message,
								"started_at": startedAt,
							}
						}
					}
				}
			}
		}
	}
	return false, nil
}
