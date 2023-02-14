package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
)

type Entry struct {
	ResourceLogs []ResourceLogs `json:"resourceLogs"`
}

type ResourceLogs struct {
	ScopeLogs []ScopeLogs `json:"scopeLogs"`
}

type ScopeLogs struct {
	LogRecords []LogRecords `json:"logRecords"`
}

type LogRecords struct {
	TimeUnixNano string           `json:"timeUnixNano"`
	Body         Body             `json:"body"`
	Attributes   []map[string]any `json:"attributes"`
}

type Body struct {
	KVListValue map[string]any `json:"kvlistValue"`
}

func main() {
	var file string
	flag.StringVar(&file, "file", "", "log file path")
	flag.Parse()

	if err := run(file); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

func run(filePath string) error {
	if filePath == "" {
		return errors.New("file path not provided")
	}

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for scanner.Scan() {
		//fmt.Println(lineNumber)
		lineNumber++
		var entry Entry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return fmt.Errorf("failed to unmarshal a log line: %v", err)
		}

		for _, resourceLog := range entry.ResourceLogs {
			for _, scopeLog := range resourceLog.ScopeLogs {
				for _, logRecord := range scopeLog.LogRecords {
					if logRecord.Body.KVListValue == nil {
						continue
					}

					attributes := flattenKeyValueList(logRecord.Body.KVListValue)
					log, err := parseAttributes(attributes)
					if err == nil {
						fmt.Printf("%s/%s %s %s\n", log.namespace, log.pod, log.container, log.log)
					}
				}
			}
		}
	}

	return nil
}

func flattenKeyValueList(node map[string]any) map[string]any {
	results := make(map[string]any)
	nodeValues := node["values"]
	for _, kvUntyped := range nodeValues.([]any) {
		kv := kvUntyped.(map[string]any)
		newKey := kv["key"].(string)
		var newValue any
		if strVal, hasStrVal := kv["value"].(map[string]any)["stringValue"]; hasStrVal {
			newValue = strVal
		} else if kvListVal, hasKVListVal := kv["value"].(map[string]any)["kvlistValue"]; hasKVListVal {
			newValue = flattenKeyValueList(kvListVal.(map[string]any))
		}
		results[newKey] = newValue
	}
	return results
}

type logRecord struct {
	log       string
	namespace string
	pod       string
	container string
}

func parseAttributes(attributes map[string]any) (logRecord, error) {
	log, hasLog := attributes["log"]
	if !hasLog {
		return logRecord{}, errors.New("no logs")
	}

	kubernetes, hasKubernetes := attributes["kubernetes"]
	if !hasKubernetes {
		return logRecord{}, errors.New("no kubernetes")
	}

	return logRecord{
		log:       log.(string),
		namespace: kubernetes.(map[string]any)["namespace_name"].(string),
		pod:       kubernetes.(map[string]any)["pod_name"].(string),
		container: kubernetes.(map[string]any)["container_name"].(string),
	}, nil
}
