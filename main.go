package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"io"
	"os"
	"strings"
	"time"
)

type LogData struct {
	ResourceLogs []ResourceLogs `json:"resourceLogs"`
}

type ResourceLogs struct {
	ScopeLogs []ScopeLogs `json:"scopeLogs"`
}

type ScopeLogs struct {
	LogRecords []LogRecords `json:"logRecords"`
}

type LogRecords struct {
	TimeUnixNano string         `json:"timeUnixNano"`
	Body         map[string]any `json:"body"`
	Attributes   []Attribute    `json:"attributes"`
}

type Attribute struct {
	Key   string
	Value map[string]any
}

type flags struct {
	matchBy
	file  string
	since time.Duration
}

type matchBy struct {
	namespace string
	pod       string
	container string
}

func main() {
	var fl flags
	flag.StringVar(&fl.file, "file", "", "log file path")
	flag.StringVar(&fl.namespace, "namespace", "", "namespace prefix to filter for")
	flag.StringVar(&fl.pod, "pod", "", "pod prefix to filter for")
	flag.StringVar(&fl.container, "container", "", "container prefix to filter for")
	flag.DurationVar(&fl.since, "since", time.Duration(0), "only return logs newer than a relative duration like 5s, 2m, or 3h")

	flag.Parse()
	if err := validate(fl); err != nil {
		fmt.Printf("Invalid flag: %v\n", err)
		os.Exit(1)
	}

	if err := run(fl, os.Stdout); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

func validate(fl flags) error {
	if fl.file == "" {
		return errors.New("compressed path not provided")
	}
	return nil
}

func run(fl flags, out io.Writer) error {
	compressed, err := os.Open(fl.file)
	if err != nil {
		return err
	}
	defer compressed.Close()

	for {
		logDataJSON, err := decompressChunk(compressed)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		var logData LogData
		if err := json.Unmarshal(logDataJSON, &logData); err != nil {
			return fmt.Errorf("failed to unmarshal a log line: %v", err)
		}

		for _, resourceLog := range logData.ResourceLogs {
			for _, scopeLog := range resourceLog.ScopeLogs {
				for _, logRecord := range scopeLog.LogRecords {
					message := logMessage(logRecord.Body)

					rawTimestamp := stringAttributeByKey(logRecord.Attributes, "time")
					timestamp, err := time.Parse(time.RFC3339, rawTimestamp)
					if err != nil {
						continue
					}

					if fl.since != time.Duration(0) {
						fromTimestamp := time.Now().UTC().Add(-1 * fl.since)
						if timestamp.Before(fromTimestamp) {
							continue
						}
					}

					rawTag := stringAttributeByKey(logRecord.Attributes, "fluent.tag")
					tag, err := parseFluentTag(rawTag)
					if err != nil {
						continue
					}

					if matches(tag, fl.matchBy) {
						fmt.Fprintf(out, "%v/%v\t%v\t%v\t%v\n", tag.namespace, tag.pod, tag.container, timestamp, message)
					}
				}
			}
		}
	}

	return nil
}

func decompressChunk(in io.Reader) ([]byte, error) {
	sizeBuf := make([]byte, 4)
	if err := binary.Read(in, binary.BigEndian, &sizeBuf); err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(sizeBuf)
	dataBuf := make([]byte, size)
	if err := binary.Read(in, binary.BigEndian, &dataBuf); err != nil {
		return nil, err
	}

	compressedChunk := bytes.NewBuffer(dataBuf)
	var decompressedChunk bytes.Buffer

	d, err := zstd.NewReader(compressedChunk)
	if err != nil {
		return nil, err
	}
	defer d.Close()

	if _, err := io.Copy(&decompressedChunk, d); err != nil {
		return nil, err
	}
	return decompressedChunk.Bytes(), nil
}

func logMessage(body map[string]any) string {
	if val, hasStringVal := body["stringValue"]; hasStringVal {
		if stringVal, ok := val.(string); ok {
			return stringVal
		}
	}
	return ""
}

func stringAttributeByKey(attributes []Attribute, key string) string {
	for _, attr := range attributes {
		if attr.Key != key {
			continue
		}
		if val, hasStringVal := attr.Value["stringValue"]; hasStringVal {
			if stringVal, ok := val.(string); ok {
				return stringVal
			}
		}
	}
	return ""
}

type fluentTag struct {
	namespace, pod, container string
}

func parseFluentTag(tag string) (fluentTag, error) {
	_, tagWithoutPrefix, found := strings.Cut(tag, "kube.var.log.containers.")
	if !found {
		return fluentTag{}, errors.New("invalid fluent tag: prefix not found")
	}
	tokens := strings.Split(tagWithoutPrefix, "_")
	if len(tokens) != 3 {
		return fluentTag{}, errors.New("invalid fluent tag: must be 3 tokens")
	}

	containerEndIndex := strings.LastIndex(tokens[2], "-")
	return fluentTag{
		namespace: tokens[1],
		pod:       tokens[0],
		container: tokens[2][:containerEndIndex],
	}, nil
}

func matches(tag fluentTag, by matchBy) bool {
	if by.namespace != "" && !strings.HasPrefix(tag.namespace, by.namespace) {
		return false
	}
	if by.pod != "" && !strings.HasPrefix(tag.pod, by.pod) {
		return false
	}
	if by.container != "" && !strings.HasPrefix(tag.container, by.container) {
		return false
	}
	return true
}
