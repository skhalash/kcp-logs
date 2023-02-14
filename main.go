package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/klauspost/compress/zstd"
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

func main() {
	var file string
	flag.StringVar(&file, "file", "", "log file path")
	flag.Parse()

	out := tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', tabwriter.AlignRight)
	defer out.Flush()
	if err := run(file, out); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

func run(filePath string, out io.Writer) error {
	if filePath == "" {
		return errors.New("compressed path not provided")
	}

	compressed, err := os.Open(filePath)
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

					rawTag := stringAttributeByKey(logRecord.Attributes, "fluent.tag")
					tag, err := parseFluentTag(rawTag)
					if err != nil {
						continue
					}

					fmt.Fprintf(out, "%v/%v\t%v\t%v\n", tag.namespace, tag.pod, tag.container, message)
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

	// Copy content...
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

func parseFluentTag(tag string) (*fluentTag, error) {
	_, tagWithoutPrefix, found := strings.Cut(tag, "kube.var.log.containers.")
	if !found {
		return nil, errors.New("invalid fluent tag: prefix not found")
	}
	tokens := strings.Split(tagWithoutPrefix, "_")
	if len(tokens) != 3 {
		return nil, errors.New("invalid fluent tag: must be 3 tokens")
	}

	containerEndIndex := strings.LastIndex(tokens[2], "-")
	return &fluentTag{
		namespace: tokens[1],
		pod:       tokens[0],
		container: tokens[2][:containerEndIndex],
	}, nil
}
