// +build !consumer

package main

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	sarama "github.com/Shopify/sarama"
	saramatls "github.com/Shopify/sarama/tools/tls"
	"github.com/minio/pkg/env"
)

type kafkaTarget struct {
	producer sarama.SyncProducer
	config   *sarama.Config
	topic    string
}

func kafkaEnvsToConfig() (*kafkaTarget, error) {
	if env.Get("DEBUG", "off") == "on" {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	if ok := env.Get("MINIO_AUDIT_KAFKA_ENABLE", "on"); ok != "on" {
		return nil, errors.New("kafka not enabled")
	}

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_0_0
	if v := env.Get("MINIO_AUDIT_KAFKA_VERSION", ""); v != "" {
		kafkaVersion, err := sarama.ParseKafkaVersion(v)
		if err != nil {
			return nil, err
		}
		config.Version = kafkaVersion
	}

	tlsConfig, err := saramatls.NewConfig(env.Get("MINIO_AUDIT_KAFKA_CLIENT_TLS_CERT", ""),
		env.Get("MINIO_AUDIT_KAFKA_CLIENT_TLS_KEY", ""))
	if err != nil {
		return nil, err
	}

	if cauth := env.Get("MINIO_AUDIT_KAFKA_TLS_CLIENT_AUTH", ""); cauth != "" {
		clientAuth, err := strconv.Atoi(cauth)
		if err != nil {
			return nil, err
		}
		config.Net.TLS.Config.ClientAuth = tls.ClientAuthType(clientAuth)
	}

	config.Net.TLS.Enable = env.Get("MINIO_AUDIT_KAFKA_TLS", "off") == "on"
	config.Net.TLS.Config = tlsConfig
	config.Net.TLS.Config.InsecureSkipVerify = env.Get("MINIO_AUDIT_KAFKA_TLS_SKIP_VERIFY", "off") == "on"

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	brokers := strings.Split(env.Get("MINIO_AUDIT_KAFKA_BROKERS", ""), ",")
	topic := env.Get("MINIO_AUDIT_KAFKA_TOPIC", "")

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &kafkaTarget{
		producer: producer,
		config:   config,
		topic:    topic,
	}, nil
}

// Entry - audit entry logs.
type Entry struct {
	Version      string `json:"version"`
	DeploymentID string `json:"deploymentid,omitempty"`
	Time         string `json:"time"`
	Trigger      string `json:"trigger"`
	API          struct {
		Name            string `json:"name,omitempty"`
		Bucket          string `json:"bucket,omitempty"`
		Object          string `json:"object,omitempty"`
		Status          string `json:"status,omitempty"`
		StatusCode      int    `json:"statusCode,omitempty"`
		TimeToFirstByte string `json:"timeToFirstByte,omitempty"`
		TimeToResponse  string `json:"timeToResponse,omitempty"`
	} `json:"api"`
	RemoteHost string                 `json:"remotehost,omitempty"`
	RequestID  string                 `json:"requestID,omitempty"`
	UserAgent  string                 `json:"userAgent,omitempty"`
	ReqClaims  map[string]interface{} `json:"requestClaims,omitempty"`
	ReqQuery   map[string]string      `json:"requestQuery,omitempty"`
	ReqHeader  map[string]string      `json:"requestHeader,omitempty"`
	RespHeader map[string]string      `json:"responseHeader,omitempty"`
	Tags       map[string]interface{} `json:"tags,omitempty"`
}

func (k *kafkaTarget) Send(e Entry) error {
	data, err := json.Marshal(&e)
	if err != nil {
		return err
	}

	key := e.API.Bucket + "/" + e.API.Object
	msg := sarama.ProducerMessage{
		Topic: k.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(data),
	}

	_, _, err = k.producer.SendMessage(&msg)
	return err
}

var logger = log.New(os.Stdout, "[http-kafka] ", log.LstdFlags)

func main() {
	kafkaTgt, err := kafkaEnvsToConfig()
	if err != nil {
		log.Fatalln(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/rest/kafka", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var e Entry
			if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
				logger.Println(err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := kafkaTgt.Send(e); err != nil {
				logger.Println(err)
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
		default:
			fmt.Fprintf(w, "Sorry, only POST method is supported.")
		}
	})

	s := &http.Server{
		Addr:           ":4222",
		Handler:        mux,
		MaxHeaderBytes: 1 << 20,
	}

	if err := s.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
