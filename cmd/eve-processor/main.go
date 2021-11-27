package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/oschwald/geoip2-golang"
	"github.com/sheacloud/surithena/internal/storage"
	"github.com/sheacloud/surithena/pkg/suricata"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	EventModels = map[string]interface{}{
		"alert": suricata.AlertEvent{},
		"dns":   suricata.DNSEvent{},
		"flow":  suricata.FlowEvent{},
		"http":  suricata.HTTPEvent{},
		"tls":   suricata.TLSEvent{},
		"stats": suricata.StatsEvent{},
		"dhcp":  suricata.DHCPEvent{},
	}
)

func signalHandler(stopCh chan struct{}) {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	for range signalCh {
		close(stopCh)
		return
	}
}

func init() {
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	viper.AutomaticEnv()

	viper.BindEnv("eve_socket_path")
	viper.SetDefault("eve_socket_path", "/tmp/eve.sock")

	viper.BindEnv("mmdb_path")
	viper.SetDefault("mmdb_path", "/var/lib/eve-processor/GeoLite2-City.mmdb")

	viper.BindEnv("s3_bucket_name")

	viper.BindEnv("worker_threads")
	viper.SetDefault("worker_threads", 10)

	viper.BindEnv("file_timeout_minutes")
	viper.SetDefault("file_timeout_minutes", 5)

	viper.BindEnv("file_max_age_minutes")
	viper.SetDefault("file_max_age_minutes", 15)

	viper.BindEnv("file_max_size_bytes")
	viper.SetDefault("file_max_size_bytes", 2000)
}

func serve(c net.Conn, outputChan chan<- string) {
	defer c.Close()

	scanner := bufio.NewScanner(c)
	for scanner.Scan() {
		outputChan <- scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
}

func main() {
	stopChannel := make(chan struct{})
	go signalHandler(stopChannel)

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-1"))
	if err != nil {
		logrus.Fatalf("failed to load configuration, %v", err)
	}

	s3Client := s3.NewFromConfig(cfg)

	if err != nil {
		panic(err)
	}
	l, err := net.Listen("unix", viper.GetString("eve_socket_path"))
	if err != nil {
		panic(err)
	}
	defer os.Remove(viper.GetString("eve_socket_path"))
	os.Chmod(viper.GetString("eve_socket_path"), 0777)

	mmdb, err := geoip2.Open(viper.GetString("mmdb_path"))
	if err != nil {
		logrus.Fatal(err)
	}
	defer mmdb.Close()

	eveChannel := make(chan string)
	go func() {
		for {
			fd, err := l.Accept()
			if err != nil {
				panic(err)
			}
			go serve(fd, eveChannel)
		}
	}()

	writers := map[string]*storage.RotatingWriter{}
	for name := range EventModels {
		writers[name] = storage.NewRotatingWriter(s3Client, viper.GetString("s3_bucket_name"), name, viper.GetInt("file_timeout_minutes"), viper.GetInt("file_max_age_minutes"), viper.GetInt64("file_max_size_bytes"))
	}

	cancelChannels := []chan bool{}
	workerWaitGroup := &sync.WaitGroup{}
	for i := 0; i < viper.GetInt("worker_threads"); i++ {
		cancelCh := make(chan bool)
		cancelChannels = append(cancelChannels, cancelCh)
		workerWaitGroup.Add(1)
		go func(cancelCh <-chan bool, workerNumber int) {
			Worker(eveChannel, cancelCh, workerWaitGroup, workerNumber, mmdb, writers)
		}(cancelCh, i)
	}

	<-stopChannel

	logrus.Info("received interupt signal")

	for _, cancelCh := range cancelChannels {
		cancelCh <- true
	}

	logrus.Info("sent cancel signal to all worker threads")

	workerWaitGroup.Wait()

	logrus.Info("worker threads have been stopped")

	for _, writer := range writers {
		err = writer.Close()
		if err != nil {
			fmt.Println(err)
		}
	}

	logrus.Info("closed all S3 parquet writers")
}
