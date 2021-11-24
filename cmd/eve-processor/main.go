package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sheacloud/surithena/internal/storage"
	"github.com/sheacloud/surithena/pkg/suricata"
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
		log.Fatalf("failed to load configuration, %v", err)
	}

	s3Client := s3.NewFromConfig(cfg)

	if err != nil {
		panic(err)
	}
	l, err := net.Listen("unix", "/tmp/eve.sock")
	if err != nil {
		panic(err)
	}
	defer os.Remove("/tmp/eve.sock")
	os.Chmod("/tmp/eve.sock", 0777)

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
		writers[name] = storage.NewRotatingWriter(s3Client, "sheacloud-core-surithena", name)
	}

	go func() {
		for eveJSON := range eveChannel {
			eveEvent := suricata.EveBase{}
			err = json.Unmarshal([]byte(eveJSON), &eveEvent)
			if err != nil {
				panic(err)
			}
			fmt.Println("got event ", eveEvent.EventType)
			var eventObject storage.Rotatable
			switch eveEvent.EventType {
			case "alert":
				alertEvent := suricata.AlertEvent{}
				err = json.Unmarshal([]byte(eveJSON), &alertEvent)
				if err != nil {
					panic(err)
				}
				eventObject = &alertEvent
			case "dns":
				dnsEvent := suricata.DNSEvent{}
				err = json.Unmarshal([]byte(eveJSON), &dnsEvent)
				if err != nil {
					panic(err)
				}
				eventObject = &dnsEvent
			case "flow":
				flowEvent := suricata.FlowEvent{}
				err = json.Unmarshal([]byte(eveJSON), &flowEvent)
				if err != nil {
					panic(err)
				}
				eventObject = &flowEvent
			case "http":
				httpEvent := suricata.HTTPEvent{}
				err = json.Unmarshal([]byte(eveJSON), &httpEvent)
				if err != nil {
					panic(err)
				}
				eventObject = &httpEvent
			case "tls":
				tlsEvent := suricata.TLSEvent{}
				err = json.Unmarshal([]byte(eveJSON), &tlsEvent)
				if err != nil {
					panic(err)
				}
				eventObject = &tlsEvent
			case "stats":
				statsEvent := suricata.StatsEvent{}
				err = json.Unmarshal([]byte(eveJSON), &statsEvent)
				if err != nil {
					panic(err)
				}
				eventObject = &statsEvent
			case "dhcp":
				dhcpEvent := suricata.DHCPEvent{}
				err = json.Unmarshal([]byte(eveJSON), &dhcpEvent)
				if err != nil {
					panic(err)
				}
				eventObject = &dhcpEvent
			default:
				eventObject = nil
			}

			if eventObject != nil {
				eventObject.UpdateFields()
				err = writers[eveEvent.EventType].Write(eventObject)
				if err != nil {
					fmt.Println(eveEvent.EventType, err)
					panic(err)
				}
			}
		}
	}()

	<-stopChannel

	for _, writer := range writers {
		err = writer.Close()
		if err != nil {
			fmt.Println(err)
		}
	}

	fmt.Println("closed all writers")
}
