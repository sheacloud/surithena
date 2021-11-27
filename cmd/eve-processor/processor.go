package main

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/oschwald/geoip2-golang"
	"github.com/sheacloud/surithena/internal/storage"
	"github.com/sheacloud/surithena/pkg/suricata"
	"github.com/sirupsen/logrus"
)

func ProcessEveEvent(workerNumber int, event string, mmdb *geoip2.Reader, writers map[string]*storage.RotatingWriter) error {
	eveEvent := suricata.EveBase{}
	err := json.Unmarshal([]byte(event), &eveEvent)
	if err != nil {
		return err
	}
	var eventObject storage.Rotatable
	switch eveEvent.EventType {
	case "alert":
		alertEvent := suricata.AlertEvent{}
		err = json.Unmarshal([]byte(event), &alertEvent)
		if err != nil {
			return err
		}
		eventObject = &alertEvent
	case "dns":
		dnsEvent := suricata.DNSEvent{}
		err = json.Unmarshal([]byte(event), &dnsEvent)
		if err != nil {
			return err
		}
		eventObject = &dnsEvent
	case "flow":
		flowEvent := suricata.FlowEvent{}
		err = json.Unmarshal([]byte(event), &flowEvent)
		if err != nil {
			return err
		}
		eventObject = &flowEvent
	case "http":
		httpEvent := suricata.HTTPEvent{}
		err = json.Unmarshal([]byte(event), &httpEvent)
		if err != nil {
			return err
		}
		eventObject = &httpEvent
	case "tls":
		tlsEvent := suricata.TLSEvent{}
		err = json.Unmarshal([]byte(event), &tlsEvent)
		if err != nil {
			return err
		}
		eventObject = &tlsEvent
	case "stats":
		statsEvent := suricata.StatsEvent{}
		err = json.Unmarshal([]byte(event), &statsEvent)
		if err != nil {
			return err
		}
		eventObject = &statsEvent
	case "dhcp":
		dhcpEvent := suricata.DHCPEvent{}
		err = json.Unmarshal([]byte(event), &dhcpEvent)
		if err != nil {
			return err
		}
		eventObject = &dhcpEvent
	default:
		eventObject = nil
	}

	if eventObject != nil {
		geoIPModel, ok := eventObject.(suricata.GeoIPModel)
		if ok {
			err = geoIPModel.UpdateGeoIP(mmdb)
			if err != nil {
				return err
			}
		}

		eventObject.UpdateFields()
		err = writers[eveEvent.EventType].Write(eventObject)
		if err != nil {
			fmt.Println(eveEvent.EventType, err)
			return err
		}
	}
	logrus.WithFields(logrus.Fields{
		"event_type":    eveEvent.EventType,
		"worker_number": workerNumber,
	}).Info("processed eve event")

	return nil
}

func Worker(eventChannel <-chan string, cancelChannel <-chan bool, workerWaitGroup *sync.WaitGroup, workerNum int, mmdb *geoip2.Reader, writers map[string]*storage.RotatingWriter) {
	logrus.Info("Started eve processor worker")
InfiniteLoop:
	for {
		select {
		case <-cancelChannel:
			break InfiniteLoop
		case event := <-eventChannel:
			err := ProcessEveEvent(workerNum, event, mmdb, writers)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
	logrus.Info("Ended eve processor worker")
	workerWaitGroup.Done()
}
