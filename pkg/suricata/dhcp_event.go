package suricata

import (
	"strconv"
	"time"

	"github.com/oschwald/geoip2-golang"
	"github.com/sheacloud/surithena/internal/storage"
)

type DHCPEvent struct {
	Timestamp string `json:"timestamp"`
	EventTime int64  `parquet:"name=event_time, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	EventType string `json:"event_type"`
	SrcIP     string `json:"src_ip" parquet:"name=src_ip, type=BYTE_ARRAY, convertedtype=UTF8"`
	DestIP    string `json:"dest_ip" parquet:"name=dest_ip, type=BYTE_ARRAY, convertedtype=UTF8"`
	SrcPort   int    `json:"src_port" parquet:"name=src_port, type=INT32"`
	DestPort  int    `json:"dest_port" parquet:"name=dest_port, type=INT32"`
	Proto     string `json:"proto" parquet:"name=proto, type=BYTE_ARRAY, convertedtype=UTF8"`
	AppProto  string `json:"app_proto" parquet:"name=app_proto, type=BYTE_ARRAY, convertedtype=UTF8"`
	FlowID    int64  `json:"flow_id" parquet:"name=flow_id, type=INT64"`
	InIface   string `json:"in_iface" parquet:"name=in_iface, type=BYTE_ARRAY, convertedtype=UTF8"`
	Vlan      int    `json:"vlan" parquet:"name=vlan, type=INT32"`
	TxID      int    `json:"tx_id" parquet:"name=tx_id, type=INT32"`

	DHCP struct {
		Type        string `json:"type" parquet:"name=type, type=BYTE_ARRAY, convertedtype=UTF8"`
		ID          int    `json:"id" parquet:"name=id, type=INT32"`
		ClientMac   string `json:"client_mac" parquet:"name=client_mac, type=BYTE_ARRAY, convertedtype=UTF8"`
		AssignedIP  string `json:"assigned_ip" parquet:"name=assigned_ip, type=BYTE_ARRAY, convertedtype=UTF8"`
		DHCPType    string `json:"dhcp_type" parquet:"name=dhcp_type, type=BYTE_ARRAY, convertedtype=UTF8"`
		RenewalTime int    `json:"renewal_time" parquet:"name=renewal_time, type=INT32"`
	} `json:"dhcp" parquet:"name=dhcp"`

	GeoIPData struct {
		Source GeoIPData `json:"source" parquet:"name=source"`
		Dest   GeoIPData `json:"dest" parquet:"name=dest"`
	} `json:"geoip_data" parquet:"name=geoip_data"`
}

func (e *DHCPEvent) UpdateGeoIP(reader *geoip2.Reader) error {
	source, err := GetGeoIPData(reader, e.SrcIP)
	if err != nil {
		return err
	}
	e.GeoIPData.Source = *source
	dest, err := GetGeoIPData(reader, e.DestIP)
	if err != nil {
		return err
	}
	e.GeoIPData.Dest = *dest
	return nil
}

func (e DHCPEvent) GetDateHourKey() storage.DateHourKey {
	hour, _ := strconv.Atoi(e.Timestamp[11:13])
	return storage.DateHourKey{
		Date: e.Timestamp[:10],
		Hour: hour,
	}
}

func (e *DHCPEvent) UpdateFields() error {
	parsedTime, err := time.Parse("2006-01-02T15:04:05.999999-0700", e.Timestamp)
	if err != nil {
		return err
	}
	e.EventTime = parsedTime.UTC().UnixMilli()
	return nil
}
