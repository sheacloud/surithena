package suricata

import (
	"strconv"
	"time"

	"github.com/oschwald/geoip2-golang"
	"github.com/sheacloud/surithena/internal/storage"
)

type HTTPEvent struct {
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

	HTTP struct {
		HTTPPort        int    `json:"http_port" parquet:"name=http_port, type=INT32"`
		Hostname        string `json:"hostname" parquet:"name=hostname, type=BYTE_ARRAY, convertedtype=UTF8"`
		URL             string `json:"url" parquet:"name=url, type=BYTE_ARRAY, convertedtype=UTF8"`
		HTTPUserAgent   string `json:"http_user_agent" parquet:"name=http_user_agent, type=BYTE_ARRAY, convertedtype=UTF8"`
		HTTPContentType string `json:"http_content_type" parquet:"name=http_content_type, type=BYTE_ARRAY, convertedtype=UTF8"`
		HTTPRefer       string `json:"http_refer" parquet:"name=http_refer, type=BYTE_ARRAY, convertedtype=UTF8"`
		HTTPMethod      string `json:"http_method" parquet:"name=http_method, type=BYTE_ARRAY, convertedtype=UTF8"`
		Protocol        string `json:"protocol" parquet:"name=protocol, type=BYTE_ARRAY, convertedtype=UTF8"`
		Status          int    `json:"status" parquet:"name=status, type=INT32"`
		Length          int    `json:"length" parquet:"name=length, type=INT32"`
	} `json:"http" parquet:"name=http"`

	GeoIPData struct {
		Source GeoIPData `json:"source" parquet:"name=source"`
		Dest   GeoIPData `json:"dest" parquet:"name=dest"`
	} `json:"geoip_data" parquet:"name=geoip_data"`
}

func (e *HTTPEvent) UpdateGeoIP(reader *geoip2.Reader) error {
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

func (e HTTPEvent) GetDateHourKey() storage.DateHourKey {
	hour, _ := strconv.Atoi(e.Timestamp[11:13])
	return storage.DateHourKey{
		Date: e.Timestamp[:10],
		Hour: hour,
	}
}

func (e *HTTPEvent) UpdateFields() error {
	parsedTime, err := time.Parse("2006-01-02T15:04:05.999999-0700", e.Timestamp)
	if err != nil {
		return err
	}
	e.EventTime = parsedTime.UTC().UnixMilli()
	return nil
}
