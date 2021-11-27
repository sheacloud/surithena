package suricata

import (
	"strconv"
	"time"

	"github.com/oschwald/geoip2-golang"
	"github.com/sheacloud/surithena/internal/storage"
)

type FlowEvent struct {
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

	Flow struct {
		PktsToServer  int64  `json:"pkts_toserver" parquet:"name=pkts_toserver, type=INT64"`
		PktsToClient  int64  `json:"pkts_toclient" parquet:"name=pkts_toclient, type=INT64"`
		BytesToServer int64  `json:"bytes_toserver" parquet:"name=bytes_toserver, type=INT64"`
		BytesToClient int64  `json:"bytes_toclient" parquet:"name=bytes_toclient, type=INT64"`
		Start         string `json:"start" parquet:"name=start, type=BYTE_ARRAY, convertedtype=UTF8"`
		End           string `json:"end" parquet:"name=end, type=BYTE_ARRAY, convertedtype=UTF8"`
		Age           int    `json:"age" parquet:"name=age, type=INT32"`
		State         string `json:"state" parquet:"name=state, type=BYTE_ARRAY, convertedtype=UTF8"`
		Reason        string `json:"reason" parquet:"name=reason, type=BYTE_ARRAY, convertedtype=UTF8"`
		Alerted       bool   `json:"alerted" parquet:"name=alerted, type=BOOLEAN"`
	} `json:"flow" parquet:"name=flow"`

	TCP struct {
		TCPFlags   string `json:"tcp_flags" parquet:"name=tcp_flags, type=BYTE_ARRAY, convertedtype=UTF8"`
		TCPFlagsTS string `json:"tcp_flags_ts" parquet:"name=tcp_flags_ts, type=BYTE_ARRAY, convertedtype=UTF8"`
		TCPFlagsTC string `json:"tcp_flags_tc" parquet:"name=tcp_flags_tc, type=BYTE_ARRAY, convertedtype=UTF8"`
		Syn        bool   `json:"syn" parquet:"name=syn, type=BOOLEAN"`
		Rst        bool   `json:"rst" parquet:"name=rst, type=BOOLEAN"`
		Ack        bool   `json:"ack" parquet:"name=ack, type=BOOLEAN"`
		Ecn        bool   `json:"ecn" parquet:"name=ecn, type=BOOLEAN"`
		Cwr        bool   `json:"cwr" parquet:"name=cwr, type=BOOLEAN"`
		Psh        bool   `json:"psh" parquet:"name=psh, type=BOOLEAN"`
		Fin        bool   `json:"fin" parquet:"name=fin, type=BOOLEAN"`
		Urg        bool   `json:"urg" parquet:"name=urg, type=BOOLEAN"`
		State      string `json:"state" parquet:"name=state, type=BYTE_ARRAY, convertedtype=UTF8"`
	} `json:"tcp" parquet:"name=tcp"`

	GeoIPData struct {
		Source GeoIPData `json:"source" parquet:"name=source"`
		Dest   GeoIPData `json:"dest" parquet:"name=dest"`
	} `json:"geoip_data" parquet:"name=geoip_data"`
}

func (e *FlowEvent) UpdateGeoIP(reader *geoip2.Reader) error {
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

func (e FlowEvent) GetDateHourKey() storage.DateHourKey {
	hour, _ := strconv.Atoi(e.Timestamp[11:13])
	return storage.DateHourKey{
		Date: e.Timestamp[:10],
		Hour: hour,
	}
}

func (e *FlowEvent) UpdateFields() error {
	parsedTime, err := time.Parse("2006-01-02T15:04:05.999999-0700", e.Timestamp)
	if err != nil {
		return err
	}
	e.EventTime = parsedTime.UTC().UnixMilli()
	return nil
}
