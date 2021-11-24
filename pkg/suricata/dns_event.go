package suricata

import (
	"time"

	"github.com/sheacloud/surithena/internal/storage"
)

type DNSEvent struct {
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

	DNS *struct {
		Version int    `json:"version" parquet:"name=version, type=INT32"`
		Type    string `json:"type" parquet:"name=type, type=BYTE_ARRAY, convertedtype=UTF8"`
		ID      int    `json:"id" parquet:"name=id, type=INT32"`
		Flags   string `json:"flags" parquet:"name=flags, type=BYTE_ARRAY, convertedtype=UTF8"`
		QR      bool   `json:"qr" parquet:"name=qr, type=BOOLEAN"`
		RD      bool   `json:"rd" parquet:"name=rd, type=BOOLEAN"`
		RA      bool   `json:"ra" parquet:"name=ra, type=BOOLEAN"`
		RRName  string `json:"rrname" parquet:"name=rrname, type=BYTE_ARRAY, convertedtype=UTF8"`
		RRType  string `json:"rrtype" parquet:"name=rrtype, type=BYTE_ARRAY, convertedtype=UTF8"`
		RCode   string `json:"rcode" parquet:"name=rcode, type=BYTE_ARRAY, convertedtype=UTF8"`
		Answers []struct {
			RRName string `json:"rrname" parquet:"name=rrname, type=BYTE_ARRAY, convertedtype=UTF8"`
			RRType string `json:"rrtype" parquet:"name=rrtype, type=BYTE_ARRAY, convertedtype=UTF8"`
			TTL    int    `json:"ttl" parquet:"name=ttl, type=INT32"`
			RData  string `json:"rdata" parquet:"name=rdata, type=BYTE_ARRAY, convertedtype=UTF8"`
		} `json:"answers"`
	} `json:"dns" parquet:"name=dns"`
}

func (e DNSEvent) GetDateHourKey() storage.DateHourKey {
	return storage.DateHourKey{
		Date: e.Timestamp[:10],
		Hour: e.Timestamp[11:13],
	}
}

func (e *DNSEvent) UpdateFields() error {
	parsedTime, err := time.Parse("2006-01-02T15:04:05.999999-0700", e.Timestamp)
	if err != nil {
		return err
	}
	e.EventTime = parsedTime.UTC().UnixMilli()
	return nil
}
