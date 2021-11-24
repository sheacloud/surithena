package suricata

import (
	"time"

	"github.com/sheacloud/surithena/internal/storage"
)

type AlertEvent struct {
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

	Alert struct {
		Action      string `json:"action" parquet:"name=action, type=BYTE_ARRAY, convertedtype=UTF8"`
		GID         int    `json:"gid" parquet:"name=gid, type=INT32"`
		SignatureID int    `json:"signature_id" parquet:"name=signature_id, type=INT32"`
		Rev         int    `json:"rev" parquet:"name=rev, type=INT32"`
		AppProto    string `json:"app_proto" parquet:"name=app_proto, type=BYTE_ARRAY, convertedtype=UTF8"`
		Signature   string `json:"signature" parquet:"name=signature, type=BYTE_ARRAY, convertedtype=UTF8"`
		Severity    int    `json:"severity" parquet:"name=severity, type=INT32"`
		Source      struct {
			IP   string `json:"ip" parquet:"name=ip, type=BYTE_ARRAY, convertedtype=UTF8"`
			Port int    `json:"port" parquet:"name=port, type=INT32"`
		} `json:"source"`
		Target struct {
			IP   string `json:"ip" parquet:"name=ip, type=BYTE_ARRAY, convertedtype=UTF8"`
			Port int    `json:"port" parquet:"name=port, type=INT32"`
		} `json:"target"`
	} `json:"alert" parquet:"name=alert"`
}

func (e AlertEvent) GetDateHourKey() storage.DateHourKey {
	return storage.DateHourKey{
		Date: e.Timestamp[:10],
		Hour: e.Timestamp[11:13],
	}
}

func (e *AlertEvent) UpdateFields() error {
	parsedTime, err := time.Parse("2006-01-02T15:04:05.999999-0700", e.Timestamp)
	if err != nil {
		return err
	}
	e.EventTime = parsedTime.UTC().UnixMilli()
	return nil
}
