package suricata

import (
	"time"

	"github.com/sheacloud/surithena/internal/storage"
)

type TLSEvent struct {
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

	TLS struct {
		Subject     string `json:"subject" parquet:"name=subject, type=BYTE_ARRAY, convertedtype=UTF8"`
		IssuerDN    string `json:"issuerdn" parquet:"name=issuerdn, type=BYTE_ARRAY, convertedtype=UTF8"`
		Serial      string `json:"serial" parquet:"name=serial, type=BYTE_ARRAY, convertedtype=UTF8"`
		Fingerprint string `json:"fingerprint" parquet:"name=fingerprint, type=BYTE_ARRAY, convertedtype=UTF8"`
		SNI         string `json:"sni" parquet:"name=sni, type=BYTE_ARRAY, convertedtype=UTF8"`
		Version     string `json:"version" parquet:"name=version, type=BYTE_ARRAY, convertedtype=UTF8"`
		NotBefore   string `json:"notbefore" parquet:"name=notbefore, type=BYTE_ARRAY, convertedtype=UTF8"`
		NotAfter    string `json:"notafter" parquet:"name=notafter, type=BYTE_ARRAY, convertedtype=UTF8"`
		JA3         struct {
			Hash   string `json:"hash" parquet:"name=hash, type=BYTE_ARRAY, convertedtype=UTF8"`
			String string `json:"string" parquet:"name=string, type=BYTE_ARRAY, convertedtype=UTF8"`
		} `json:"ja3"`
		JA3S struct {
			Hash   string `json:"hash" parquet:"name=hash, type=BYTE_ARRAY, convertedtype=UTF8"`
			String string `json:"string" parquet:"name=string, type=BYTE_ARRAY, convertedtype=UTF8"`
		} `json:"ja3s"`
	} `json:"tls" parquet:"name=tls"`
}

func (e TLSEvent) GetDateHourKey() storage.DateHourKey {
	return storage.DateHourKey{
		Date: e.Timestamp[:10],
		Hour: e.Timestamp[11:13],
	}
}

func (e *TLSEvent) UpdateFields() error {
	parsedTime, err := time.Parse("2006-01-02T15:04:05.999999-0700", e.Timestamp)
	if err != nil {
		return err
	}
	e.EventTime = parsedTime.UTC().UnixMilli()
	return nil
}
