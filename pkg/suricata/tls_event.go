package suricata

import (
	"strconv"
	"time"

	"github.com/oschwald/geoip2-golang"
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

	Traffic *struct {
		ID    []string `json:"id" parquet:"name=id, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
		Label []string `json:"label" parquet:"name=label, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	} `json:"traffic" parquet:"name=traffic"`

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
		} `json:"ja3" parquet:"name=ja3"`
		JA3S struct {
			Hash   string `json:"hash" parquet:"name=hash, type=BYTE_ARRAY, convertedtype=UTF8"`
			String string `json:"string" parquet:"name=string, type=BYTE_ARRAY, convertedtype=UTF8"`
		} `json:"ja3s" parquet:"name=ja3s"`
	} `json:"tls" parquet:"name=tls"`

	GeoIPData struct {
		Source GeoIPData `json:"source" parquet:"name=source"`
		Dest   GeoIPData `json:"dest" parquet:"name=dest"`
	} `json:"geoip_data" parquet:"name=geoip_data"`
}

func (e *TLSEvent) UpdateGeoIP(reader *geoip2.Reader) error {
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

func (e TLSEvent) GetDateHourKey() storage.DateHourKey {
	hour, _ := strconv.Atoi(e.Timestamp[11:13])
	return storage.DateHourKey{
		Date: e.Timestamp[:10],
		Hour: hour,
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
