package suricata

import (
	"strconv"
	"time"

	"github.com/sheacloud/surithena/internal/storage"
)

type StatsEvent struct {
	Timestamp string `json:"timestamp"`
	EventTime int64  `parquet:"name=event_time, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	EventType string `json:"event_type"`

	Stats struct {
		Uptime  int64 `json:"uptime" parquet:"name=uptime, type=INT64"`
		Capture struct {
			KernelPackets int64 `json:"kernel_packets" parquet:"name=kernel_packets, type=INT64"`
			KernelDrops   int64 `json:"kernel_drops" parquet:"name=kernel_drops, type=INT64"`
			Errors        int64 `json:"errors" parquet:"name=errors, type=INT64"`
		} `json:"capture" parquet:"name=capture"`
		Decoder struct {
			Pkts           int64 `json:"pkts" parquet:"name=pkts, type=INT64"`
			Bytes          int64 `json:"bytes" parquet:"name=bytes, type=INT64"`
			Invalid        int64 `json:"invalid" parquet:"name=invalid, type=INT64"`
			IPv4           int64 `json:"ipv4" parquet:"name=ipv4, type=INT64"`
			IPv6           int64 `json:"ipv6" parquet:"name=ipv6, type=INT64"`
			Ethernet       int64 `json:"ethernet" parquet:"name=ethernet, type=INT64"`
			Chdlc          int64 `json:"chdlc" parquet:"name=chdlc, type=INT64"`
			Raw            int64 `json:"raw" parquet:"name=raw, type=INT64"`
			Null           int64 `json:"null" parquet:"name=null, type=INT64"`
			SLL            int64 `json:"sll" parquet:"name=sll, type=INT64"`
			TCP            int64 `json:"tcp" parquet:"name=tcp, type=INT64"`
			UDP            int64 `json:"udp" parquet:"name=udp, type=INT64"`
			SCTP           int64 `json:"sctp" parquet:"name=sctp, type=INT64"`
			ICMPv4         int64 `json:"icmpv4" parquet:"name=icmpv4, type=INT64"`
			ICMPv6         int64 `json:"icmpv6" parquet:"name=icmpv6, type=INT64"`
			PPP            int64 `json:"ppp" parquet:"name=ppp, type=INT64"`
			PPPoE          int64 `json:"pppoe" parquet:"name=pppoe, type=INT64"`
			Geneve         int64 `json:"geneve" parquet:"name=geneve, type=INT64"`
			GRE            int64 `json:"gre" parquet:"name=gre, type=INT64"`
			VLAN           int64 `json:"vlan" parquet:"name=vlan, type=INT64"`
			VLANQinQ       int64 `json:"vlan_qinq" parquet:"name=vlan_qinq, type=INT64"`
			VXLAN          int64 `json:"vxlan" parquet:"name=vxlan, type=INT64"`
			VNTAG          int64 `json:"vntag" parquet:"name=vntag, type=INT64"`
			IEEE8021ah     int64 `json:"ieee8021ah" parquet:"name=ieee8021ah, type=INT64"`
			Teredo         int64 `json:"teredo" parquet:"name=teredo, type=INT64"`
			IPv4InIPv6     int64 `json:"ipv4_in_ipv6" parquet:"name=ipv4_in_ipv6, type=INT64"`
			IPv6InIPv6     int64 `json:"ipv6_in_ipv6" parquet:"name=ipv6_in_ipv6, type=INT64"`
			MPLS           int64 `json:"mpls" parquet:"name=mpls, type=INT64"`
			AvgPacketSize  int64 `json:"avg_packet_size" parquet:"name=avg_packet_size, type=INT64"`
			MaxPacketSize  int64 `json:"max_packet_size" parquet:"name=max_packet_size, type=INT64"`
			MaxMacAddrsSrc int64 `json:"max_mac_addrs_src" parquet:"name=max_mac_addrs_src, type=INT64"`
			MaxMacAddrsDst int64 `json:"max_mac_addrs_dst" parquet:"name=max_mac_addrs_dst, type=INT64"`
			ERSpan         int64 `json:"erspan" parquet:"name=erspan, type=INT64"`
		} `json:"decoder" parquet:"name=decoder"`
		Flow struct {
			Memcap            int64 `json:"memcap" parquet:"name=memcap, type=INT64"`
			TCP               int64 `json:"tcp" parquet:"name=tcp, type=INT64"`
			UDP               int64 `json:"udp" parquet:"name=udp, type=INT64"`
			ICMPv4            int64 `json:"icmpv4" parquet:"name=icmpv4, type=INT64"`
			ICMPv6            int64 `json:"icmpv6" parquet:"name=icmpv6, type=INT64"`
			TCPReuse          int64 `json:"tcp_reuse" parquet:"name=tcp_reuse, type=INT64"`
			GetUsed           int64 `json:"get_used" parquet:"name=get_used, type=INT64"`
			GetUsedEval       int64 `json:"get_used_eval" parquet:"name=get_used_eval, type=INT64"`
			GetUsedEvalReject int64 `json:"get_used_eval_reject" parquet:"name=get_used_eval_reject, type=INT64"`
			GetUsedEvalBusy   int64 `json:"get_used_eval_busy" parquet:"name=get_used_eval_busy, type=INT64"`
			GetUsedFailed     int64 `json:"get_used_failed" parquet:"name=get_used_failed, type=INT64"`
		} `json:"flow" parquet:"name=flow"`
		TCP struct {
			Sessions        int64 `json:"sessions" parquet:"name=sessions, type=INT64"`
			SSNMemcapDrop   int64 `json:"ssn_memcap_drop" parquet:"name=ssn_memcap_drop, type=INT64"`
			Pseudo          int64 `json:"pseudo" parquet:"name=pseudo, type=INT64"`
			PseudoFailed    int64 `json:"pseudo_failed" parquet:"name=pseudo_failed, type=INT64"`
			InvalidChecksum int64 `json:"invalid_checksum" parquet:"name=invalid_checksum, type=INT64"`
			NoFlow          int64 `json:"no_flow" parquet:"name=no_flow, type=INT64"`
			Syn             int64 `json:"syn" parquet:"name=syn, type=INT64"`
			Synack          int64 `json:"synack" parquet:"name=synack, type=INT64"`
			Rst             int64 `json:"rst" parquet:"name=rst, type=INT64"`
		} `json:"tcp" parquet:"name=tcp"`
	} `json:"stats" parquet:"name=stats"`
}

func (e StatsEvent) GetDateHourKey() storage.DateHourKey {
	hour, _ := strconv.Atoi(e.Timestamp[11:13])
	return storage.DateHourKey{
		Date: e.Timestamp[:10],
		Hour: hour,
	}
}

func (e *StatsEvent) UpdateFields() error {
	parsedTime, err := time.Parse("2006-01-02T15:04:05.999999-0700", e.Timestamp)
	if err != nil {
		return err
	}
	e.EventTime = parsedTime.UTC().UnixMilli()
	return nil
}
