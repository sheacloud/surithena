
resource "aws_glue_catalog_table" "stats_events" {
  name          = "stats_events"
  database_name = aws_glue_catalog_database.surithena.name
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    EXTERNAL                         = "TRUE"
    "parquet.compression"            = "SNAPPY"
    "projection.enabled"             = "true"
    "projection.event_date.format"   = "yyyy-MM-dd"
    "projection.event_date.range"    = "NOW-1YEARS,NOW"
    "projection.event_date.type"     = "date"
    "projection.event_hour.interval" = "1"
    "projection.event_hour.range"    = "0,23"
    "projection.event_hour.type"     = "integer"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_name}/stats/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "my-stream"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    columns {
      name    = "event_time"
      type    = "timestamp"
      comment = ""
    }
    columns {
      name    = "stats"
      type    = "struct<uptime:bigint,capture:struct<kernel_packets:bigint,kernel_drops:bigint,errors:bigint>,decoder:struct<pkts:bigint,bytes:bigint,invalid:bigint,ipv4:bigint,ipv6:bigint,ethernet:bigint,chdlc:bigint,raw:bigint,null:bigint,sll:bigint,tcp:bigint,udp:bigint,sctp:bigint,icmpv4:bigint,icmpv6:bigint,ppp:bigint,pppoe:bigint,geneve:bigint,gre:bigint,vlan:bigint,vlan_qinq:bigint,vxlan:bigint,vntag:bigint,ieee8021ah:bigint,teredo:bigint,ipv4_in_ipv6:bigint,ipv6_in_ipv6:bigint,mpls:bigint,avg_packet_size:bigint,max_packet_size:bigint,max_mac_addrs_src:bigint,max_mac_addrs_dst:bigint,erspan:bigint>,flow:struct<memcap:bigint,tcp:bigint,udp:bigint,icmpv4:bigint,icmpv6:bigint,tcp_reuse:bigint,get_used:bigint,get_used_eval:bigint,get_used_eval_reject:bigint,get_used_eval_busy:bigint,get_used_failed:bigint>,tcp:struct<sessions:bigint,ssn_memcap_drop:bigint,pseudo:bigint,pseudo_failed:bigint,invalid_checksum:bigint,no_flow:bigint,syn:bigint,synack:bigint,rst:bigint>>"
      comment = ""
    }
  }

  partition_keys {
    name = "event_date"
    type = "date"
  }
  partition_keys {
    name = "event_hour"
    type = "int"
  }
}
