
resource "aws_glue_catalog_table" "flow_events" {
  name          = "flow_events"
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
    location      = "s3://${var.bucket_name}/flow/"
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
      name    = "src_ip"
      type    = "string"
      comment = ""
    }
    columns {
      name    = "dest_ip"
      type    = "string"
      comment = ""
    }
    columns {
      name    = "src_port"
      type    = "int"
      comment = ""
    }
    columns {
      name    = "dest_port"
      type    = "int"
      comment = ""
    }
    columns {
      name    = "proto"
      type    = "string"
      comment = ""
    }
    columns {
      name    = "app_proto"
      type    = "string"
      comment = ""
    }
    columns {
      name    = "flow_id"
      type    = "bigint"
      comment = ""
    }
    columns {
      name    = "in_iface"
      type    = "string"
      comment = ""
    }
    columns {
      name    = "vlan"
      type    = "int"
      comment = ""
    }
    columns {
      name    = "tx_id"
      type    = "int"
      comment = ""
    }
    columns {
      name    = "flow"
      type    = "struct<pkts_toserver:bigint,pkts_toclient:bigint,bytes_toserver:bigint,bytes_toclient:bigint,start:string,end:string,age:int,state:string,reason:string,alerted:boolean>"
      comment = ""
    }
    columns {
      name    = "tcp"
      type    = "struct<tcp_flags:string,tcp_flags_ts:string,tcp_flags_tc:string,syn:boolean,rst:boolean,ack:boolean,ecn:boolean,cwr:boolean,psh:boolean,fin:boolean,urg:boolean,state:string>"
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
