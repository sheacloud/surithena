
resource "aws_glue_catalog_table" "dns_events" {
  name          = "dns_events"
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
    location      = "s3://${var.bucket_name}/dns/"
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
      name    = "dns"
      type    = "struct<version:int,type:string,id:int,flags:string,qr:boolean,rd:boolean,ra:boolean,rrname:string,rrtype:string,rcode:string,answers:array<struct<rrname:string,rrtype:string,ttl:int,rdata:string>>>"
      comment = ""
    }
    columns {
      name    = "geoip_data"
      type    = "struct<source:struct<city_name:string,continent_code:string,continent_name:string,country_iso_code:string,country_name:string,latitude:double,longitude:double,location_accuracy_radius:int,time_zone:string,postal_code:string,is_anonymous_proxy:boolean,is_satellite_provider:boolean,subdivisions:array<struct<iso_code:string,name:string>>>,dest:struct<city_name:string,continent_code:string,continent_name:string,country_iso_code:string,country_name:string,latitude:double,longitude:double,location_accuracy_radius:int,time_zone:string,postal_code:string,is_anonymous_proxy:boolean,is_satellite_provider:boolean,subdivisions:array<struct<iso_code:string,name:string>>>>"
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
