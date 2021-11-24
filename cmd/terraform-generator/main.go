package main

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/fatih/structtag"
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/sheacloud/surithena/pkg/suricata"
)

var (
	FieldKindToType = map[reflect.Kind]string{
		reflect.Bool:    "boolean",
		reflect.String:  "string",
		reflect.Int:     "int",
		reflect.Int32:   "int",
		reflect.Int64:   "bigint",
		reflect.Float32: "float",
		reflect.Float64: "double",
	}
)

type BaseConfig struct {
	Resources []GlueCatalogTable `hcl:"resource,block"`
}

type GlueCatalogTable struct {
	ResourceLabel     string            `hcl:"resource_label,label"`
	NameLabel         string            `hcl:"name_label,label"`
	Name              string            `hcl:"name"`
	DatabaseName      string            `hcl:"database_name"`
	TableType         string            `hcl:"table_type"`
	Parameters        map[string]string `hcl:"parameters"`
	StorageDescriptor StorageDescriptor `hcl:"storage_descriptor,block"`
	PartitionKeys     []PartitionKeys   `hcl:"partition_keys,block"`
}

type StorageDescriptor struct {
	Location     string    `hcl:"location"`
	InputFormat  string    `hcl:"input_format"`
	OutputFormat string    `hcl:"output_format"`
	SerDeInfo    SerDeInfo `hcl:"ser_de_info,block"`
	Columns      []Columns `hcl:"columns,block"`
}

type SerDeInfo struct {
	Name                 string            `hcl:"name"`
	SerializationLibrary string            `hcl:"serialization_library"`
	Parameters           map[string]string `hcl:"parameters"`
}

type Columns struct {
	Name    string `hcl:"name"`
	Type    string `hcl:"type"`
	Comment string `hcl:"comment"`
}

type PartitionKeys struct {
	Name string `hcl:"name"`
	Type string `hcl:"type"`
}

func GetParquetNameTag(tag reflect.StructTag) (string, error) {
	tags, err := structtag.Parse(string(tag))
	if err != nil {
		return "", err
	}

	parquetTag, err := tags.Get("parquet")
	if err != nil {
		return "", err
	}
	parquetFieldName := strings.Split(parquetTag.Name, "=")[1]

	return parquetFieldName, nil
}

func ParquetFieldIsTimestamp(tag reflect.StructTag) bool {
	tags, err := structtag.Parse(string(tag))
	if err != nil {
		return false
	}

	parquetTag, err := tags.Get("parquet")
	if err != nil {
		return false
	}

	//FIXME parse the tags better
	return parquetTag.HasOption(" convertedtype=TIMESTAMP_MILLIS") || parquetTag.HasOption("convertedtype=TIMESTAMP_MILLIS")
}

//f should be the structfield corresponding to v
func GetFieldTypeParquetString(fieldType reflect.Type) string {

	if fieldType.Kind() == reflect.Ptr {
		fieldType = fieldType.Elem()
	}

	if fieldType.Kind() == reflect.Struct {
		//for each field of the struct, call GetFieldTypeParquetString
		parquetString := "struct<"
		for i := 0; i < fieldType.NumField(); i++ {
			subfieldType := fieldType.Field(i)

			tag := subfieldType.Tag

			parquetFieldName, err := GetParquetNameTag(tag)
			if err != nil {
				continue
			}

			var typeString string
			if ParquetFieldIsTimestamp(tag) {
				typeString = "timestamp"
			} else {
				typeString = GetFieldTypeParquetString(subfieldType.Type)
			}

			parquetString += fmt.Sprintf("%s:%s", parquetFieldName, typeString)
			if i != fieldType.NumField()-1 {
				parquetString += ","
			}
		}
		parquetString += ">"

		return parquetString
	} else if fieldType.Kind() == reflect.Array || fieldType.Kind() == reflect.Slice {
		parquetString := "array<"
		arrayType := fieldType.Elem()
		parquetString += GetFieldTypeParquetString(arrayType)
		parquetString += ">"
		return parquetString
	} else if fieldType.Kind() == reflect.Map {
		parquetString := "map<"
		keyType := fieldType.Key()
		elemType := fieldType.Elem()
		keyString := GetFieldTypeParquetString(keyType)
		elemString := GetFieldTypeParquetString(elemType)
		parquetString += keyString
		parquetString += ","
		parquetString += elemString
		parquetString += ">"
		return parquetString
	} else {
		return FieldKindToType[fieldType.Kind()]
	}
}

func ConvertStructToGlueTable(obj interface{}, name string) (GlueCatalogTable, error) {
	tableName := fmt.Sprintf("%s_events", name)
	table := GlueCatalogTable{
		ResourceLabel: "aws_glue_catalog_table",
		NameLabel:     tableName,
		Name:          tableName,
		DatabaseName:  "replace-me",
		TableType:     "EXTERNAL_TABLE",
		Parameters: map[string]string{
			"EXTERNAL":                       "TRUE",
			"parquet.compression":            "SNAPPY",
			"projection.event_date.type":     "date",
			"projection.event_date.range":    "NOW-1YEARS,NOW",
			"projection.event_date.format":   "yyyy-MM-dd",
			"projection.event_hour.type":     "integer",
			"projection.event_hour.range":    "0,23",
			"projection.event_hour.interval": "1",
			"projection.enabled":             "true",
		},
		StorageDescriptor: StorageDescriptor{
			Location:     "replaceme",
			InputFormat:  "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
			OutputFormat: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
			SerDeInfo: SerDeInfo{
				Name:                 "my-stream",
				SerializationLibrary: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
				Parameters: map[string]string{
					"serialization.format": "1",
				},
			},
			Columns: []Columns{},
		},
		PartitionKeys: []PartitionKeys{
			{
				Name: "event_date",
				Type: "date",
			},
			{
				Name: "event_hour",
				Type: "int",
			},
		},
	}

	objValue := reflect.ValueOf(obj)
	objValue = reflect.Indirect(objValue)
	objType := objValue.Type()

	//iterate over each field in obj
	for i := 0; i < objValue.NumField(); i++ {
		typeField := objType.Field(i)
		tag := typeField.Tag

		parquetFieldName, err := GetParquetNameTag(tag)
		if err != nil {
			continue
		}

		typeString := ""

		if ParquetFieldIsTimestamp(tag) {
			typeString = "timestamp"
		} else {
			typeString = GetFieldTypeParquetString(typeField.Type)
		}

		column := Columns{
			Name: parquetFieldName,
			Type: typeString,
		}
		table.StorageDescriptor.Columns = append(table.StorageDescriptor.Columns, column)
	}

	return table, nil
}

func main() {
	EventModels := map[string]interface{}{
		"alert": suricata.AlertEvent{},
		"dns":   suricata.DNSEvent{},
		"flow":  suricata.FlowEvent{},
		"http":  suricata.HTTPEvent{},
		"tls":   suricata.TLSEvent{},
		"stats": suricata.StatsEvent{},
		"dhcp":  suricata.DHCPEvent{},
	}

	for eventName, eventModel := range EventModels {
		tableName := fmt.Sprintf("%s_events", eventName)
		table, err := ConvertStructToGlueTable(eventModel, eventName)
		if err != nil {
			panic(err)
		}

		config := BaseConfig{
			Resources: []GlueCatalogTable{table},
		}

		hclFile := hclwrite.NewEmptyFile()
		gohcl.EncodeIntoBody(&config, hclFile.Body())

		//update database name to be variable reference
		rootBody := hclFile.Body()
		tableBlock := rootBody.FirstMatchingBlock("resource", []string{"aws_glue_catalog_table", tableName})
		tableBlock.Body().SetAttributeTraversal("database_name", hcl.Traversal{
			hcl.TraverseRoot{
				Name: "aws_glue_catalog_database",
			},
			hcl.TraverseAttr{
				Name: "surithena",
			},
			hcl.TraverseAttr{
				Name: "name",
			},
		})
		//update storage location to use interpolation
		storageDescriptor := tableBlock.Body().FirstMatchingBlock("storage_descriptor", []string{})
		// construct an interpolated string - see https://stackoverflow.com/questions/67945463/how-to-use-hcl-write-to-set-expressions-with for justification for this complexity
		locationTokens := hclwrite.Tokens{
			{
				Type:  hclsyntax.TokenOQuote,
				Bytes: []byte("\""),
			},
			{
				Type:  hclsyntax.TokenQuotedLit,
				Bytes: []byte("s3://"),
			},
			{
				Type:  hclsyntax.TokenTemplateInterp,
				Bytes: []byte("${"),
			},
			{
				Type:  hclsyntax.TokenIdent,
				Bytes: []byte("var"),
			},
			{
				Type:  hclsyntax.TokenDot,
				Bytes: []byte("."),
			},
			{
				Type:  hclsyntax.TokenIdent,
				Bytes: []byte("bucket_name"),
			},
			{
				Type:  hclsyntax.TokenTemplateSeqEnd,
				Bytes: []byte("}"),
			},
			{
				Type:  hclsyntax.TokenQuotedLit,
				Bytes: []byte(fmt.Sprintf("/%s/", eventName)),
			},
			{
				Type:  hclsyntax.TokenCQuote,
				Bytes: []byte("\""),
			},
		}
		storageDescriptor.Body().SetAttributeRaw("location", locationTokens)

		filename := fmt.Sprintf("terraform/%s.tf", eventName)
		err = os.WriteFile(filename, hclFile.Bytes(), 0755)
		if err != nil {
			panic(err)
		}
	}

}
