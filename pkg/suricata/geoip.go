package suricata

import (
	"errors"
	"net"

	"github.com/oschwald/geoip2-golang"
)

type GeoIPModel interface {
	UpdateGeoIP(reader *geoip2.Reader) error
}

type GeoIPData struct {
	CityName               string  `json:"city_name" parquet:"name=city_name, type=BYTE_ARRAY, convertedtype=UTF8"`
	ContinentCode          string  `json:"continent_code" parquet:"name=continent_code, type=BYTE_ARRAY, convertedtype=UTF8"`
	ContinentName          string  `json:"continent_name" parquet:"name=continent_name, type=BYTE_ARRAY, convertedtype=UTF8"`
	CountryIsoCode         string  `json:"country_iso_code" parquet:"name=country_iso_code, type=BYTE_ARRAY, convertedtype=UTF8"`
	CountryName            string  `json:"country_name" parquet:"name=country_name, type=BYTE_ARRAY, convertedtype=UTF8"`
	Latitude               float64 `json:"latitude" parquet:"name=latitude, type=DOUBLE"`
	Longitude              float64 `json:"longitude" parquet:"name=longitude, type=DOUBLE"`
	LocationAccuracyRadius int     `json:"location_accuracy_radius" parquet:"name=location_accuracy_radius, type=INT32"`
	TimeZone               string  `json:"time_zone" parquet:"name=time_zone, type=BYTE_ARRAY, convertedtype=UTF8"`
	PostalCode             string  `json:"postal_code" parquet:"name=postal_code, type=BYTE_ARRAY, convertedtype=UTF8"`
	IsAnonymousProxy       bool    `json:"is_anonymous_proxy" parquet:"name=is_anonymous_proxy, type=BOOLEAN"`
	IsSatelliteProvider    bool    `json:"is_satellite_provider" parquet:"name=is_satellite_provider, type=BOOLEAN"`
	Subdivisions           []struct {
		IsoCode string `json:"iso_code" parquet:"name=iso_code, type=BYTE_ARRAY, convertedtype=UTF8"`
		Name    string `json:"name" parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	} `json:"subdivisions" parquet:"name=subdivisions, type=LIST"`
}

func GetGeoIPData(reader *geoip2.Reader, ipString string) (*GeoIPData, error) {
	ip := net.ParseIP(ipString)
	if ip == nil {
		return nil, errors.New("invalid IP address " + ipString)
	}
	city, err := reader.City(ip)
	if err != nil {
		return nil, err
	}

	g := GeoIPData{}

	g.CityName = city.City.Names["en"]
	g.ContinentCode = city.Continent.Code
	g.ContinentName = city.Continent.Names["en"]
	g.CountryIsoCode = city.Country.IsoCode
	g.CountryName = city.Country.Names["en"]
	g.Latitude = city.Location.Latitude
	g.Longitude = city.Location.Longitude
	g.LocationAccuracyRadius = int(city.Location.AccuracyRadius)
	g.TimeZone = city.Location.TimeZone
	g.PostalCode = city.Postal.Code
	g.IsAnonymousProxy = city.Traits.IsAnonymousProxy
	g.IsSatelliteProvider = city.Traits.IsSatelliteProvider
	g.Subdivisions = make([]struct {
		IsoCode string `json:"iso_code" parquet:"name=iso_code, type=BYTE_ARRAY, convertedtype=UTF8"`
		Name    string `json:"name" parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	}, len(city.Subdivisions))
	for i, s := range city.Subdivisions {
		g.Subdivisions[i].IsoCode = s.IsoCode
		g.Subdivisions[i].Name = s.Names["en"]
	}
	return &g, nil
}
