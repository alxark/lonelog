package filters

import (
	"log"
	"github.com/alxark/lonelog/internal/structs"
	"github.com/oschwald/geoip2-golang"
	"net"
	"strconv"
)

type GeoipFilter struct {
	BasicFilter

	Database string
	Lang string

	log log.Logger
}

func NewGeoipFilter(options map[string]string, logger log.Logger) (g *GeoipFilter, err error) {
	g = &GeoipFilter{}

	if database, ok := options["database"]; ok {
		g.Database = database
	}

	if lang, ok := options["lang"]; ok {
		g.Lang = lang
	} else {
		g.Lang = "en"
	}

	g.log = logger

	return g, nil
}

/**
 * Split content field by delimiter
 */
func (g *GeoipFilter) Proceed(input chan structs.Message, output chan structs.Message) (err error) {
	g.log.Printf("GeoIP filter, database from %s, check field: %s", g.Database, g.Field)

	db, err := geoip2.Open(g.Database)
	if err != nil {
		g.log.Fatal(err.Error())
	}
	defer db.Close()

	for msg := range input {
		if ip, ok := msg.Payload[g.Field]; ok {
			ipNet := net.ParseIP(ip)

			if ipNet != nil {
				record, err := db.City(ipNet)
				if err == nil {
					payload := msg.Payload

					payload["geoip_country_code"] = record.Country.IsoCode[0:2]
					payload["geoip_city_name"] = record.City.Names[g.Lang]

					if len(record.Subdivisions) > 0 {
						payload["geoip_region_name"] = record.Subdivisions[0].Names[g.Lang]
					} else {
						payload["geoip_region_name"] = ""
					}

					payload["geoip_latitude"] = strconv.FormatFloat(record.Location.Latitude, 'f', -1, 64)
					payload["geoip_longitude"] = strconv.FormatFloat(record.Location.Longitude, 'f', -1, 64)

					msg.Payload = payload
					if g.Debug {
						g.log.Print(record)
					}
				}
			}
		}

		output <- msg
	}

	g.log.Printf("Channel processing finished. Exiting")

	return
}
