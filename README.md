opencensus-influxdb
====

InfluxDB exporter for the opencensus lib

## Installation

	$ go get -u github.com/kpacha/opencensus-influxdb

## Register the exporter

	e, err := influxdb.NewExporter(influxdb.Options{
		Database: "db",
		Address:  "http://example.tld",
		Username: "user",
		Password: "password",
	})
	if err != nil {
		log.Fatalf("unexpected error: %s", err.Error())
	}
	view.RegisterExporter(e)