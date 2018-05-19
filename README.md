opencensus-influxdb
====
[![Build Status](https://travis-ci.org/kpacha/opencensus-influxdb.svg?branch=master)](https://travis-ci.org/kpacha/opencensus-influxdb) [![Go Report Card](https://goreportcard.com/badge/github.com/kpacha/opencensus-influxdb)](https://goreportcard.com/report/github.com/kpacha/opencensus-influxdb) [![GoDoc](https://godoc.org/github.com/kpacha/opencensus-influxdb?status.svg)](https://godoc.org/github.com/kpacha/opencensus-influxdb)

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