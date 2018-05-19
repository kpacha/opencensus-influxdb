package influxdb

import (
	"fmt"
	"log"
	"time"

	client "github.com/influxdata/influxdb/client/v2"
	"go.opencensus.io/stats/view"
)

// Options contains options for configuring the exporter.
type Options struct {
	// Address should be of the form "http://host:port"
	// or "http://[ipv6-host%zone]:port".
	Address string

	// Username is the influxdb username, optional.
	Username string

	// Password is the influxdb password, optional.
	Password string

	// Timeout for influxdb writes, defaults to no timeout.
	Timeout time.Duration

	// PingEnabled flags if the client should ping the server
	// after instantiation
	PingEnabled bool

	// Database is the database to write points to.
	Database string

	// OnError is the hook to be called when there is
	// an error occurred when uploading the stats data.
	// If no custom hook is set, errors are logged.
	// Optional.
	OnError func(err error)
}

// NewExporter returns an exporter that exports stats to Influx.
func NewExporter(o Options) (*Exporter, error) {
	c, err := newClient(o)
	if err != nil {
		return nil, err
	}

	onError := func(err error) {
		if o.OnError != nil {
			o.OnError(err)
			return
		}
		log.Printf("Error when uploading stats to Influx: %s", err.Error())
	}

	return &Exporter{
		opts:    o,
		client:  c,
		onError: onError,
	}, nil
}

// Exporter exports stats to Influxdb
type Exporter struct {
	opts    Options
	client  Client
	onError func(err error)
}

// ExportView exports to the Influx if view data has one or more rows.
func (e *Exporter) ExportView(vd *view.Data) {
	if len(vd.Rows) == 0 {
		return
	}

	bp := e.batch()
	bp.AddPoints(toPoints(vd))

	if err := e.client.Write(bp); err != nil {
		e.onError(exportError{err, bp})
	}
}

func (e *Exporter) batch() client.BatchPoints {
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  e.opts.Database,
		Precision: "s",
	})
	return bp
}

func toPoints(vd *view.Data) []*client.Point {
	pts := []*client.Point{}
	for _, row := range vd.Rows {
		p, err := client.NewPoint(vd.View.Name, toTags(row), toFields(vd.View, row))
		if err != nil {
			continue
		}
		pts = append(pts, p)
	}
	return pts
}

func toTags(row *view.Row) map[string]string {
	res := make(map[string]string, len(row.Tags))
	for _, tag := range row.Tags {
		res[tag.Key.Name()] = tag.Value
	}
	return res
}

func toFields(v *view.View, row *view.Row) map[string]interface{} {
	switch data := row.Data.(type) {
	case *view.CountData:
		return map[string]interface{}{"count": data.Value}

	case *view.SumData:
		return map[string]interface{}{"sum": data.Value}

	case *view.LastValueData:
		return map[string]interface{}{"last": data.Value}

	case *view.DistributionData:
		res := map[string]interface{}{
			"sum":   data.Sum(),
			"count": data.Count,
			"max":   data.Max,
			"min":   data.Min,
			"mean":  data.Mean,
		}

		indicesMap := make(map[float64]int)
		buckets := make([]float64, 0, len(v.Aggregation.Buckets))
		for i, b := range v.Aggregation.Buckets {
			if _, ok := indicesMap[b]; !ok {
				indicesMap[b] = i
				buckets = append(buckets, b)
			}
		}
		for _, b := range buckets {
			res[fmt.Sprintf("bucket_%.2f", b)] = uint64(data.CountPerBucket[indicesMap[b]])
		}

		return res

	default:
		log.Printf("aggregation %T is not yet supported", v.Aggregation)
		return map[string]interface{}{}
	}
}

// Client defines the interface of the influxdb client to be used
type Client interface {
	Ping(timeout time.Duration) (time.Duration, string, error)
	Write(bp client.BatchPoints) error
}

func newClient(o Options) (client.Client, error) {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     o.Address,
		Username: o.Username,
		Password: o.Password,
		Timeout:  o.Timeout,
	})

	if err != nil {
		return nil, err
	}

	if o.PingEnabled {
		if _, _, err := c.Ping(time.Second); err != nil {
			return nil, err
		}
	}

	return c, nil
}

type exportError struct {
	err error
	bp  client.BatchPoints
}

func (e exportError) Error() string {
	return e.err.Error()
}
