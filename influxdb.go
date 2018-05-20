// Package influxdb contains a view exporter for InfluxDB.
//
// Please note that this exporter is currently work in progress and not complete.
package influxdb

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	client "github.com/influxdata/influxdb/client/v2"
	"go.opencensus.io/stats/view"
)

const (
	defaultBufferSize      = 1000 * 1000
	defaultReportingPeriod = 15 * time.Second
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

	// InstanceName is the name of the node writing the points.
	InstanceName string

	// BufferSize is the capacity of the buffer of batch points to send.
	BufferSize int

	// ReportingPeriod is the duration between two consecutive reports.
	ReportingPeriod time.Duration

	// OnError is the hook to be called when there is
	// an error occurred when uploading the stats data.
	// If no custom hook is set, errors are logged.
	// Optional.
	OnError func(err error)
}

// NewExporter returns an implementation of view.Exporter that uploads datapoints
// to an InfluxDB server.
func NewExporter(ctx context.Context, o Options) (*Exporter, error) {
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

	e := &Exporter{
		opts:    o,
		client:  c,
		buffer:  newBuffer(o.BufferSize),
		onError: onError,
	}

	reportingPeriod := o.ReportingPeriod
	if reportingPeriod <= 0 {
		reportingPeriod = defaultReportingPeriod
	}

	go e.flushBuffer(ctx, time.NewTicker(reportingPeriod))

	return e, nil
}

// Exporter is an implementation of view.Exporter that uploads stats to an
// InfluxDB server.
type Exporter struct {
	opts    Options
	client  Client
	buffer  *buffer
	onError func(error)
}

var _ view.Exporter = (*Exporter)(nil)

// ExportView processes the received view if its data has one or more rows. The
// generated points are added to the internal buffer for later exporting to
// InfluxDB
func (e *Exporter) ExportView(vd *view.Data) {
	if len(vd.Rows) == 0 {
		return
	}

	bp := e.batch()
	bp.AddPoints(e.viewToPoints(vd))

	e.buffer.Add(bp)
}

func (e *Exporter) flushBuffer(ctx context.Context, ticker *time.Ticker) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bps := e.buffer.Elements()
			if len(bps) == 0 {
				continue
			}
			bp := e.batch()
			for _, b := range bps {
				bp.AddPoints(b.Points())
			}
			if err := e.client.Write(bp); err != nil {
				e.buffer.Add(bps...)
				e.onError(exportError{err, bp})
			}
		}
	}
}

func (e *Exporter) batch() client.BatchPoints {
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  e.opts.Database,
		Precision: "s",
	})
	return bp
}

func (e *Exporter) viewToPoints(vd *view.Data) []*client.Point {
	pts := []*client.Point{}
	for _, row := range vd.Rows {
		f, err := e.toFields(row)
		if err != nil {
			continue
		}
		p, err := client.NewPoint(vd.View.Name, e.toTags(row), f, vd.End)
		if err != nil {
			continue
		}
		pts = append(pts, p)
		data, ok := row.Data.(*view.DistributionData)
		if !ok {
			continue
		}
		pts = append(pts, e.parseBuckets(vd.View, data, vd.End)...)
	}
	return pts
}

func (e *Exporter) toTags(row *view.Row) map[string]string {
	res := make(map[string]string, len(row.Tags)+1)
	for _, tag := range row.Tags {
		res[tag.Key.Name()] = tag.Value
	}
	res["instance"] = e.opts.InstanceName
	return res
}

func (e *Exporter) toFields(row *view.Row) (map[string]interface{}, error) {
	switch data := row.Data.(type) {
	case *view.CountData:
		return map[string]interface{}{"count": data.Value}, nil

	case *view.SumData:
		return map[string]interface{}{"sum": data.Value}, nil

	case *view.LastValueData:
		return map[string]interface{}{"last": data.Value}, nil

	case *view.DistributionData:
		return map[string]interface{}{
			"sum":   data.Sum(),
			"count": data.Count,
			"max":   data.Max,
			"min":   data.Min,
			"mean":  data.Mean,
		}, nil

	default:
		err := fmt.Errorf("aggregation %T is not yet supported", data)
		log.Print(err)
		return map[string]interface{}{}, err
	}
}

func (e *Exporter) parseBuckets(v *view.View, data *view.DistributionData, timestamp time.Time) []*client.Point {
	res := []*client.Point{}

	indicesMap := make(map[float64]int)
	buckets := make([]float64, 0, len(v.Aggregation.Buckets))
	for i, b := range v.Aggregation.Buckets {
		if _, ok := indicesMap[b]; !ok {
			indicesMap[b] = i
			buckets = append(buckets, b)
		}
	}
	for _, b := range buckets {
		if value := data.CountPerBucket[indicesMap[b]]; value != 0 {
			pt, err := client.NewPoint(
				v.Name+"_buckets",
				map[string]string{
					"bucket":   fmt.Sprintf("%.0f", b),
					"instance": e.opts.InstanceName,
				},
				map[string]interface{}{"count": value},
				timestamp,
			)
			if err != nil {
				continue
			}
			res = append(res, pt)
		}
	}

	return res
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

func newBuffer(size int) *buffer {
	if size == 0 {
		size = defaultBufferSize
	}
	return &buffer{
		data: []client.BatchPoints{},
		size: size,
		mu:   new(sync.Mutex),
	}
}

type buffer struct {
	data []client.BatchPoints
	size int
	mu   *sync.Mutex
}

func (b *buffer) Add(ps ...client.BatchPoints) {
	b.mu.Lock()
	b.data = append(b.data, ps...)
	if len(b.data) > b.size {
		b.data = b.data[len(b.data)-b.size:]
	}
	b.mu.Unlock()
}

func (b *buffer) Elements() []client.BatchPoints {
	var res []client.BatchPoints
	b.mu.Lock()
	res, b.data = b.data, []client.BatchPoints{}
	b.mu.Unlock()
	return res
}
