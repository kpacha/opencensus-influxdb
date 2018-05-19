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

	go e.flushBuffer(context.Background(), time.NewTicker(reportingPeriod))

	return e, nil
}

// Exporter exports stats to Influxdb
type Exporter struct {
	opts    Options
	client  Client
	buffer  *buffer
	onError func(error)
}

// ExportView exports to the Influx if view data has one or more rows.
func (e *Exporter) ExportView(vd *view.Data) {
	if len(vd.Rows) == 0 {
		return
	}

	bp := e.batch()
	bp.AddPoints(viewToPoints(vd))

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

func viewToPoints(vd *view.Data) []*client.Point {
	pts := []*client.Point{}
	for _, row := range vd.Rows {
		f, err := toFields(row)
		if err != nil {
			continue
		}
		p, err := client.NewPoint(vd.View.Name, toTags(row), f, vd.End)
		if err != nil {
			continue
		}
		pts = append(pts, p)
		data, ok := row.Data.(*view.DistributionData)
		if !ok {
			continue
		}
		buckets, err := client.NewPoint(vd.View.Name+"_buckets", toTags(row), parseBuckets(vd.View, data), vd.End)
		if err != nil {
			continue
		}
		pts = append(pts, buckets)
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

func toFields(row *view.Row) (map[string]interface{}, error) {
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

func parseBuckets(v *view.View, data *view.DistributionData) map[string]interface{} {
	res := make(map[string]interface{}, len(v.Aggregation.Buckets))

	indicesMap := make(map[float64]int)
	buckets := make([]float64, 0, len(v.Aggregation.Buckets))
	for i, b := range v.Aggregation.Buckets {
		if _, ok := indicesMap[b]; !ok {
			indicesMap[b] = i
			buckets = append(buckets, b)
		}
	}
	for _, b := range buckets {
		if v := data.CountPerBucket[indicesMap[b]]; v != 0 {
			res[fmt.Sprintf("%.0f", b)] = v
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
