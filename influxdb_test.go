package influxdb

import (
	"bytes"
	"context"
	"errors"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
)

var (
	frontendKey tag.Key
)

func TestExporter_ExportView_DistributionData(t *testing.T) {
	ctx := context.Background()

	view.SetReportingPeriod(time.Second)

	e := &Exporter{
		opts:   Options{},
		buffer: newBuffer(100),
		client: &dummyClient{
			WriteFunc: func(bp client.BatchPoints) error {
				if len(bp.Points()) != 2 {
					t.Errorf("unexpected number of points: %d", len(bp.Points()))
					return nil
				}
				for _, v := range bp.Points() {
					switch v.Name() {
					case "my.org/views/video_size":
						if len(v.Tags()) != 1 {
							t.Errorf("unexpected number of tags: %d", len(v.Tags()))
							return nil
						}
						if !reflect.DeepEqual(v.Tags(), map[string]string{
							"my.org/keys/frontend": "mobile-ios9.3.5",
						}) {
							t.Errorf("unexpected tag values: %v", v.Tags())
							return nil
						}
						fields, _ := v.Fields()
						if min := fields["min"].(float64); min != 2564 {
							t.Errorf("unexpected field values: %v", fields)
							return nil
						}
						if count := fields["count"].(int64); count != 10 {
							t.Errorf("unexpected field values: %v", fields)
							return nil
						}
					case "my.org/views/video_size_buckets":
					default:
						t.Errorf("unexpected point name: %s", v.Name())
						return nil
					}
				}
				return nil
			},
			PingFunc: func(timeout time.Duration) (time.Duration, string, error) {
				return 0, "", nil
			},
		},
	}

	view.RegisterExporter(e)
	defer view.UnregisterExporter(e)

	var err error
	frontendKey, err := tag.NewKey("my.org/keys/frontend")
	if err != nil {
		t.Error(err)
	}
	videoSize := stats.Int64("my.org/measure/video_size", "size of processed videos", stats.UnitBytes)

	v := &view.View{
		Name:        "my.org/views/video_size",
		Description: "processed video size over time",
		TagKeys:     []tag.Key{frontendKey},
		Measure:     videoSize,
		Aggregation: view.Distribution(0, 1<<16, 1<<32),
	}

	if err := view.Register(v); err != nil {
		t.Errorf("Cannot subscribe to the view: %v", err)
	}
	defer view.Unregister(v)

	for i := 0; i < 10; i++ {
		ctx, err := tag.New(ctx,
			tag.Insert(frontendKey, "mobile-ios9.3.5"),
		)
		if err != nil {
			t.Error(err)
		}

		stats.Record(ctx, videoSize.M(25648/int64(i+1)))
	}

	time.Sleep(1500 * time.Millisecond)
}

func TestExporter_ExportView_CountData(t *testing.T) {
	ctx := context.Background()

	view.SetReportingPeriod(time.Second)

	e := &Exporter{
		opts:   Options{},
		buffer: newBuffer(100),
		client: &dummyClient{
			WriteFunc: func(bp client.BatchPoints) error {
				if len(bp.Points()) != 1 {
					t.Errorf("unexpected number of points: %d", len(bp.Points()))
					return nil
				}
				for _, v := range bp.Points() {
					if name := v.Name(); name != "my.org/views/video_count" {
						t.Errorf("unexpected point name: %s", name)
						return nil
					}
					if len(v.Tags()) != 1 {
						t.Errorf("unexpected number of tags: %d", len(v.Tags()))
						return nil
					}
					if !reflect.DeepEqual(v.Tags(), map[string]string{
						"my.org/keys/frontend": "mobile-ios9.3.5",
					}) {
						t.Errorf("unexpected tag values: %v", v.Tags())
						return nil
					}
					fields, _ := v.Fields()
					if min := fields["count"].(int64); min != 10 {
						t.Errorf("unexpected field values: %v", fields)
						return nil
					}
				}
				return nil
			},
			PingFunc: func(timeout time.Duration) (time.Duration, string, error) {
				return 0, "", nil
			},
		},
	}

	view.RegisterExporter(e)
	defer view.UnregisterExporter(e)

	var err error
	frontendKey, err := tag.NewKey("my.org/keys/frontend")
	if err != nil {
		t.Error(err)
	}
	videoCounter := stats.Int64("my.org/measure/video_count", "count of processed videos", stats.UnitNone)

	v := &view.View{
		Name:        "my.org/views/video_count",
		Description: "count of processed videos",
		TagKeys:     []tag.Key{frontendKey},
		Measure:     videoCounter,
		Aggregation: view.Count(),
	}
	if err := view.Register(v); err != nil {
		t.Errorf("Cannot subscribe to the view: %v", err)
	}
	defer view.Unregister(v)

	for i := 0; i < 10; i++ {
		ctx, err := tag.New(ctx,
			tag.Insert(frontendKey, "mobile-ios9.3.5"),
		)
		if err != nil {
			t.Error(err)
		}

		stats.Record(ctx, videoCounter.M(1))
	}

	time.Sleep(1500 * time.Millisecond)
}

func TestExporter_ExportView_SumData(t *testing.T) {
	ctx := context.Background()

	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	view.SetReportingPeriod(time.Second)

	e := &Exporter{
		opts:   Options{},
		buffer: newBuffer(100),
		client: &dummyClient{
			WriteFunc: func(bp client.BatchPoints) error {
				if len(bp.Points()) != 1 {
					t.Errorf("unexpected number of points: %d", len(bp.Points()))
					return nil
				}
				for _, v := range bp.Points() {
					if name := v.Name(); name != "my.org/views/video_sum" {
						t.Errorf("unexpected point name: %s", name)
						return nil
					}
					if len(v.Tags()) != 1 {
						t.Errorf("unexpected number of tags: %d", len(v.Tags()))
						return nil
					}
					if !reflect.DeepEqual(v.Tags(), map[string]string{
						"my.org/keys/frontend": "mobile-ios9.3.5",
					}) {
						t.Errorf("unexpected tag values: %v", v.Tags())
						return nil
					}
					fields, _ := v.Fields()
					if min := fields["sum"].(float64); min != 55 {
						t.Errorf("unexpected field values: %v", fields)
						return nil
					}
				}
				return nil
			},
			PingFunc: func(timeout time.Duration) (time.Duration, string, error) {
				return 0, "", nil
			},
		},
	}

	view.RegisterExporter(e)
	defer view.UnregisterExporter(e)

	var err error
	frontendKey, err := tag.NewKey("my.org/keys/frontend")
	if err != nil {
		t.Error(err)
	}
	videoCounter := stats.Float64("my.org/measure/video_sum", "count of processed videos", stats.UnitNone)

	v := &view.View{
		Name:        "my.org/views/video_sum",
		Description: "count of processed videos",
		TagKeys:     []tag.Key{frontendKey},
		Measure:     videoCounter,
		Aggregation: view.Sum(),
	}
	if err := view.Register(v); err != nil {
		t.Errorf("Cannot subscribe to the view: %v", err)
	}
	defer view.Unregister(v)

	for i := 0; i < 10; i++ {
		ctx, err := tag.New(ctx,
			tag.Insert(frontendKey, "mobile-ios9.3.5"),
		)
		if err != nil {
			t.Error(err)
		}

		stats.Record(ctx, videoCounter.M(float64(i+1)))
	}

	time.Sleep(1500 * time.Millisecond)
}

func TestExporter_ExportView_LastValueData(t *testing.T) {
	ctx := context.Background()

	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	view.SetReportingPeriod(time.Second)

	e := &Exporter{
		opts:   Options{},
		buffer: newBuffer(100),
		client: &dummyClient{
			WriteFunc: func(bp client.BatchPoints) error {
				if len(bp.Points()) != 1 {
					t.Errorf("unexpected number of points: %d", len(bp.Points()))
					return nil
				}
				for _, v := range bp.Points() {
					if name := v.Name(); name != "my.org/views/video_gauge" {
						t.Errorf("unexpected point name: %s", name)
						return nil
					}
					if len(v.Tags()) != 1 {
						t.Errorf("unexpected number of tags: %d", len(v.Tags()))
						return nil
					}
					if !reflect.DeepEqual(v.Tags(), map[string]string{
						"my.org/keys/frontend": "mobile-ios9.3.5",
					}) {
						t.Errorf("unexpected tag values: %v", v.Tags())
						return nil
					}
					fields, _ := v.Fields()
					if min := fields["last"].(float64); min != 10 {
						t.Errorf("unexpected field values: %v", fields)
						return nil
					}
				}
				return nil
			},
			PingFunc: func(timeout time.Duration) (time.Duration, string, error) {
				return 0, "", nil
			},
		},
	}

	view.RegisterExporter(e)
	defer view.UnregisterExporter(e)

	var err error
	frontendKey, err := tag.NewKey("my.org/keys/frontend")
	if err != nil {
		t.Error(err)
	}
	videoCounter := stats.Float64("my.org/measure/video_gauge", "count of processed videos", stats.UnitNone)

	v := &view.View{
		Name:        "my.org/views/video_gauge",
		Description: "count of processed videos",
		TagKeys:     []tag.Key{frontendKey},
		Measure:     videoCounter,
		Aggregation: view.LastValue(),
	}
	if err := view.Register(v); err != nil {
		t.Errorf("Cannot subscribe to the view: %v", err)
	}
	defer view.Unregister(v)

	for i := 0; i < 10; i++ {
		ctx, err := tag.New(ctx,
			tag.Insert(frontendKey, "mobile-ios9.3.5"),
		)
		if err != nil {
			t.Error(err)
		}

		stats.Record(ctx, videoCounter.M(float64(i+1)))
	}

	time.Sleep(1500 * time.Millisecond)
}

func TestExporter_ExportView_noRows(t *testing.T) {
	e := &Exporter{}
	e.ExportView(&view.Data{Rows: []*view.Row{}})
}

func TestExporter_ExportView_clientError(t *testing.T) {
	var isErrFuncCalled bool
	expectedErr := errors.New("expect me")
	e := &Exporter{
		opts: Options{
			Database: "db",
		},
		buffer: newBuffer(100),
		client: &dummyClient{
			WriteFunc: func(bp client.BatchPoints) error {
				return expectedErr
			},
			PingFunc: func(timeout time.Duration) (time.Duration, string, error) {
				return 0, "", nil
			},
		},
		onError: func(err error) {
			isErrFuncCalled = true
			expErr, ok := err.(exportError)
			if !ok {
				t.Errorf("unexpected error type: %v", err)
				return
			}
			if expErr.err != expectedErr {
				t.Errorf("unexpected error %v", expErr.err)
			}
			if expErr.bp == nil {
				t.Errorf("unexpected error details %v", expErr.bp)
			}
		},
	}
	e.ExportView(&view.Data{
		Rows: []*view.Row{{
			Data: &view.CountData{Value: 123},
		}},
		View: &view.View{
			Name:        "my.org/views/video_gauge",
			Description: "count of processed videos",
			TagKeys:     []tag.Key{frontendKey},
			Measure:     stats.Float64("my.org/measure/video_gauge", "count of processed videos", stats.UnitNone),
			Aggregation: view.LastValue(),
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	e.flushBuffer(ctx, time.NewTicker(time.Millisecond))
	if !isErrFuncCalled {
		t.Error("error func not called")
	}
}

func TestExporter_new(t *testing.T) {
	var isErrFuncCalled bool
	expectedErr := errors.New("expect me")
	e, err := NewExporter(context.Background(), Options{
		Database: "db",
		Address:  "http://example.tld",
		OnError: func(err error) {
			isErrFuncCalled = true
			expErr, ok := err.(exportError)
			if !ok {
				t.Errorf("unexpected error type: %v", err)
				return
			}
			if expErr.err != expectedErr {
				t.Errorf("unexpected error %v", expErr.err)
			}
			if expErr.bp == nil {
				t.Errorf("unexpected error details %v", expErr.bp)
			}
		},
	})
	if err != nil {
		t.Errorf("unexpected error: %s", err.Error())
		return
	}
	e.client = &dummyClient{
		WriteFunc: func(bp client.BatchPoints) error {
			return expectedErr
		},
		PingFunc: func(timeout time.Duration) (time.Duration, string, error) {
			return 0, "", nil
		},
	}

	e.ExportView(&view.Data{
		Rows: []*view.Row{{
			Data: &view.CountData{Value: 123},
		}},
		View: &view.View{
			Name:        "my.org/views/video_gauge",
			Description: "count of processed videos",
			TagKeys:     []tag.Key{frontendKey},
			Measure:     stats.Float64("my.org/measure/video_gauge", "count of processed videos", stats.UnitNone),
			Aggregation: view.LastValue(),
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	e.flushBuffer(ctx, time.NewTicker(time.Millisecond))
	if !isErrFuncCalled {
		t.Error("error func not called")
	}
}

func TestExporter_new_defaultOnError(t *testing.T) {
	expectedErr := errors.New("expect me")
	buf := new(bytes.Buffer)
	log.SetOutput(buf)
	e, err := NewExporter(context.Background(), Options{
		Database: "db",
		Address:  "http://example.tld",
	})
	if err != nil {
		t.Errorf("unexpected error: %s", err.Error())
		return
	}
	e.client = &dummyClient{
		WriteFunc: func(bp client.BatchPoints) error {
			return expectedErr
		},
		PingFunc: func(timeout time.Duration) (time.Duration, string, error) {
			return 0, "", nil
		},
	}

	e.ExportView(&view.Data{
		Rows: []*view.Row{{
			Data: &view.CountData{Value: 123},
		}},
		View: &view.View{
			Name:        "my.org/views/video_gauge",
			Description: "count of processed videos",
			TagKeys:     []tag.Key{frontendKey},
			Measure:     stats.Float64("my.org/measure/video_gauge", "count of processed videos", stats.UnitNone),
			Aggregation: view.LastValue(),
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	e.flushBuffer(ctx, time.NewTicker(time.Millisecond))
	if logContent := buf.String(); logContent == "" {
		t.Error("error func not called")
	}
}

func TestExporter_new_wrongClient(t *testing.T) {
	_, err := NewExporter(context.Background(), Options{
		Database: "db",
	})
	if err == nil {
		t.Errorf("expecteing error")
	}
}

type dummyClient struct {
	PingFunc  func(timeout time.Duration) (time.Duration, string, error)
	WriteFunc func(bp client.BatchPoints) error
}

func (d *dummyClient) Ping(timeout time.Duration) (time.Duration, string, error) {
	return d.PingFunc(timeout)
}

func (d *dummyClient) Write(bp client.BatchPoints) error {
	return d.WriteFunc(bp)
}
