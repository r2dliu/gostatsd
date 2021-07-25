package sqlserver

import (
	"math"
	"testing"
	"time"

	"github.com/atlassian/gostatsd"
)

func TestPreparePayloadLegacy(t *testing.T) {
}

func TestPreparePayloadBasic(t *testing.T) {
}

func TestPreparePayloadTags(t *testing.T) {
}

func TestPreparePayloadHistogram(t *testing.T) {
}

func sortLines(s string) string {
}

func TestSendMetricsAsync(t *testing.T) {
}

func metrics() *gostatsd.MetricMap {
	timestamp := gostatsd.Nanotime(time.Unix(123456, 0).UnixNano())

	mm := gostatsd.NewMetricMap()
	mm.Counters["stat1"] = map[string]gostatsd.Counter{}
	mm.Counters["stat1"][""] = gostatsd.Counter{PerSecond: 1.1, Value: 5, Timestamp: timestamp}
	mm.Timers["t1"] = map[string]gostatsd.Timer{}
	mm.Timers["t1"][""] = gostatsd.Timer{Values: []float64{10}, Percentiles: gostatsd.Percentiles{gostatsd.Percentile{Float: 90, Str: "count_90"}}, Timestamp: timestamp}
	mm.Gauges["g1"] = map[string]gostatsd.Gauge{}
	mm.Gauges["g1"][""] = gostatsd.Gauge{Value: 3, Timestamp: timestamp}
	mm.Sets["users"] = map[string]gostatsd.Set{}
	mm.Sets["users"][""] = gostatsd.Set{Values: map[string]struct{}{"joe": {}, "bob": {}, "john": {}}, Timestamp: timestamp}
	return mm
}

// twoCounters returns two counters.
func metricsTwoCounters() *gostatsd.MetricMap {
	return &gostatsd.MetricMap{
		Counters: gostatsd.Counters{
			"stat1": map[string]gostatsd.Counter{
				"tag1": gostatsd.NewCounter(gostatsd.Nanotime(time.Now().UnixNano()), 5, "", nil),
			},
			"stat2": map[string]gostatsd.Counter{
				"tag2": gostatsd.NewCounter(gostatsd.Nanotime(time.Now().UnixNano()), 50, "", nil),
			},
		},
	}
}

// nolint:dupl
func metricsOneOfEach() *gostatsd.MetricMap {
	return &gostatsd.MetricMap{
		Counters: gostatsd.Counters{
			"c1": map[string]gostatsd.Counter{
				"tag1": {PerSecond: 1.1, Value: 5, Timestamp: gostatsd.Nanotime(100), Source: "h1", Tags: gostatsd.Tags{"tag1"}},
			},
		},
		Timers: gostatsd.Timers{
			"t1": map[string]gostatsd.Timer{
				"tag2": {
					Count:      1,
					PerSecond:  1.1,
					Mean:       0.5,
					Median:     0.5,
					Min:        0,
					Max:        1,
					StdDev:     0.1,
					Sum:        1,
					SumSquares: 1,
					Values:     []float64{0, 1},
					Percentiles: gostatsd.Percentiles{
						gostatsd.Percentile{Float: 0.1, Str: "count_90"},
					},
					Timestamp: gostatsd.Nanotime(200),
					Source:    "h2",
					Tags:      gostatsd.Tags{"tag2"},
				},
			},
		},
		Gauges: gostatsd.Gauges{
			"g1": map[string]gostatsd.Gauge{
				"tag3": {Value: 3, Timestamp: gostatsd.Nanotime(300), Source: "h3", Tags: gostatsd.Tags{"tag3"}},
			},
		},
		Sets: gostatsd.Sets{
			"users": map[string]gostatsd.Set{
				"tag4": {
					Values: map[string]struct{}{
						"joe":  {},
						"bob":  {},
						"john": {},
					},
					Timestamp: gostatsd.Nanotime(400),
					Source:    "h4",
					Tags:      gostatsd.Tags{"tag4"},
				},
			},
		},
	}
}

func metricsWithHistogram() *gostatsd.MetricMap {
	timestamp := gostatsd.Nanotime(time.Unix(123456, 0).UnixNano())

	mm := gostatsd.NewMetricMap()
	mm.Timers["t1"] = map[string]gostatsd.Timer{}
	mm.Timers["t1"]["gsd_histogram:20_30_40_50_60"] = gostatsd.Timer{Values: []float64{10}, Timestamp: timestamp, Histogram: map[gostatsd.HistogramThreshold]int{
		20:                                       5,
		30:                                       10,
		40:                                       10,
		50:                                       10,
		60:                                       19,
		gostatsd.HistogramThreshold(math.Inf(1)): 19,
	}}
	return mm
}

func metricsWithTags() *gostatsd.MetricMap {
	timestamp := gostatsd.Nanotime(time.Unix(123456, 0).UnixNano())

	m := metrics()
	m.Counters["stat1"]["t"] = gostatsd.Counter{PerSecond: 2.2, Value: 10, Timestamp: timestamp, Tags: gostatsd.Tags{"t"}}
	m.Counters["stat1"]["k:v"] = gostatsd.Counter{PerSecond: 3.3, Value: 15, Timestamp: timestamp, Tags: gostatsd.Tags{"k:v"}}
	m.Counters["stat1"]["k:v.t"] = gostatsd.Counter{PerSecond: 4.4, Value: 20, Timestamp: timestamp, Tags: gostatsd.Tags{"k:v", "t"}}
	return m
}
