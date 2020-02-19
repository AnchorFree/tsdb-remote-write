package app

import (
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/tsdb"
	tsdb_labels "github.com/prometheus/tsdb/labels"
)

func buildWriteRequest(samples []prompb.TimeSeries, buf []byte) ([]byte, error) {
	req := &prompb.WriteRequest{
		Timeseries: samples,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	// snappy uses len() to see if it needs to allocate a new slice. Make the
	// buffer as long as possible.
	if buf != nil {
		buf = buf[0:cap(buf)]
	}
	compressed := snappy.Encode(buf, data)
	return compressed, nil
}

func extractLabels(series tsdb.Series) []prompb.Label {
	labels := make([]prompb.Label, 0)
	for _, label := range series.Labels() {
		labels = append(labels, prompb.Label{Name:  label.Name, Value: label.Value})
	}
	return labels
}

func extractSeries(series tsdb.Series) []prompb.Sample {
	iterator := series.Iterator()
	samples := make([]prompb.Sample, 0)
	for iterator.Next() {
		ts, value := iterator.At()
		samples = append(samples, prompb.Sample{Value: value, Timestamp: ts})
	}
	return samples
}

func getSeriesSet(blockReader tsdb.BlockReader, mint, maxt int64) (tsdb.SeriesSet, error) {
	querier, err := tsdb.NewBlockQuerier(blockReader, mint, maxt)
	if err != nil {
		return nil, err
	}
	seriesSet, err := querier.Select(tsdb_labels.NewMustRegexpMatcher("", ".*"))
	if err != nil {
		return nil, err
	}
	return seriesSet, nil
}

