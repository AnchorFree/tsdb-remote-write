package app

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/url"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/AnchorFree/tsdb-remote-write/internal/utils"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunkenc"
	tsdb_labels "github.com/prometheus/tsdb/labels"
)

type AppConfig struct {
	TsdbDir                string
	RemoteURL              *url.URL
	RemoteTimeout          time.Duration
	Concurrency            int
	QueueCapacity          int
	QueueMaxSamplesPerSend int
	QueueMaxRetries        int
	Backward               bool
}

type app struct {
	config        AppConfig
	remoteStorage []*remote.Storage
	logger        log.Logger
	pool          *chunkenc.Pool
	blockMetaCh   chan tsdb.BlockMeta
	wg            sync.WaitGroup
	sampleCh      chan model.Sample
	clients       []*remote.Client
}

func NewApp(c *AppConfig, l log.Logger) (*app, error) {
	if l == nil {
		l = log.NewNopLogger()
	}

	pool := chunkenc.NewPool()

	clients := make([]*remote.Client, c.Concurrency)
	for i := range clients {
		clients[i], _ = remote.NewClient(i, &remote.ClientConfig{
			URL:     &config_util.URL{URL: c.RemoteURL},
			Timeout: model.Duration(c.RemoteTimeout),
		})
	}

	return &app{
		config:      *c,
		logger:      l,
		pool:        &pool,
		blockMetaCh: make(chan tsdb.BlockMeta),
		wg:          *new(sync.WaitGroup),
		sampleCh:    make(chan model.Sample, c.QueueCapacity*c.Concurrency),
		clients:     clients,
	}, nil
}

func (app *app) Run() error {
	if err := app.collectBlockMeta(); err != nil {
		return err
	}
	app.startWriters()
	app.blockReader()
	app.wg.Wait()

	return nil
}

func (app *app) Close() {
	return
}

func (app *app) collectBlockMeta() error {
	files, err := ioutil.ReadDir(app.config.TsdbDir)
	if err != nil {
		return err
	}

	var blocksMeta []tsdb.BlockMeta
	for _, fi := range files {
		if !fi.IsDir() {
			continue
		}

		if _, err := ulid.Parse(fi.Name()); err != nil {
			continue
		}

		blockPath := filepath.Join(app.config.TsdbDir, fi.Name())
		b, err := ioutil.ReadFile(filepath.Join(blockPath, "meta.json"))
		if err != nil {
			continue
		}

		var m tsdb.BlockMeta
		if err := json.Unmarshal(b, &m); err != nil {
			continue
		}
		if m.Version != 1 {
			continue
		}

		blocksMeta = append(blocksMeta, m)
	}

	sort.Slice(blocksMeta, func(i, j int) bool {
		if !app.config.Backward {
			return blocksMeta[i].MinTime < blocksMeta[j].MinTime
		}
		return blocksMeta[i].MinTime > blocksMeta[j].MinTime
	})

	level.Info(app.logger).Log("msg", "loaded blocks meta", "blocks", len(blocksMeta))
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		for _, blockMeta := range blocksMeta {
			app.blockMetaCh <- blockMeta
		}
		close(app.blockMetaCh)
	}()
	level.Info(app.logger).Log("msg", "started blocks meta sender", "blocks", len(blocksMeta))

	return nil
}

func (app *app) blockReader() {
	logger := log.With(app.logger, "component", "block_reader")
	level.Info(app.logger).Log("msg", "starting blocks reader")
	for blockMeta := range app.blockMetaCh {
		logger := log.With(logger, "ulid", blockMeta.ULID, "mint", blockMeta.MinTime, "maxt", blockMeta.MaxTime)
		level.Info(logger).Log("msg", "opening block")
		blockPath := filepath.Join(app.config.TsdbDir, blockMeta.ULID.String())
		block, err := tsdb.OpenBlock(logger, blockPath, *app.pool)
		if err != nil {
			level.Warn(logger).Log("msg", "error open block", "error", err)
			continue
		}
		level.Info(logger).Log("msg", "block opened, start sending")
		trIter := utils.NewTimeRangeIter(block.Meta().MinTime, block.Meta().MaxTime, 3600000, app.config.Backward)
		for trIter.Next() {
			tr := trIter.At()
			level.Info(logger).Log("msg", "start sending hour", "from", tr.Start, "to", tr.End)
			seriesSet, err := getSeriesSet(block, tr.Start, tr.End)
			if err != nil {
				level.Warn(logger).Log("msg", "can't get series set for block", "error", err)
				continue
			}
			for seriesSet.Next() {
				series := seriesSet.At()
				metric := labelsToMetric(series.Labels())
				iterator := series.Iterator()
				for iterator.Next() {
					ts, value := iterator.At()
					app.sampleCh <- model.Sample{
						Metric:    metric,
						Timestamp: model.Time(ts),
						Value:     model.SampleValue(value),
					}
				}
			}
			level.Info(logger).Log("msg", "done sending hour")
		}
		level.Info(logger).Log("msg", "done sending block")
	}
	close(app.sampleCh)
}

func (app *app) startWriters() {
	level.Info(app.logger).Log("msg", "starting remote writers")
	for i := 0; i < app.config.Concurrency; i++ {
		app.wg.Add(1)
		go app.remoteWriter(i)
	}
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

func (app *app) remoteWriter(serial int) {
	defer app.wg.Done()
	logger := log.With(app.logger, "component", "remote_writer", "remote_writer", serial)
	level.Info(logger).Log("msg", "start storage writer")
	pendingSamples := model.Samples{}

	for {
		select {
		case sample, ok := <-app.sampleCh:
			if !ok {
				level.Info(logger).Log("msg", "error reading from channel")
				if len(pendingSamples) > 0 {
					level.Info(logger).Log("msg", "flushing pending samples")
					app.sendSamples(pendingSamples, serial)
				}
				return
			}

			pendingSamples = append(pendingSamples, &sample)
			if len(pendingSamples) >= app.config.QueueMaxSamplesPerSend {
				app.sendSamples(pendingSamples[:app.config.QueueMaxSamplesPerSend], serial)
				pendingSamples = pendingSamples[app.config.QueueMaxSamplesPerSend:]
			}
		}
	}
}

func labelsToMetric(ls tsdb_labels.Labels) model.Metric {
	metric := make(model.Metric, len(ls))
	for _, l := range ls {
		metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	}
	return metric
}

func (app *app) sendSamples(samples model.Samples, serial int) {
	req := remote.ToWriteRequest(samples)
	for retries := app.config.QueueMaxRetries; retries > 0; retries-- {
		err := app.clients[serial].Store(context.TODO(), req)
		if err == nil {
			break
		}
		level.Info(app.logger).Log("msg", "failed to send metrics to remote", "client_id", serial, "error", err)
	}
}
