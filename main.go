package main

import (
	"log"
	"net/http"
	"os"

	_ "net/http/pprof"

	"github.com/AnchorFree/tsdb-remote-write/internal/app"
	kitlog "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	var (
		pprofAddr string
	)

	c := app.AppConfig{}
	cli := kingpin.New("tsdb-remote-write", "Write to Prometheus remote storage directly from Prometheus TSDB")
	cli.Flag("tsdb-dir", "TSDB Directory").Required().ExistingDirVar(&c.TsdbDir)
	cli.Flag("remote-url", "Remote Write Storage URL").Required().URLVar(&c.RemoteURL)
	cli.Flag("remote-timeout", "Remote timeout").Default("30s").DurationVar(&c.RemoteTimeout)
	cli.Flag("concurrency", "Number of remote writers").Default("4").IntVar(&c.Concurrency)
	cli.Flag("queue-capacity", "Queue capacity").Default("1000").IntVar(&c.QueueCapacity)
	cli.Flag("queue-max-samples-per-send", "Queue max samples per send").Default("100").IntVar(&c.QueueMaxSamplesPerSend)
	cli.Flag("queue-max-retries", "Queue max retries").Default("3").IntVar(&c.QueueMaxRetries)
	cli.Flag("min-time", "Minimum time limit in milliseconds to send to remote storage").Default("0").Int64Var(&c.MinTime)
	cli.Flag("max-time", "Maximum time limit in milliseconds to send to remote storage").Default("0").Int64Var(&c.MaxTime)
	cli.Flag("backward", "Walk trough tsdb in backward order").Default("false").BoolVar(&c.Backward)
	cli.Flag("pprof-addr", "Listen address for pprof endpoint").Default("localhost:8081").StringVar(&pprofAddr)

	kingpin.MustParse(cli.Parse(os.Args[1:]))

	logger := kitlog.With(kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stdout)),
		"cli", "tsdb-remote-write", "ts", kitlog.DefaultTimestampUTC)
	logger = level.NewFilter(logger, level.AllowInfo())
	log.SetOutput(kitlog.NewStdlibAdapter(logger))

	tsdbrwApp, err := app.NewApp(&c, logger)
	if err != nil {
		log.Panic(err)
	}
	defer tsdbrwApp.Close()

	pprofMux := http.DefaultServeMux
	http.DefaultServeMux = http.NewServeMux()
	httpServer := &http.Server{
		Addr:    pprofAddr,
		Handler: pprofMux,
	}
	go func() {
		log.Fatal(httpServer.ListenAndServe())
	}()

	log.Fatal(tsdbrwApp.Run())
}
