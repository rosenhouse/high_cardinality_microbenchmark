package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	_ "net/http/pprof" // pprof: for debug listen server if configured
	"os"
	"path/filepath"
	"time"

	"github.com/chronosphereiox/high_cardinality_microbenchmark/pkg/generator"

	"github.com/m3db/m3/src/x/instrument"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"go.uber.org/zap"
)

var (
	blockSize = 2 * time.Hour
)

func timeToPromTime(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

func main() {
	var (
		flagCardinality = flag.Int("cardinality", 5000000, "cardinality to generate")
		flagDir         = flag.String("dir", "/tmp", "directory for output")
	)

	flag.Parse()

	logger := instrument.NewOptions().Logger()

	if *flagCardinality <= 0 || *flagDir == "" {
		flag.Usage()
		os.Exit(1)
		return
	}

	var (
		cardinality = *flagCardinality
		dir         = *flagDir
		samples     []*tsdb.MetricSample
	)
	srv := httptest.NewServer(http.DefaultServeMux)
	logger.Info("test server with pprof", zap.String("url", srv.URL))

	start := time.Now().Truncate(blockSize).Add(-1 * blockSize)
	timeNowFn := func() time.Time { return start }

	podNameRng := rand.New(rand.NewSource(0)) // deterministic
	podNameFn := func() string {
		return fmt.Sprintf("%x-%x", podNameRng.Uint64(), podNameRng.Uint64())
	}

	gen := generator.NewHostsSimulator(10000, start,
		generator.HostsSimulatorOptions{TimeNowFn: timeNowFn})

	genStartTime := time.Now()
	logger.Info("starting generate loop")
TopLoop:
	for {
		ts, err := gen.Generate(10*time.Second, 10*time.Second, 1.0)
		if err != nil {
			logger.Fatal("unable to generate series", zap.Error(err))
		}
		for _, results := range ts {
			for _, series := range results {
				sampleLabels := make([]labels.Label, 0, len(series.Labels))
				for _, label := range series.Labels {
					sampleLabels = append(sampleLabels, labels.Label{
						Name:  label.Name,
						Value: label.Value,
					})
				}
				sampleLabels = append(sampleLabels, labels.Label{
					Name:  "pod",
					Value: podNameFn(),
				})

				if len(series.Samples) != 1 {
					logger.Fatal("expected single sample",
						zap.Int("samples", len(series.Samples)))
				}
				for _, value := range series.Samples {
					sample := &tsdb.MetricSample{
						TimestampMs: value.Timestamp,
						Value:       value.Value,
						Labels:      sampleLabels,
					}
					samples = append(samples, sample)
					if len(samples) >= cardinality {
						break TopLoop
					}
				}
			}
		}
	}

	// Determine end
	end := start
	for i := range samples {
		t := time.Unix(0, samples[i].TimestampMs*int64(time.Millisecond))
		if t.After(end) {
			end = t
		}
	}

	hardEnd := start.Add(blockSize)
	if end.After(hardEnd) {
		logger.Fatal("too many samples for block",
			zap.Stringer("start", start),
			zap.Stringer("hardEnd", hardEnd),
			zap.Stringer("actualEnd", end))
	}

	logger.Info("writing block", zap.Int("samples", len(samples)), zap.Duration("gen-time", time.Since(genStartTime)))
	blockWriteStartTime := time.Now()

	if err := os.MkdirAll(dir, 0777); err != nil {
		logger.Fatal("could not create dir", zap.String("dir", dir), zap.Error(err))
	}
	outFile, err := os.Create(filepath.Join(dir, "samples.json"))
	if err != nil {
		logger.Fatal("could not create output file", zap.Error(err))
	}
	encoder := json.NewEncoder(outFile)
	for _, s := range samples {
		if err = encoder.Encode(s); err != nil {
			logger.Fatal("could not encode sample to json", zap.Error(err))
		}
	}

	logger.Info("created flat file", zap.Duration("write-time", time.Since(blockWriteStartTime)))
}