// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package puller

import (
	"context"
	"math"
	"os"
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	_ "net/http/pprof"
)

const (
	numProducers          = 16
	eventCountPerProducer = 10000
)

type sorterSuite struct{}

var _ = check.Suite(&sorterSuite{})

func generateMockRawKV(ts uint64) *model.RawKVEntry {
	return &model.RawKVEntry{
		OpType:   model.OpTypePut,
		Key:      []byte{},
		Value:    []byte{},
		OldValue: nil,
		StartTs:  ts - 5,
		CRTs:     ts,
		RegionID: 0,
	}
}

func (s *sorterSuite) TestSorterBasic(c *check.C) {
	config.SetSorterConfig(&config.SorterConfig{
		NumConcurrentWorker:  8,
		ChunkSizeLimit:       1 * 1024 * 1024 * 1024,
		MaxMemoryPressure:    60,
		MaxMemoryConsumption: 16 * 1024 * 1024 * 1024,
	})

	err := os.MkdirAll("./sorter", 0755)
	c.Assert(err, check.IsNil)
	sorter := NewUnifiedSorter("./sorter")
	testSorter(c, sorter)
}

func testSorter(c *check.C, sorter EventSorter) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)

	errg, ctx := errgroup.WithContext(timeoutCtx)
	errg.Go(func() error {
		return sorter.Run(ctx)
	})

	producerProgress := make([]uint64, numProducers)

	// launch the producers
	for i := 0; i < numProducers; i++ {
		finalI := i
		errg.Go(func() error {
			for j := 0; j < eventCountPerProducer; j++ {
				sorter.AddEntry(ctx, model.NewPolymorphicEvent(generateMockRawKV(uint64(j)<<5)))
				if j%10000 == 0 {
					atomic.StoreUint64(&producerProgress[finalI], uint64(j)<<5)
				}
			}
			sorter.AddEntry(ctx, model.NewPolymorphicEvent(generateMockRawKV(uint64(eventCountPerProducer)<<5)))
			atomic.StoreUint64(&producerProgress[finalI], uint64(eventCountPerProducer)<<5)
			return nil
		})
	}

	// launch the resolver
	errg.Go(func() error {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				resolvedTs := uint64(math.MaxUint64)
				for i := range producerProgress {
					ts := atomic.LoadUint64(&producerProgress[i])
					if resolvedTs > ts {
						resolvedTs = ts
					}
				}
				sorter.AddEntry(ctx, model.NewResolvedPolymorphicEvent(0, resolvedTs))
				if resolvedTs == uint64(eventCountPerProducer)<<5 {
					return nil
				}
			}
		}
	})

	// launch the consumer
	errg.Go(func() error {
		counter := 0
		lastTs := uint64(0)
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case event := <-sorter.Output():
				if event.RawKV.OpType != model.OpTypeResolved {
					if event.CRTs < lastTs {
						panic("regressed")
					}
					lastTs = event.CRTs
					counter += 1
					if counter%10000 == 0 {
						log.Debug("Messages received", zap.Int("counter", counter))
					}
					if counter >= numProducers*eventCountPerProducer {
						log.Debug("Unified Sorter test successful")
						cancel()
					}
				}
			case <-ticker.C:
				log.Debug("Consumer is alive")
			}
		}
	})

	err := errg.Wait()
	if err == errors.Cause(err) {
		return
	}
	c.Assert(err, check.IsNil)
}