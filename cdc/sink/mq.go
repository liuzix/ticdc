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

package sink

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/codec"
	"github.com/pingcap/ticdc/cdc/sink/dispatcher"
	"github.com/pingcap/ticdc/cdc/sink/mqProducer"
	"github.com/pingcap/ticdc/cdc/sink/pulsar"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/notify"
	"github.com/pingcap/ticdc/pkg/security"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type mqSink struct {
	mqProducers   map[string]mqProducer.Producer
	mqProducerMu  sync.Mutex
	newMQProducer func(topic string) (mqProducer.Producer, error)
	dispatcher    dispatcher.Dispatcher
	newEncoder    func() codec.EventBatchEncoder
	filter        *filter.Filter
	protocol      codec.Protocol

	partitionNum   int32
	partitionInput []chan struct {
		row        *model.RowChangedEvent
		resolvedTs uint64
	}
	partitionResolvedTs []uint64
	checkpointTs        uint64
	resolvedNotifier    *notify.Notifier
	resolvedReceiver    *notify.Receiver
	topicFmt            string
	statistics *Statistics
}

func newMqSink(
	ctx context.Context, credential *security.Credential, newMQProducer func(topic string) (mqProducer.Producer, error), partitionNum int32,
	filter *filter.Filter, config *config.ReplicaConfig, opts map[string]string, errCh chan error, topicFmt string,
) (*mqSink, error) {
	partitionInput := make([]chan struct {
		row        *model.RowChangedEvent
		resolvedTs uint64
	}, partitionNum)
	for i := 0; i < int(partitionNum); i++ {
		partitionInput[i] = make(chan struct {
			row        *model.RowChangedEvent
			resolvedTs uint64
		}, 12800)
	}
	d, err := dispatcher.NewDispatcher(config, partitionNum)
	if err != nil {
		return nil, errors.Trace(err)
	}
	notifier := new(notify.Notifier)
	var protocol codec.Protocol
	protocol.FromString(config.Sink.Protocol)

	newEncoder := codec.NewEventBatchEncoder(protocol)
	if protocol == codec.ProtocolAvro {
		registryURI, ok := opts["registry"]
		if !ok {
			return nil, errors.New(`Avro protocol requires parameter "registry"`)
		}
		keySchemaManager, err := codec.NewAvroSchemaManager(ctx, credential, registryURI, "-key")
		if err != nil {
			return nil, errors.Annotate(err, "Could not create Avro schema manager for message keys")
		}
		valueSchemaManager, err := codec.NewAvroSchemaManager(ctx, credential, registryURI, "-value")
		if err != nil {
			return nil, errors.Annotate(err, "Could not create Avro schema manager for message values")
		}
		newEncoder1 := newEncoder
		newEncoder = func() codec.EventBatchEncoder {
			avroEncoder := newEncoder1().(*codec.AvroEventBatchEncoder)
			avroEncoder.SetKeySchemaManager(keySchemaManager)
			avroEncoder.SetValueSchemaManager(valueSchemaManager)
			return avroEncoder
		}
	}

	k := &mqSink{
		mqProducers:   make(map[string]mqProducer.Producer),
		newMQProducer: newMQProducer,
		dispatcher:    d,
		newEncoder:    newEncoder,
		filter:        filter,
		protocol:      protocol,

		partitionNum:        partitionNum,
		partitionInput:      partitionInput,
		partitionResolvedTs: make([]uint64, partitionNum),
		resolvedNotifier:    notifier,
		resolvedReceiver:    notifier.NewReceiver(50 * time.Millisecond),

		statistics: NewStatistics(ctx, "MQ", opts),
	}

	go func() {
		if err := k.run(ctx); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
			}
		}
	}()
	return k, nil
}

func (k *mqSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	rowsCount := 0
	for _, row := range rows {
		if k.filter.ShouldIgnoreDMLEvent(row.StartTs, row.Table.Schema, row.Table.Table) {
			log.Info("Row changed event ignored", zap.Uint64("start-ts", row.StartTs))
			continue
		}
		partition := k.dispatcher.Dispatch(row)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case k.partitionInput[partition] <- struct {
			row        *model.RowChangedEvent
			resolvedTs uint64
		}{row: row}:
		}
		rowsCount++
	}
	k.statistics.AddRowsCount(rowsCount)
	return nil
}

func (k *mqSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) error {
	if resolvedTs <= k.checkpointTs {
		return nil
	}

	for i := 0; i < int(k.partitionNum); i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case k.partitionInput[i] <- struct {
			row        *model.RowChangedEvent
			resolvedTs uint64
		}{resolvedTs: resolvedTs}:
		}
	}

	// waiting for all row events are sent to mq producer
flushLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-k.resolvedReceiver.C:
			for i := 0; i < int(k.partitionNum); i++ {
				if resolvedTs > atomic.LoadUint64(&k.partitionResolvedTs[i]) {
					continue flushLoop
				}
			}
			break flushLoop
		}
	}
	for _, producer := range k.mqProducers {
		err := producer.Flush(ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}
	k.checkpointTs = resolvedTs
	k.statistics.PrintStatus()
	return nil
}

func (k *mqSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	encoder := k.newEncoder()
	op, err := encoder.AppendResolvedEvent(ts)
	if err != nil {
		return errors.Trace(err)
	}
	if op == codec.EncoderNoOperation {
		return nil
	}
	key, value := encoder.Build()

	k.mqProducerMu.Lock()
	topics := make([]string, 0, len(k.mqProducers))
	for topic := range k.mqProducers {
		topics = append(topics, topic)
	}
	k.mqProducerMu.Unlock()

	for _, topic := range topics {
		err = k.writeToProducer(ctx, key, value, op, -1, topic)
		if err != nil {
			return errors.Trace(err)
		}
		return errors.Trace(err)
	}

	return nil
}

func (k *mqSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	if k.filter.ShouldIgnoreDDLEvent(ddl.StartTs, ddl.Type, ddl.TableInfo.Schema, ddl.TableInfo.Table) {
		log.Info(
			"DDL event ignored",
			zap.String("query", ddl.Query),
			zap.Uint64("startTs", ddl.StartTs),
			zap.Uint64("commitTs", ddl.CommitTs),
		)
		return errors.Trace(model.ErrorDDLEventIgnored)
	}
	encoder := k.newEncoder()
	op, err := encoder.AppendDDLEvent(ddl)
	if err != nil {
		return errors.Trace(err)
	}

	if op == codec.EncoderNoOperation {
		return nil
	}

	key, value := encoder.Build()
	log.Info("emit ddl event", zap.ByteString("key", key), zap.ByteString("value", value))
	err = k.writeToProducer(ctx, key, value, op, -1, k.dispatchTopic(ddl.TableInfo.Schema, ddl.TableInfo.Table))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Initialize registers Avro schemas for all tables
func (k *mqSink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	// No longer need it for now
	return nil
}

func (k *mqSink) Close() error {
	for _, producer := range k.mqProducers {
		err := producer.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (k *mqSink) run(ctx context.Context) error {
	defer k.resolvedReceiver.Stop()
	wg, ctx := errgroup.WithContext(ctx)
	for i := int32(0); i < k.partitionNum; i++ {
		partition := i
		wg.Go(func() error {
			return k.runWorker(ctx, partition)
		})
	}
	return wg.Wait()
}

const batchSizeLimit = 4 * 1024 * 1024 // 4MB

func (k *mqSink) runWorker(ctx context.Context, partition int32) error {
	input := k.partitionInput[partition]
	encoder := k.newEncoder()
	batchSize := 0
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	flushToProducer := func(op codec.EncoderResult) error {
		return k.statistics.RecordBatchExecution(func() (int, error) {
			if batchSize == 0 {
				return 0, nil
			}
			key, value := encoder.Build()
			encoder = k.newEncoder()
			thisBatchSize := batchSize
			batchSize = 0
			return thisBatchSize, k.writeToProducer(ctx, key, value, op, partition)
		})
	}
	for {
		var e struct {
			row        *model.RowChangedEvent
			resolvedTs uint64
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			if err := flushToProducer(codec.EncoderNeedAsyncWrite); err != nil {
				return errors.Trace(err)
			}
			continue
		case e = <-input:
		}
		if e.row == nil {
			if e.resolvedTs != 0 {
				if err := flushToProducer(codec.EncoderNeedAsyncWrite); err != nil {
					return errors.Trace(err)
				}
				atomic.StoreUint64(&k.partitionResolvedTs[partition], e.resolvedTs)
				k.resolvedNotifier.Notify()
			}
			continue
		}
		op, err := encoder.AppendRowChangedEvent(e.row)
		if err != nil {
			return errors.Trace(err)
		}
		batchSize++

		if encoder.Size() >= batchSizeLimit {
			if err := flushToProducer(codec.EncoderNeedAsyncWrite); err != nil {
				return errors.Trace(err)
			}
			continue
		}

		if op == codec.EncoderNeedSyncWrite || op == codec.EncoderNeedAsyncWrite {
			if err := flushToProducer(op); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (k *mqSink) writeToProducer(ctx context.Context, key []byte, value []byte, op codec.EncoderResult, partition int32, topic string) error {
	k.mqProducerMu.Lock()
	p, ok := k.mqProducers[topic]
	if !ok {
		log.Info("MQSink: producer not found for topic, will create one.", zap.String("topic", topic))
		newProducer, err := k.newMQProducer(topic)
		if err != nil {
			k.mqProducerMu.Unlock()
			return errors.Trace(err)
		}
		k.mqProducers[topic] = newProducer
		p = newProducer
	}
	k.mqProducerMu.Unlock()

	switch op {
	case codec.EncoderNeedAsyncWrite:
		if partition >= 0 {
			return p.SendMessage(ctx, key, value, partition)
		}
		return errors.New("Async broadcasts not supported")
	case codec.EncoderNeedSyncWrite:
		if partition >= 0 {
			err := p.SendMessage(ctx, key, value, partition)
			if err != nil {
				return err
			}
			return p.Flush(ctx)
		}
		return p.SyncBroadcastMessage(ctx, key, value)
	}

	log.Warn("writeToProducer called with no-op",
		zap.ByteString("key", key),
		zap.ByteString("value", value),
		zap.Int32("partition", partition))
	return nil
}



func (k *mqSink) dispatchTopic(schema string, table string) string {
	ret := strings.ReplaceAll(k.topicFmt, "${db}", schema)
	ret = strings.ReplaceAll(ret, "${table}", table)
	return ret
}

func newKafkaSaramaSink(ctx context.Context, sinkURI *url.URL, filter *filter.Filter, replicaConfig *config.ReplicaConfig, opts map[string]string, errCh chan error) (*mqSink, error) {
	config := mqProducer.NewKafkaConfig()

	scheme := strings.ToLower(sinkURI.Scheme)
	if scheme != "kafka" {
		return nil, errors.New("can not create MQ sink with unsupported scheme")
	}
	s := sinkURI.Query().Get("partition-num")
	if s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			return nil, errors.Trace(err)
		}
		config.PartitionNum = int32(c)
	}

	s = sinkURI.Query().Get("replication-factor")
	if s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			return nil, errors.Trace(err)
		}
		config.ReplicationFactor = int16(c)
	}

	s = sinkURI.Query().Get("kafka-version")
	if s != "" {
		config.Version = s
	}

	s = sinkURI.Query().Get("max-message-bytes")
	if s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			return nil, errors.Trace(err)
		}
		config.MaxMessageBytes = c
	}

	s = sinkURI.Query().Get("compression")
	if s != "" {
		config.Compression = s
	}

	config.ClientID = sinkURI.Query().Get("kafka-client-id")

	s = sinkURI.Query().Get("protocol")
	if s != "" {
		replicaConfig.Sink.Protocol = s
	}

	s = sinkURI.Query().Get("ca")
	if s != "" {
		config.Credential.CAPath = s
	}

	s = sinkURI.Query().Get("cert")
	if s != "" {
		config.Credential.CertPath = s
	}

	s = sinkURI.Query().Get("key")
	if s != "" {
		config.Credential.KeyPath = s
	}

	topicFmt := strings.TrimFunc(sinkURI.Path, func(r rune) bool {
		return r == '/'
	})

	newMQProducer := func (topic string) (mqProducer.Producer, error) {
		return mqProducer.NewKafkaSaramaProducer(ctx, sinkURI.Host, topic, config, errCh)
	}

	sink, err := newMqSink(ctx, config.Credential, newMQProducer, config.PartitionNum, filter, replicaConfig, opts, errCh, topicFmt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return sink, nil
}

func newPulsarSink(ctx context.Context, sinkURI *url.URL, filter *filter.Filter, replicaConfig *config.ReplicaConfig, opts map[string]string, errCh chan error) (*mqSink, error) {
	s := sinkURI.Query().Get("protocol")
	if s != "" {
		replicaConfig.Sink.Protocol = s
	}
	// For now, it's a place holder. Avro format have to make connection to Schema Registery,
	// and it may needs credential.
	credential := &security.Credential{}

	newMQProducer := func (topic string) (mqProducer.Producer, error) {
		return pulsar.NewProducer(sinkURI, errCh)
	}

	sink, err := newMqSink(ctx, credential, newMQProducer, 0, filter, replicaConfig, opts, errCh, "_")
	if err != nil {
		return nil, errors.Trace(err)
	}
	return sink, nil
}
