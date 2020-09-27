package puller

import (
	"bufio"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"go.uber.org/zap"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
)

const (
	fileBufferSize     = 16 * 1024 * 1024
	heapSizeLimit      = 4 * 1024 * 1024 // 4MB
	numConcurrentHeaps = 16
	memoryLimit        = 1024 * 1024 * 1024 // 1GB
)

type sorterBackEnd interface {
	readNext() (*model.PolymorphicEvent, error)
	writeNext(event *model.PolymorphicEvent) error
	getSize() int
	flush() error
	reset() error
}

type fileSorterBackEnd struct {
	f          *os.File
	readWriter *bufio.ReadWriter
	serde      serializerDeserializer
	rawBytes   []byte
	name       string
	size       int
}

func (f *fileSorterBackEnd) flush() error {
	err := f.readWriter.Flush()
	if err != nil {
		return errors.AddStack(err)
	}

	_, err = f.f.Seek(0, 0)
	if err != nil {
		return errors.Trace(err)
	}
	f.readWriter.Reader.Reset(f.f)
	f.readWriter.Writer.Reset(f.f)
	return nil
}

func (f *fileSorterBackEnd) getSize() int {
	return f.size
}

func (f *fileSorterBackEnd) reset() error {
	err := f.f.Truncate(int64(f.size))
	if err != nil {
		return errors.AddStack(err)
	}

	_, err = f.f.Seek(0, 0)
	if err != nil {
		return errors.AddStack(err)
	}

	f.size = 0
	f.readWriter.Reader.Reset(f.f)
	f.readWriter.Writer.Reset(f.f)
	return nil
}

type serializerDeserializer interface {
	marshal(event *model.PolymorphicEvent, bytes []byte) ([]byte, error)
	unmarshal(event *model.PolymorphicEvent, bytes []byte) ([]byte, error)
}

type msgPackGenSerde struct {
}

func (m *msgPackGenSerde) marshal(event *model.PolymorphicEvent, bytes []byte) ([]byte, error) {
	return event.RawKV.MarshalMsg(bytes)
}

func (m *msgPackGenSerde) unmarshal(event *model.PolymorphicEvent, bytes []byte) ([]byte, error) {
	if event.RawKV == nil {
		event.RawKV = new(model.RawKVEntry)
	}

	bytes, err := event.RawKV.UnmarshalMsg(bytes)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	event.StartTs = event.RawKV.StartTs
	event.CRTs = event.RawKV.CRTs

	return bytes, nil
}

func newFileSorterBackEnd(fileName string, serde serializerDeserializer) (*fileSorterBackEnd, error) {
	f, err := os.Create(fileName)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	reader := bufio.NewReaderSize(f, fileBufferSize)
	writer := bufio.NewWriterSize(f, fileBufferSize)
	readWriter := bufio.NewReadWriter(reader, writer)
	rawBytes := make([]byte, 0, 1024)

	log.Debug("new FileSorterBackEnd created", zap.String("filename", fileName))
	return &fileSorterBackEnd{
		f:          f,
		readWriter: readWriter,
		serde:      serde,
		rawBytes:   rawBytes,
		name:       fileName}, nil
}

func (f *fileSorterBackEnd) readNext() (*model.PolymorphicEvent, error) {
	var size uint32
	err := binary.Read(f.readWriter, binary.LittleEndian, &size)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, errors.AddStack(err)
	}

	if cap(f.rawBytes) < int(size) {
		f.rawBytes = make([]byte, 0, size)
	}
	f.rawBytes = f.rawBytes[:size]

	err = binary.Read(f.readWriter, binary.LittleEndian, f.rawBytes)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	event := new(model.PolymorphicEvent)
	_, err = f.serde.unmarshal(event, f.rawBytes)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	return event, nil
}

func (f *fileSorterBackEnd) writeNext(event *model.PolymorphicEvent) error {
	var err error
	f.rawBytes, err = f.serde.marshal(event, f.rawBytes)
	if err != nil {
		return errors.AddStack(err)
	}

	size := len(f.rawBytes)
	err = binary.Write(f.readWriter, binary.LittleEndian, uint32(size))
	if err != nil {
		return errors.AddStack(err)
	}

	err = binary.Write(f.readWriter, binary.LittleEndian, f.rawBytes)
	if err != nil {
		return errors.AddStack(err)
	}

	f.size += f.size + 8
	return nil
}

type memorySorterBackEnd struct {
	events    []*model.PolymorphicEvent
	readIndex int
}

func (m *memorySorterBackEnd) readNext() (*model.PolymorphicEvent, error) {
	if m.readIndex >= len(m.events) {
		return nil, nil
	}
	ret := m.events[m.readIndex]
	m.readIndex += 1
	return ret, nil
}

func (m *memorySorterBackEnd) writeNext(event *model.PolymorphicEvent) error {
	m.events = append(m.events, event)
	return nil
}

func (m *memorySorterBackEnd) getSize() int {
	return -1
}

func (m *memorySorterBackEnd) flush() error {
	return nil
}

func (m *memorySorterBackEnd) reset() error {
	m.events = m.events[0:0]
	m.readIndex = 0
	return nil
}

type backEndPool struct {
	memoryUseEstimate int64
	fileNameCounter   uint64
	mu                sync.Mutex
	cache             []unsafe.Pointer
	dir               string
}

func newBackEndPool(dir string) *backEndPool {
	return &backEndPool{
		memoryUseEstimate: 0,
		fileNameCounter:   0,
		mu:                sync.Mutex{},
		cache:             make([]unsafe.Pointer, 256),
		dir:               dir,
	}
}

func (p *backEndPool) alloc() (sorterBackEnd, error) {
	if atomic.LoadInt64(&p.memoryUseEstimate) < memoryLimit {
		ret := new(memorySorterBackEnd)
		atomic.AddInt64(&p.memoryUseEstimate, heapSizeLimit)
		return ret, nil
	}

	log.Debug("Unified Sorter: insufficient memory, using files to sort")

	for i := range p.cache {
		ptr := &p.cache[i]
		ret := atomic.SwapPointer(ptr, nil)
		if ret != nil {
			log.Debug("Unified Sorter: returning cached file backEnd")
			return *(*sorterBackEnd)(ret), nil
		}
	}

	fname := fmt.Sprintf("%s/sort-%d", p.dir, atomic.AddUint64(&p.fileNameCounter, 1))
	log.Debug("Unified Sorter: trying to create file backEnd")
	ret, err := newFileSorterBackEnd(fname, &msgPackGenSerde{})
	if err != nil {
		return nil, errors.AddStack(err)
	}

	atomic.AddInt64(&p.memoryUseEstimate, heapSizeLimit)
	return ret, nil
}

func (p *backEndPool) dealloc(backEnd sorterBackEnd) error {
	err := backEnd.reset()
	if err != nil {
		return errors.Trace(err)
	}

	switch b := backEnd.(type) {
	case *memorySorterBackEnd:
		atomic.AddInt64(&p.memoryUseEstimate, -heapSizeLimit)
		// Let GC do its job
		return nil
	case *fileSorterBackEnd:
		for i := range p.cache {
			ptr := &p.cache[i]
			if atomic.CompareAndSwapPointer(ptr, nil, unsafe.Pointer(b)) {
				return nil
			}
		}
		// Cache is full. Let GC do its job
	}
	panic("Unexpected type")
}

type flushTask struct {
	heapSorterId  int
	backend       sorterBackEnd
	maxResolvedTs uint64
	finished      chan error
	dealloc       func() error
}

type heapSorter struct {
	id          int
	inputCh     chan *model.PolymorphicEvent
	outputCh    chan *flushTask
	heap        sortHeap
	backEndPool *backEndPool
}

func newHeapSorter(id int, pool *backEndPool, out chan *flushTask) *heapSorter {
	return &heapSorter{
		id:          id,
		inputCh:     make(chan *model.PolymorphicEvent, 1024*1024),
		outputCh:    out,
		heap:        make(sortHeap, 0, 65536),
		backEndPool: pool,
	}
}

// flush should only be called within the main loop in run().
func (h *heapSorter) flush(ctx context.Context, maxResolvedTs uint64) error {
	isEmptyFlush := h.heap.Len() == 0
	var backEnd sorterBackEnd = nil

	if !isEmptyFlush {
		var err error
		backEnd, err = h.backEndPool.alloc()
		if err != nil {
			return errors.AddStack(err)
		}
	}

	task := &flushTask{
		heapSorterId:  h.id,
		backend:       backEnd,
		maxResolvedTs: maxResolvedTs,
		finished:      make(chan error),
	}

	var oldHeap sortHeap
	if !isEmptyFlush {
		task.dealloc = func() error {
			return h.backEndPool.dealloc(backEnd)
		}
		oldHeap = h.heap
		h.heap = make(sortHeap, 0, 65536)
	} else {
		task.dealloc = func() error {
			return nil
		}
	}

	log.Debug("Unified Sorter new flushTask", zap.Int("heap-id", task.heapSorterId),
		zap.Uint64("resolvedTs", task.maxResolvedTs))
	go func() {
		defer close(task.finished)
		if isEmptyFlush {
			return
		}
		batchSize := oldHeap.Len()
		for oldHeap.Len() > 0 {
			event := heap.Pop(&oldHeap).(*sortItem).entry
			err := task.backend.writeNext(event)
			if err != nil {
				task.finished <- err
				return
			}
		}
		err := task.backend.flush()
		if err != nil {
			task.finished <- err
		}

		log.Debug("Unified Sorter flushTask finished",
			zap.Int("heap-id", task.heapSorterId),
			zap.Uint64("resolvedTs", task.maxResolvedTs),
			zap.Int("size", batchSize))
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case h.outputCh <- task:
	}
	return nil
}

func (h *heapSorter) run(ctx context.Context) error {
	var (
		maxResolved           uint64
		heapSizeBytesEstimate int64
	)
	maxResolved = 0
	heapSizeBytesEstimate = 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-h.inputCh:
			heap.Push(&h.heap, &sortItem{entry: event})
			isResolvedEvent := event.RawKV != nil && event.RawKV.OpType == model.OpTypeResolved
			if isResolvedEvent {
				log.Debug("heapSorter got resolved event", zap.Uint64("CRTs", event.RawKV.CRTs), zap.Int("heap-id", h.id))
				if event.RawKV.CRTs < maxResolved {
					log.Fatal("ResolvedTs regression, bug?", zap.Uint64("event-resolvedTs", event.RawKV.CRTs),
						zap.Uint64("max-resolvedTs", maxResolved))
				}
				maxResolved = event.RawKV.CRTs
			}

			heapSizeBytesEstimate += event.RawKV.ApproximateSize()
			if heapSizeBytesEstimate >= heapSizeLimit || isResolvedEvent {
				err := h.flush(ctx, maxResolved)
				if err != nil {
					return errors.AddStack(err)
				}
				heapSizeBytesEstimate = 0
			}
		}
	}
}

func runMerger(ctx context.Context, numSorters int, in chan *flushTask, out chan *model.PolymorphicEvent) error {
	lastResolvedTs := make([]uint64, numSorters)
	minResolvedTs := uint64(0)

	pendingSet := make(map[*flushTask]*model.PolymorphicEvent, 0)

	sendResolvedEvent := func(ts uint64) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- model.NewResolvedPolymorphicEvent(0, ts):
				return nil
			default:
				log.Warn("sendResolvedEvent blocked!")
			}
		}
	}

	onMinResolvedTsUpdate := func() error {
		log.Debug("onMinResolvedTsUpdate", zap.Uint64("minResolvedTs", minResolvedTs))
		workingSet := make(map[*flushTask]struct{}, 0)
		sortHeap := new(sortHeap)
		for task, cache := range pendingSet {
			if task.maxResolvedTs <= minResolvedTs {
				var event *model.PolymorphicEvent
				if cache != nil {
					event = cache
				} else {
					var err error

					select {
					case <-ctx.Done():
						return ctx.Err()
					case err := <-task.finished:
						if err != nil {
							return errors.Trace(err)
						}
					}

					event, err = task.backend.readNext()
					if err != nil {
						return errors.Trace(err)
					}

				}

				if event != nil && event.CRTs > minResolvedTs {
					pendingSet[task] = event
					continue
				}

				pendingSet[task] = nil
				workingSet[task] = struct{}{}

				heap.Push(sortHeap, &sortItem{
					entry: event,
					data:  task,
				})
			}
		}

		log.Debug("Started merging", zap.Int("num-flush-tasks", len(workingSet)))

		resolvedTicker := time.NewTicker(1 * time.Second)
		defer resolvedTicker.Stop()

		retire := func(task *flushTask) error {
			delete(workingSet, task)
			nextEvent, err := task.backend.readNext()
			if err != nil {
				return errors.Trace(err)
			}

			if nextEvent == nil {
				delete(pendingSet, task)

				err := task.dealloc()
				if err != nil {
					return errors.Trace(err)
				}
			} else {
				pendingSet[task] = nextEvent
			}
			return nil
		}
		for sortHeap.Len() > 0 {
			item := heap.Pop(sortHeap).(*sortItem)
			task := item.data.(*flushTask)
			event := item.entry

			if event.RawKV != nil && event.RawKV.OpType != model.OpTypeResolved {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- event:
				}
			}

			// read next event from backend
			event, err := task.backend.readNext()
			if err != nil {
				return errors.Trace(err)
			}

			if event == nil {
				// EOF
				delete(workingSet, task)
				delete(pendingSet, task)

				err := task.dealloc()
				if err != nil {
					return errors.Trace(err)
				}

				continue
			}

			if event.CRTs >= minResolvedTs {
				// we have processed all events from this task that need to be processed in this merge
				err := retire(task)
				if err != nil {
					return errors.Trace(err)
				}
				continue
			}

			heap.Push(sortHeap, &sortItem{
				entry: event,
				data:  task,
			})

			select {
			case <-resolvedTicker.C:
				err := sendResolvedEvent(event.CRTs)
				if err != nil {
					return errors.Trace(err)
				}
			default:
			}
		}

		log.Debug("Unified Sorter: merging ended", zap.Uint64("resolvedTs", minResolvedTs))
		err := sendResolvedEvent(minResolvedTs)
		if err != nil {
			return errors.Trace(err)
		}

		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-in:
			if task == nil {
				return errors.New("Unified Sorter: nil flushTask, exiting")
			} else {
				log.Debug("Merger got flushTask", zap.Int("heap-id", task.heapSorterId))
			}

			if task.backend != nil {
				pendingSet[task] = nil
			} // otherwise it is an empty flush

			if lastResolvedTs[task.heapSorterId] < task.maxResolvedTs {
				lastResolvedTs[task.heapSorterId] = task.maxResolvedTs
			}

			minTemp := uint64(math.MaxUint64)
			for _, ts := range lastResolvedTs {
				if minTemp > ts {
					minTemp = ts
				}
			}

			if minTemp > minResolvedTs {
				minResolvedTs = minTemp
				err := onMinResolvedTsUpdate()
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}
}

type UnifiedSorter struct {
	inputCh  chan *model.PolymorphicEvent
	outputCh chan *model.PolymorphicEvent
	dir      string
	pool     *backEndPool
}

func NewUnifiedSorter(dir string) *UnifiedSorter {
	return &UnifiedSorter{
		inputCh:  make(chan *model.PolymorphicEvent, 128000),
		outputCh: make(chan *model.PolymorphicEvent, 128000),
		dir:      dir,
		pool:     newBackEndPool(dir),
	}
}

func (s *UnifiedSorter) Run(ctx context.Context) error {
	nextSorterId := 0
	heapSorters := make([]*heapSorter, numConcurrentHeaps)

	sorterOutCh := make(chan *flushTask, 4096)
	defer close(sorterOutCh)

	errCh := make(chan error)
	for i := range heapSorters {
		finalI := i
		heapSorters[finalI] = newHeapSorter(finalI, s.pool, sorterOutCh)
		go func() {
			errCh <- heapSorters[finalI].run(ctx)
		}()
	}

	go func() {
		errCh <- runMerger(ctx, numConcurrentHeaps, sorterOutCh, s.outputCh)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			if err != nil {
				return errors.Trace(err)
			}
		case event := <-s.inputCh:
			if event.RawKV != nil && event.RawKV.OpType == model.OpTypeResolved {
				// broadcast resolved events
				for _, sorter := range heapSorters {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case sorter.inputCh <- event:
					}
				}
				log.Debug("Unified Sorter: event broadcast", zap.Uint64("CRTs", event.CRTs))
				continue
			}

			// dispatch a row changed event
			targetId := nextSorterId % numConcurrentHeaps
			nextSorterId++
			select {
			case <-ctx.Done():
				return ctx.Err()
			case heapSorters[targetId].inputCh <- event:
			}

			//log.Debug("Unified Sorter: event dispatched",
			//	zap.Uint64("CRTs", event.CRTs),
			//	zap.Int("heap-id", targetId))
		}
	}
}

func (s *UnifiedSorter) AddEntry(ctx context.Context, entry *model.PolymorphicEvent) {
	select {
	case <-ctx.Done():
		return
	case s.inputCh <- entry:
	}
}

func (s *UnifiedSorter) Output() <-chan *model.PolymorphicEvent {
	return s.outputCh
}
