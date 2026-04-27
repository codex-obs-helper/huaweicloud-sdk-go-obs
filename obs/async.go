/*
 * Copyright (C) 2024 Huawei Device Co., Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package obs

import (
	"sync"
	"time"
)

// AsyncItem is the interface for applying data to target
type AsyncItem interface {
	Apply(target interface{})
}

// AsyncFlusher is the interface for flushing data to storage
type AsyncFlusher interface {
	Flush(items []AsyncItem) error
}

// AsyncWriter is a generic async batch writer
// Receives items, accumulates them, and calls Flusher to write
type AsyncWriter struct {
	ch            chan AsyncItem
	done          chan struct{}
	flushReq      chan chan error
	pendingReq    chan chan []AsyncItem
	items         []AsyncItem
	batchSize     int           // TODO: to be determined by performance testing, temp value 5
	flushInterval time.Duration // TODO: to be determined by performance testing, temp value 5s
	target        interface{}
	flusher       AsyncFlusher
	wg            sync.WaitGroup
	closeOnce     sync.Once
}

// AsyncWriterOption is the configuration option for AsyncWriter
type AsyncWriterOption func(*AsyncWriter)

// WithAsyncBatchSize sets the batch size
func WithAsyncBatchSize(n int) AsyncWriterOption {
	return func(aw *AsyncWriter) {
		if n > 0 {
			aw.batchSize = n
		}
	}
}

// WithAsyncFlushInterval sets the forced flush interval
func WithAsyncFlushInterval(d time.Duration) AsyncWriterOption {
	return func(aw *AsyncWriter) {
		if d > 0 {
			aw.flushInterval = d
		}
	}
}

// WithChannelCapacity sets the channel capacity (default 50, Checkpoint recommends 10000)
func WithChannelCapacity(n int) AsyncWriterOption {
	return func(aw *AsyncWriter) {
		if n > 0 {
			aw.ch = make(chan AsyncItem, n)
		}
	}
}

// NewAsyncWriter creates an AsyncWriter
// target: target data structure (e.g. *UploadCheckpoint)
// flusher: actual object that performs the write
func NewAsyncWriter(target interface{}, flusher AsyncFlusher, opts ...AsyncWriterOption) *AsyncWriter {
	aw := &AsyncWriter{
		ch:            make(chan AsyncItem, 50), // default 50, Checkpoint uses 10000
		done:          make(chan struct{}),
		flushReq:      make(chan chan error),
		pendingReq:    make(chan chan []AsyncItem),
		items:         make([]AsyncItem, 0, 100),
		batchSize:     5,               // TODO: to be determined by perf testing, temp value
		flushInterval: 5 * time.Second, // TODO: to be determined by perf testing, temp value
		target:        target,
		flusher:       flusher,
	}
	for _, opt := range opts {
		opt(aw)
	}
	aw.wg.Add(1)
	go aw.run()
	return aw
}

// Submit submits an update.
// It blocks when the writer is back-pressured so updates are never dropped.
func (aw *AsyncWriter) Submit(item AsyncItem) {
	select {
	case aw.ch <- item:
	case <-aw.done:
		doLog(LEVEL_WARN, "async writer already shut down, dropping update")
	}
}

// Shutdown gracefully shuts down
func (aw *AsyncWriter) Shutdown() {
	aw.closeOnce.Do(func() {
		close(aw.done)
		aw.wg.Wait()
	})
}

func (aw *AsyncWriter) run() {
	defer aw.wg.Done()
	ticker := time.NewTicker(aw.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case item := <-aw.ch:
			aw.items = append(aw.items, item)
			// check if batchSize threshold is reached
			if len(aw.items) >= aw.batchSize {
				if err := aw.doFlush(); err != nil {
					doLog(LEVEL_ERROR, "async flush failed: %v", err)
				}
			}

		case <-ticker.C:
			// timer fallback, ensure flush even in extreme cases
			if err := aw.doFlush(); err != nil {
				doLog(LEVEL_ERROR, "async flush failed: %v", err)
			}

		case resultCh := <-aw.flushReq:
			aw.drainChannel()
			resultCh <- aw.doFlush()

		case resultCh := <-aw.pendingReq:
			items := make([]AsyncItem, len(aw.items))
			copy(items, aw.items)
			resultCh <- items

		case <-aw.done:
			// drain remaining items in channel, then flush
			aw.drainChannel()
			if err := aw.doFlush(); err != nil {
				doLog(LEVEL_ERROR, "async flush failed: %v", err)
			}
			return
		}
	}
}

func (aw *AsyncWriter) drainChannel() {
	for {
		select {
		case item := <-aw.ch:
			aw.items = append(aw.items, item)
		default:
			return
		}
	}
}

func (aw *AsyncWriter) doFlush() error {
	if len(aw.items) == 0 {
		return nil
	}
	items := aw.items
	aw.items = make([]AsyncItem, 0, aw.batchSize)

	// apply and file write are both handled by Flusher
	return aw.flusher.Flush(items)
}

// SyncFlush forces a synchronous flush of all pending items.
// Called before shutdown to ensure no data loss on crash.
func (aw *AsyncWriter) SyncFlush() {
	resultCh := make(chan error, 1)
	select {
	case aw.flushReq <- resultCh:
		if err := <-resultCh; err != nil {
			doLog(LEVEL_ERROR, "sync flush failed: %v", err)
		}
	case <-aw.done:
		return
	}
}

// GetPendingItems returns a copy of currently buffered items
func (aw *AsyncWriter) GetPendingItems() []AsyncItem {
	resultCh := make(chan []AsyncItem, 1)
	select {
	case aw.pendingReq <- resultCh:
		return <-resultCh
	case <-aw.done:
		return nil
	}
}
