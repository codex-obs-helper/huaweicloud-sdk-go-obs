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
	"runtime"
	"sync"
	"testing"
	"time"
)

// testFlusher implements AsyncFlusher for testing
type testFlusher struct {
	mu      sync.Mutex
	results [][]int
}

func (f *testFlusher) Flush(items []AsyncItem) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	batch := make([]int, len(items))
	for i, item := range items {
		batch[i] = item.(*testItem).id
	}
	f.results = append(f.results, batch)
	return nil
}

type testItem struct {
	id int
}

func (t *testItem) Apply(target interface{}) {
	// no-op for unit test
}

type blockingFlusher struct {
	mu      sync.Mutex
	results [][]int
	start   chan struct{}
	release chan struct{}
	calls   int
}

func (f *blockingFlusher) Flush(items []AsyncItem) error {
	f.mu.Lock()
	f.calls++
	callNum := f.calls
	batch := make([]int, len(items))
	for i, item := range items {
		batch[i] = item.(*testItem).id
	}
	f.results = append(f.results, batch)
	start := f.start
	release := f.release
	f.mu.Unlock()

	if callNum == 1 {
		close(start)
		<-release
	}
	return nil
}

func TestAsyncWriter_BatchFlush(t *testing.T) {
	flusher := &testFlusher{}
	aw := NewAsyncWriter(nil, flusher, WithAsyncBatchSize(5))
	defer aw.Shutdown()

	for i := 1; i <= 10; i++ {
		aw.Submit(&testItem{id: i})
	}
	time.Sleep(time.Second)

	flusher.mu.Lock()
	defer flusher.mu.Unlock()

	if len(flusher.results) != 2 {
		t.Errorf("expected 2 batches, got %d", len(flusher.results))
	}
	if len(flusher.results[0]) != 5 {
		t.Errorf("expected 5 items in first batch, got %d", len(flusher.results[0]))
	}
	if len(flusher.results[1]) != 5 {
		t.Errorf("expected 5 items in second batch, got %d", len(flusher.results[1]))
	}
}

func TestAsyncWriter_ShutdownFlush(t *testing.T) {
	flusher := &testFlusher{}
	aw := NewAsyncWriter(nil, flusher, WithAsyncBatchSize(100))

	// only submit 3 items, not enough for a batch
	for i := 1; i <= 3; i++ {
		aw.Submit(&testItem{id: i})
	}

	// shutdown should flush all
	time.Sleep(100 * time.Millisecond)
	aw.Shutdown()

	flusher.mu.Lock()
	defer flusher.mu.Unlock()

	if len(flusher.results) != 1 {
		t.Errorf("expected 1 batch after shutdown, got %d", len(flusher.results))
	}
	if len(flusher.results[0]) != 3 {
		t.Errorf("expected 3 items in batch, got %d", len(flusher.results[0]))
	}
}

func TestAsyncWriter_NonBlocking(t *testing.T) {
	flusher := &testFlusher{}
	aw := NewAsyncWriter(nil, flusher, WithAsyncBatchSize(100)) // large batch to avoid flush
	defer aw.Shutdown()

	done := make(chan struct{})

	go func() {
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
				aw.Submit(&testItem{id: i + 1})
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)
	close(done)
	flusher.mu.Lock()
	defer flusher.mu.Unlock()

	if len(flusher.results) == 0 {
		t.Error("no batches flushed")
	}
}

func TestAsyncWriter_EmptyFlush(t *testing.T) {
	flusher := &testFlusher{}
	aw := NewAsyncWriter(nil, flusher, WithAsyncBatchSize(5))
	defer aw.Shutdown()

	// shutdown without submitting any items - should not panic

	flusher.mu.Lock()
	defer flusher.mu.Unlock()

	if len(flusher.results) != 0 {
		t.Errorf("expected 0 batches, got %d", len(flusher.results))
	}
}

func TestAsyncWriter_TickerFlush(t *testing.T) {
	flusher := &testFlusher{}
	// batchSize=100 (large), flushInterval=100ms (short)
	aw := NewAsyncWriter(nil, flusher, WithAsyncBatchSize(100), WithAsyncFlushInterval(100*time.Millisecond))
	defer aw.Shutdown()

	// submit only 3 items, not enough for batchSize
	for i := 1; i <= 3; i++ {
		aw.Submit(&testItem{id: i})
	}

	// wait for ticker to trigger flush (needs >100ms)
	time.Sleep(200 * time.Millisecond)

	flusher.mu.Lock()
	defer flusher.mu.Unlock()

	if len(flusher.results) != 1 {
		t.Errorf("expected 1 batch from ticker flush, got %d", len(flusher.results))
	}
	if len(flusher.results[0]) != 3 {
		t.Errorf("expected 3 items in batch, got %d", len(flusher.results[0]))
	}
}

func TestAsyncWriter_ConcurrentSubmit(t *testing.T) {
	flusher := &testFlusher{}
	// large channel capacity to avoid drops during concurrent submit
	aw := NewAsyncWriter(nil, flusher, WithAsyncBatchSize(50), WithChannelCapacity(200))
	defer aw.Shutdown()

	var wg sync.WaitGroup
	numGoroutines := 10
	itemsPerGoroutine := 20

	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < itemsPerGoroutine; i++ {
				aw.Submit(&testItem{id: gid*100 + i})
			}
		}(g)
	}

	wg.Wait()
	time.Sleep(200 * time.Millisecond)

	flusher.mu.Lock()
	defer flusher.mu.Unlock()

	totalItems := 0
	for _, batch := range flusher.results {
		totalItems += len(batch)
	}

	expectedTotal := numGoroutines * itemsPerGoroutine
	if totalItems != expectedTotal {
		t.Errorf("expected %d items total, got %d", expectedTotal, totalItems)
	}
}

func TestAsyncWriter_ChannelFullBlocksUntilSpaceAvailable(t *testing.T) {
	flusher := &blockingFlusher{
		start:   make(chan struct{}),
		release: make(chan struct{}),
	}
	aw := NewAsyncWriter(nil, flusher, WithAsyncBatchSize(1), WithChannelCapacity(1), WithAsyncFlushInterval(time.Hour))
	defer aw.Shutdown()

	aw.Submit(&testItem{id: 1})
	<-flusher.start
	aw.Submit(&testItem{id: 2})

	done := make(chan struct{})
	go func() {
		aw.Submit(&testItem{id: 3})
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("expected submit to block while channel is full")
	case <-time.After(100 * time.Millisecond):
	}

	close(flusher.release)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("submit did not resume after writer made progress")
	}
}

func TestAsyncWriter_GoroutineLeak(t *testing.T) {
	flusher := &testFlusher{}
	aw := NewAsyncWriter(nil, flusher, WithAsyncBatchSize(5))

	// baseline goroutine count
	baseline := runtime.NumGoroutine()

	// submit and shutdown
	for i := 0; i < 5; i++ {
		aw.Submit(&testItem{id: i})
	}
	aw.Shutdown()

	// wait a bit for goroutine to exit
	time.Sleep(100 * time.Millisecond)

	after := runtime.NumGoroutine()
	if after > baseline {
		t.Errorf("goroutine leak detected: baseline=%d, after=%d", baseline, after)
	}
}

func TestCheckpointAsyncWriter_BatchSize(t *testing.T) {
	// create mock upload checkpoint with 10 parts
	cp := &UploadCheckpoint{
		UploadParts: make([]UploadPartInfo, 10),
	}
	for i := 0; i < 10; i++ {
		cp.UploadParts[i] = UploadPartInfo{PartNumber: i + 1}
	}

	// use temp file path for checkpoint
	tmpfile := t.TempDir() + "/checkpoint_test.xml"

	cw := NewCheckpointAsyncWriter(cp, tmpfile, WithAsyncBatchSize(5))
	defer cw.Shutdown()

	// submit 10 part updates
	for i := 1; i <= 10; i++ {
		cw.Submit(&UploadPartUpdate{PartNumber: i, ETag: "etag", IsCompleted: true})
	}

	// wait for batch flush to complete
	time.Sleep(300 * time.Millisecond)

	// verify checkpoint was updated
	for i := 0; i < 10; i++ {
		if cp.UploadParts[i].Etag != "etag" {
			t.Errorf("part %d: expected etag set, got empty", i+1)
		}
	}
}

func TestCheckpointAsyncWriter_FlushInterval(t *testing.T) {
	cp := &UploadCheckpoint{
		UploadParts: make([]UploadPartInfo, 5),
	}
	for i := 0; i < 5; i++ {
		cp.UploadParts[i] = UploadPartInfo{PartNumber: i + 1}
	}

	tmpfile := t.TempDir() + "/checkpoint_interval_test.xml"

	// batchSize large, flushInterval short
	cw := NewCheckpointAsyncWriter(cp, tmpfile, WithAsyncBatchSize(100), WithAsyncFlushInterval(100*time.Millisecond))
	defer cw.Shutdown()

	// submit only 3 updates (less than batchSize)
	for i := 1; i <= 3; i++ {
		cw.Submit(&UploadPartUpdate{PartNumber: i, ETag: "etag", IsCompleted: true})
	}

	// wait for ticker flush
	time.Sleep(250 * time.Millisecond)

	// verify checkpoint was updated by ticker
	for i := 0; i < 3; i++ {
		if cp.UploadParts[i].Etag != "etag" {
			t.Errorf("part %d: expected etag set after ticker flush, got empty", i+1)
		}
	}
}

func TestAsyncWriter_SyncFlushDrainsQueuedItems(t *testing.T) {
	flusher := &blockingFlusher{
		start:   make(chan struct{}),
		release: make(chan struct{}),
	}
	aw := NewAsyncWriter(nil, flusher, WithAsyncBatchSize(1), WithChannelCapacity(4), WithAsyncFlushInterval(time.Hour))
	defer aw.Shutdown()

	aw.Submit(&testItem{id: 1})
	<-flusher.start
	aw.Submit(&testItem{id: 2})
	aw.Submit(&testItem{id: 3})

	done := make(chan struct{})
	go func() {
		aw.SyncFlush()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("sync flush should wait for the in-flight flush to complete")
	case <-time.After(100 * time.Millisecond):
	}

	close(flusher.release)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sync flush did not complete")
	}

	flusher.mu.Lock()
	defer flusher.mu.Unlock()

	totalItems := 0
	for _, batch := range flusher.results {
		totalItems += len(batch)
	}
	if totalItems != 3 {
		t.Fatalf("expected all 3 items to be flushed, got %d", totalItems)
	}
	if len(flusher.results) < 2 {
		t.Fatalf("expected at least 2 flushes, got %d", len(flusher.results))
	}
}

func TestUploadPartConcurrent_NoCheckpointDoesNotPanic(t *testing.T) {
	obsClient := ObsClient{}
	ufc := &UploadCheckpoint{
		FileInfo: FileStatus{
			Size: 1,
		},
		UploadParts: []UploadPartInfo{
			{PartNumber: 1, PartSize: 1, IsCompleted: true},
		},
	}

	err := obsClient.uploadPartConcurrent(ufc, "", &UploadFileInput{
		TaskNum:          1,
		EnableCheckpoint: false,
	}, nil)
	if err != nil {
		t.Fatalf("expected no error when checkpoint is disabled, got %v", err)
	}
}

func TestDownloadFileConcurrent_NoCheckpointDoesNotPanic(t *testing.T) {
	obsClient := ObsClient{}
	dfc := &DownloadCheckpoint{
		ObjectInfo: ObjectInfo{
			Size: 1,
		},
		DownloadParts: []DownloadPartInfo{
			{PartNumber: 1, Offset: 0, RangeEnd: 0, IsCompleted: true},
		},
	}

	err := obsClient.downloadFileConcurrent(&DownloadFileInput{
		TaskNum:          1,
		EnableCheckpoint: false,
	}, dfc, nil)
	if err != nil {
		t.Fatalf("expected no error when checkpoint is disabled, got %v", err)
	}
}
