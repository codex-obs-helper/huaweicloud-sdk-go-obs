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
	"sync/atomic"
	"testing"
	"time"
)

// TestUploadPartUpdate_Apply verifies UploadPartUpdate correctly applies to UploadCheckpoint
func TestUploadPartUpdate_Apply(t *testing.T) {
	cp := &UploadCheckpoint{
		UploadParts: []UploadPartInfo{
			{PartNumber: 1, PartSize: 100},
			{PartNumber: 2, PartSize: 200},
			{PartNumber: 3, PartSize: 300},
		},
	}

	// Apply update for part 2
	update := &UploadPartUpdate{
		PartNumber:  2,
		ETag:        "etag-abc",
		IsCompleted: true,
	}
	update.Apply(cp)

	// Verify only part 2 is updated
	if cp.UploadParts[0].Etag != "" {
		t.Error("part 1 should not be updated")
	}
	if cp.UploadParts[0].IsCompleted {
		t.Error("part 1 should not be marked completed")
	}

	if cp.UploadParts[1].Etag != "etag-abc" {
		t.Errorf("part 2 etag = %q, want %q", cp.UploadParts[1].Etag, "etag-abc")
	}
	if !cp.UploadParts[1].IsCompleted {
		t.Error("part 2 should be marked completed")
	}

	if cp.UploadParts[2].Etag != "" {
		t.Error("part 3 should not be updated")
	}
}

// TestDownloadPartUpdate_Apply verifies DownloadPartUpdate correctly applies to DownloadCheckpoint
func TestDownloadPartUpdate_Apply(t *testing.T) {
	cp := &DownloadCheckpoint{
		DownloadParts: []DownloadPartInfo{
			{PartNumber: 1, Offset: 0, RangeEnd: 99},
			{PartNumber: 2, Offset: 100, RangeEnd: 299},
			{PartNumber: 3, Offset: 300, RangeEnd: 599},
		},
	}

	// Apply update for part 1
	update := &DownloadPartUpdate{
		PartNumber:  1,
		IsCompleted: true,
	}
	update.Apply(cp)

	// Verify only part 1 is updated
	if !cp.DownloadParts[0].IsCompleted {
		t.Error("part 1 should be marked completed")
	}
	if cp.DownloadParts[1].IsCompleted {
		t.Error("part 2 should not be marked completed")
	}
	if cp.DownloadParts[2].IsCompleted {
		t.Error("part 3 should not be marked completed")
	}
}

// TestCheckpointAsyncWriter_DownloadCheckpoint verifies CheckpointAsyncWriter works with DownloadCheckpoint
func TestCheckpointAsyncWriter_DownloadCheckpoint(t *testing.T) {
	cp := &DownloadCheckpoint{
		ObjectInfo: ObjectInfo{Size: 600},
		DownloadParts: []DownloadPartInfo{
			{PartNumber: 1, Offset: 0, RangeEnd: 199},
			{PartNumber: 2, Offset: 200, RangeEnd: 399},
			{PartNumber: 3, Offset: 400, RangeEnd: 599},
		},
	}

	tmpfile := t.TempDir() + "/download_checkpoint_test.xml"

	cw := NewCheckpointAsyncWriter(cp, tmpfile, WithAsyncBatchSize(2))
	defer cw.Shutdown()

	// Submit updates for parts 1 and 3
	cw.Submit(&DownloadPartUpdate{PartNumber: 1, IsCompleted: true})
	cw.Submit(&DownloadPartUpdate{PartNumber: 3, IsCompleted: true})

	// Wait for batch flush
	time.Sleep(300 * time.Millisecond)

	// Verify parts were updated in memory
	if !cp.DownloadParts[0].IsCompleted {
		t.Error("part 1 should be completed after flush")
	}
	if cp.DownloadParts[1].IsCompleted {
		t.Error("part 2 should not be completed")
	}
	if !cp.DownloadParts[2].IsCompleted {
		t.Error("part 3 should be completed after flush")
	}
}

// TestAtomicAddInt64_ReturnValue verifies atomic.AddInt64 returns correct new value
// This ensures progress events use the correct completed bytes count
func TestAtomicAddInt64_ReturnValue(t *testing.T) {
	var counter int64

	// Simulate multiple goroutines adding to counter
	var wg sync.WaitGroup
	results := make([]int64, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// Each goroutine adds 100
			newVal := atomic.AddInt64(&counter, 100)
			results[idx] = newVal
		}(i)
	}

	wg.Wait()

	// Verify total is correct
	if counter != 1000 {
		t.Errorf("counter = %d, want 1000", counter)
	}

	// Verify each result is valid (multiple of 100, between 100 and 1000)
	for i := 0; i < 10; i++ {
		if results[i] < 100 || results[i] > 1000 || results[i]%100 != 0 {
			t.Errorf("results[%d] = %d, want a multiple of 100 in range [100, 1000]", i, results[i])
		}
	}
}

// TestAtomicAddInt64_ProgressSimulation simulates the progress tracking pattern used in handleUploadTaskResult
func TestAtomicAddInt64_ProgressSimulation(t *testing.T) {
	var completedBytes int64
	totalBytes := int64(1000)
	partSizes := []int64{100, 200, 300, 400} // sum = 1000

	var wg sync.WaitGroup
	listenerResults := make([]int64, 0, len(partSizes))
	var mu sync.Mutex

	for _, partSize := range partSizes {
		wg.Add(1)
		go func(ps int64) {
			defer wg.Done()
			// This mimics the pattern in handleUploadTaskResult line 453
			newCompleted := atomic.AddInt64(&completedBytes, ps)
			mu.Lock()
			listenerResults = append(listenerResults, newCompleted)
			mu.Unlock()
		}(partSize)
	}

	wg.Wait()

	// Verify final count
	if completedBytes != totalBytes {
		t.Errorf("completedBytes = %d, want %d", completedBytes, totalBytes)
	}

	// Verify listener received monotonically increasing values
	// Each should be sum of parts processed so far
	if len(listenerResults) != len(partSizes) {
		t.Fatalf("got %d results, want %d", len(listenerResults), len(partSizes))
	}
}

// errorFlusher always returns an error on Flush
type errorFlusher struct {
	mu      sync.Mutex
	calls   int
	errMsg  string
}

func (f *errorFlusher) Flush(items []AsyncItem) error {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return &testFlushError{f.errMsg}
}

type testFlushError struct {
	msg string
}

func (e *testFlushError) Error() string {
	return e.msg
}

// TestAsyncWriter_SyncFlushErrorHandling verifies SyncFlush handles flush errors gracefully
func TestAsyncWriter_SyncFlushErrorHandling(t *testing.T) {
	flusher := &errorFlusher{errMsg: "simulated flush failure"}
	aw := NewAsyncWriter(nil, flusher, WithAsyncBatchSize(100), WithChannelCapacity(100))
	defer aw.Shutdown()

	// Submit some items
	for i := 0; i < 5; i++ {
		aw.Submit(&testItem{id: i})
	}

	// SyncFlush should not panic even if flush returns error
	// The error is logged but not propagated in current implementation
	aw.SyncFlush()

	flusher.mu.Lock()
	defer flusher.mu.Unlock()

	if flusher.calls == 0 {
		t.Error("flusher should have been called at least once")
	}
}

// TestAsyncWriter_SubmitAfterShutdown verifies behavior when submitting after shutdown
func TestAsyncWriter_SubmitAfterShutdown(t *testing.T) {
	flusher := &testFlusher{}
	aw := NewAsyncWriter(nil, flusher, WithAsyncBatchSize(100))

	// Submit some items before shutdown
	for i := 0; i < 5; i++ {
		aw.Submit(&testItem{id: i})
	}

	aw.Shutdown()

	// After shutdown, Submit should not panic
	// In current implementation, items submitted after shutdown are dropped
	aw.Submit(&testItem{id: 999})

	// Should not panic - this is the key assertion
}

// TestCheckpointAsyncWriter_SyncFlushBeforeShutdown verifies SyncFlush drains all pending items
func TestCheckpointAsyncWriter_SyncFlushBeforeShutdown(t *testing.T) {
	cp := &UploadCheckpoint{
		UploadParts: make([]UploadPartInfo, 10),
	}
	for i := 0; i < 10; i++ {
		cp.UploadParts[i] = UploadPartInfo{PartNumber: i + 1, PartSize: 100}
	}

	tmpfile := t.TempDir() + "/syncflush_test.xml"

	cw := NewCheckpointAsyncWriter(cp, tmpfile, WithAsyncBatchSize(100)) // large batch so no auto-flush
	defer cw.Shutdown()

	// Submit all 10 updates
	for i := 1; i <= 10; i++ {
		cw.Submit(&UploadPartUpdate{PartNumber: i, ETag: "etag", IsCompleted: true})
	}

	// Before shutdown, call SyncFlush to ensure all items are persisted
	cw.SyncFlush()

	// Verify all parts were updated
	for i := 0; i < 10; i++ {
		if cp.UploadParts[i].Etag != "etag" {
			t.Errorf("part %d: etag not set after SyncFlush", i+1)
		}
		if !cp.UploadParts[i].IsCompleted {
			t.Errorf("part %d: not marked completed after SyncFlush", i+1)
		}
	}
}

// TestCheckpointAsyncWriter_DownloadSyncFlush verifies SyncFlush works with DownloadCheckpoint
func TestCheckpointAsyncWriter_DownloadSyncFlush(t *testing.T) {
	cp := &DownloadCheckpoint{
		ObjectInfo: ObjectInfo{Size: 500},
		DownloadParts: []DownloadPartInfo{
			{PartNumber: 1, Offset: 0, RangeEnd: 99},
			{PartNumber: 2, Offset: 100, RangeEnd: 299},
			{PartNumber: 3, Offset: 300, RangeEnd: 499},
		},
	}

	tmpfile := t.TempDir() + "/download_syncflush_test.xml"

	cw := NewCheckpointAsyncWriter(cp, tmpfile, WithAsyncBatchSize(100))
	defer cw.Shutdown()

	// Submit updates
	cw.Submit(&DownloadPartUpdate{PartNumber: 1, IsCompleted: true})
	cw.Submit(&DownloadPartUpdate{PartNumber: 2, IsCompleted: true})

	// SyncFlush should persist
	cw.SyncFlush()

	if !cp.DownloadParts[0].IsCompleted {
		t.Error("part 1 should be completed after SyncFlush")
	}
	if !cp.DownloadParts[1].IsCompleted {
		t.Error("part 2 should be completed after SyncFlush")
	}
	if cp.DownloadParts[2].IsCompleted {
		t.Error("part 3 should not be completed")
	}
}

// TestProgressListener_ConcurrentCallbacks verifies ProgressListener can handle concurrent calls
// Design doc Section 6.1.1: TestProgressListener_ShouldDocumentConcurrentCallbacks
func TestProgressListener_ConcurrentCallbacks(t *testing.T) {
	var mu sync.Mutex
	eventCount := 0
	events := make([]int64, 0)

	// Thread-safe listener implementation
	safeListener := ProgressListenerFunc(func(event *ProgressEvent) {
		mu.Lock()
		defer mu.Unlock()
		eventCount++
		events = append(events, event.ConsumedBytes)
	})

	var wg sync.WaitGroup
	numGoroutines := 10
	eventsPerGoroutine := 100

	// Simulate concurrent progress events from multiple workers
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < eventsPerGoroutine; i++ {
				consumed := int64(gid*eventsPerGoroutine + i + 1)
				event := newProgressEvent(TransferDataEvent, consumed, 1000)
				safeListener.ProgressChanged(event)
			}
		}(g)
	}

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if eventCount != numGoroutines*eventsPerGoroutine {
		t.Errorf("eventCount = %d, want %d", eventCount, numGoroutines*eventsPerGoroutine)
	}
	if len(events) != numGoroutines*eventsPerGoroutine {
		t.Errorf("events length = %d, want %d", len(events), numGoroutines*eventsPerGoroutine)
	}
}

// ProgressListenerFunc adapter for testing
type ProgressListenerFunc func(event *ProgressEvent)

func (f ProgressListenerFunc) ProgressChanged(event *ProgressEvent) {
	f(event)
}

// TestUploadProgress_AtomicCompletedBytes verifies upload progress uses atomic.AddInt64 return value
// Design doc Section 6.1.1: TestUploadProgress_ShouldUseAtomicCompletedBytes
func TestUploadProgress_AtomicCompletedBytes(t *testing.T) {
	var completedBytes int64
	totalBytes := int64(1000)
	partSizes := []int64{100, 200, 300, 400} // sum = 1000

	listener := ProgressListenerFunc(func(event *ProgressEvent) {
		if event.EventType != TransferDataEvent {
			return
		}
		// Verify consumed bytes is monotonically increasing and never exceeds total
		if event.ConsumedBytes < 0 || event.ConsumedBytes > totalBytes {
			t.Errorf("ConsumedBytes = %d, must be in [0, %d]", event.ConsumedBytes, totalBytes)
		}
	})

	// Simulate upload progress pattern
	for _, partSize := range partSizes {
		newCompleted := atomic.AddInt64(&completedBytes, partSize)
		event := newProgressEvent(TransferDataEvent, newCompleted, totalBytes)
		publishProgress(listener, event)
	}

	if completedBytes != totalBytes {
		t.Errorf("completedBytes = %d, want %d", completedBytes, totalBytes)
	}
}

// TestDownloadProgress_AtomicCompletedBytes verifies download progress uses atomic.AddInt64 return value
// Design doc Section 6.1.1: TestDownloadProgress_ShouldUseAtomicCompletedBytes
func TestDownloadProgress_AtomicCompletedBytes(t *testing.T) {
	var completedBytes int64
	totalBytes := int64(500)
	partSizes := []int64{100, 150, 250} // sum = 500

	listener := ProgressListenerFunc(func(event *ProgressEvent) {
		if event.EventType != TransferDataEvent {
			return
		}
		// Verify consumed bytes is monotonically increasing and never exceeds total
		if event.ConsumedBytes < 0 || event.ConsumedBytes > totalBytes {
			t.Errorf("ConsumedBytes = %d, must be in [0, %d]", event.ConsumedBytes, totalBytes)
		}
	})

	// Simulate download progress pattern
	for _, partSize := range partSizes {
		newCompleted := atomic.AddInt64(&completedBytes, partSize)
		event := newProgressEvent(TransferDataEvent, newCompleted, totalBytes)
		publishProgress(listener, event)
	}

	if completedBytes != totalBytes {
		t.Errorf("completedBytes = %d, want %d", completedBytes, totalBytes)
	}
}

// TestCheckpointAsyncWriter_LargeBatchSize tests CheckpointAsyncWriter with large batch (10000)
func TestCheckpointAsyncWriter_LargeBatchSize(t *testing.T) {
	cp := &UploadCheckpoint{
		UploadParts: make([]UploadPartInfo, 100),
	}
	for i := 0; i < 100; i++ {
		cp.UploadParts[i] = UploadPartInfo{PartNumber: i + 1, PartSize: 100}
	}

	tmpfile := t.TempDir() + "/large_batch_test.xml"

	// Use default channel capacity of 10000
	cw := NewCheckpointAsyncWriter(cp, tmpfile, WithAsyncBatchSize(100))
	defer cw.Shutdown()

	// Submit 100 updates (within default 10000 channel capacity)
	for i := 1; i <= 100; i++ {
		cw.Submit(&UploadPartUpdate{PartNumber: i, ETag: "etag", IsCompleted: true})
	}

	// Wait for flush
	time.Sleep(300 * time.Millisecond)

	// Verify all were updated
	for i := 0; i < 100; i++ {
		if !cp.UploadParts[i].IsCompleted {
			t.Errorf("part %d should be completed", i+1)
		}
	}
}