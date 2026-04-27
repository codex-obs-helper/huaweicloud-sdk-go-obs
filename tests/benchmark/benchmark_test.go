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

package benchmark

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

// benchmarkFlusher mimics checkpoint flusher for benchmarking
type benchmarkFlusher struct {
	mu       sync.Mutex
	path     string
	flushes  int
	totalNum int
}

func (f *benchmarkFlusher) Flush(items []obs.AsyncItem) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.flushes++
	f.totalNum += len(items)
	// simulate file I/O
	if f.path != "" {
		_ = ioutil.WriteFile(f.path+".tmp", []byte("x"), 0644)
		_ = os.Rename(f.path+".tmp", f.path)
	}
	return nil
}

// testItem implements AsyncItem for benchmarking
type testItem struct {
	id int
}

func (t *testItem) Apply(target interface{}) {
	// no-op for benchmark
}

func BenchmarkAsyncWriter_Throughput(b *testing.B) {
	flusher := &benchmarkFlusher{}
	aw := obs.NewAsyncWriter(nil, flusher,
		obs.WithAsyncBatchSize(100),
		obs.WithAsyncFlushInterval(10*time.Second),
		obs.WithChannelCapacity(10000),
	)
	defer aw.Shutdown()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		aw.Submit(&testItem{id: i})
	}
	// ensure all items are flushed before measuring
	time.Sleep(200 * time.Millisecond)
}

func BenchmarkAsyncWriter_BatchSizes(b *testing.B) {
	batchSizes := []int{1, 5, 10, 20, 50, 100}

	for _, bs := range batchSizes {
		b.Run(string(rune(bs)), func(b *testing.B) {
			flusher := &benchmarkFlusher{}
			aw := obs.NewAsyncWriter(nil, flusher,
				obs.WithAsyncBatchSize(bs),
				obs.WithAsyncFlushInterval(10*time.Second),
				obs.WithChannelCapacity(10000),
			)
			defer aw.Shutdown()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				aw.Submit(&testItem{id: i})
			}
			time.Sleep(200 * time.Millisecond)
		})
	}
}

func BenchmarkAsyncWriter_FlushIntervals(b *testing.B) {
	intervals := []time.Duration{
		1 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
	}

	for _, interval := range intervals {
		b.Run(interval.String(), func(b *testing.B) {
			flusher := &benchmarkFlusher{}
			aw := obs.NewAsyncWriter(nil, flusher,
				obs.WithAsyncBatchSize(1000),
				obs.WithAsyncFlushInterval(interval),
				obs.WithChannelCapacity(10000),
			)
			defer aw.Shutdown()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				aw.Submit(&testItem{id: i})
			}
			time.Sleep(200 * time.Millisecond)
		})
	}
}

func BenchmarkCheckpointFlush_Sync(b *testing.B) {
	tmpDir := b.TempDir()
	cpPath := filepath.Join(tmpDir, "checkpoint.xml")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// simulate sync flush (what old code did)
		data := []byte("x")
		_ = ioutil.WriteFile(cpPath+".tmp", data, 0644)
		_ = os.Rename(cpPath+".tmp", cpPath)
	}
}

func BenchmarkCheckpointFlush_Async(b *testing.B) {
	tmpDir := b.TempDir()
	cpPath := filepath.Join(tmpDir, "checkpoint.xml")

	flusher := &benchmarkFlusher{path: cpPath}
	aw := obs.NewAsyncWriter(nil, flusher,
		obs.WithAsyncBatchSize(100),
		obs.WithAsyncFlushInterval(10*time.Second),
		obs.WithChannelCapacity(10000),
	)
	defer aw.Shutdown()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		aw.Submit(&testItem{id: i})
	}
	time.Sleep(200 * time.Millisecond)
}

func BenchmarkCheckpointFlush_Mixed(b *testing.B) {
	tmpDir := b.TempDir()
	cpPath := filepath.Join(tmpDir, "checkpoint.xml")

	flusher := &benchmarkFlusher{path: cpPath}
	aw := obs.NewAsyncWriter(nil, flusher,
		obs.WithAsyncBatchSize(5),  // small batch
		obs.WithAsyncFlushInterval(1*time.Second),
		obs.WithChannelCapacity(10000),
	)
	defer aw.Shutdown()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		aw.Submit(&testItem{id: i})
	}
	time.Sleep(200 * time.Millisecond)
}

func BenchmarkConcurrentCheckpointWrites(b *testing.B) {
	numWriters := 4
	itemsPerWriter := 1000

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	wg.Add(numWriters)

	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			defer wg.Done()
			flusher := &benchmarkFlusher{}
			aw := obs.NewAsyncWriter(nil, flusher,
				obs.WithAsyncBatchSize(50),
				obs.WithAsyncFlushInterval(10*time.Second),
				obs.WithChannelCapacity(5000),
			)
			defer aw.Shutdown()

			for i := 0; i < itemsPerWriter; i++ {
				aw.Submit(&testItem{id: writerID*10000 + i})
			}
		}(w)
	}

	wg.Wait()
	time.Sleep(200 * time.Millisecond)
}

func BenchmarkLargeScale(b *testing.B) {
	tmpDir := b.TempDir()
	cpPath := filepath.Join(tmpDir, "large_scale.xml")

	flusher := &benchmarkFlusher{path: cpPath}
	aw := obs.NewAsyncWriter(nil, flusher,
		obs.WithAsyncBatchSize(10),
		obs.WithAsyncFlushInterval(2*time.Second),
		obs.WithChannelCapacity(10000),
	)
	defer aw.Shutdown()

	// simulate 10000 parts (large scale upload)
	parts := 10000

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < parts; i++ {
		aw.Submit(&testItem{id: i})
	}

	// wait for all flushes to complete
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		flusher.mu.Lock()
		total := flusher.totalNum
		flusher.mu.Unlock()
		if total >= parts {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	b.StopTimer()
}
