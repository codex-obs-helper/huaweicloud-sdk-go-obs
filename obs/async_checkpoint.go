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

// UploadPartUpdate represents an upload part update
type UploadPartUpdate struct {
	PartNumber  int
	ETag        string
	IsCompleted bool
}

func (u *UploadPartUpdate) Apply(target interface{}) {
	if cp, ok := target.(*UploadCheckpoint); ok {
		cp.UploadParts[u.PartNumber-1].Etag = u.ETag
		cp.UploadParts[u.PartNumber-1].IsCompleted = u.IsCompleted
	}
}

// DownloadPartUpdate represents a download part update
type DownloadPartUpdate struct {
	PartNumber  int // consistent with UploadPartUpdate
	IsCompleted bool
}

func (u *DownloadPartUpdate) Apply(target interface{}) {
	if cp, ok := target.(*DownloadCheckpoint); ok {
		cp.DownloadParts[u.PartNumber-1].IsCompleted = u.IsCompleted
	}
}

// CheckpointAsyncWriter combines AsyncWriter with checkpoint-specific logic
type CheckpointAsyncWriter struct {
	async  *AsyncWriter
	target interface{}
	path   string
}

func NewCheckpointAsyncWriter(target interface{}, path string, opts ...AsyncWriterOption) *CheckpointAsyncWriter {
	cw := &CheckpointAsyncWriter{
		target: target,
		path:   path,
	}

	flusher := &checkpointFlusher{
		target: target,
		path:   path,
	}

	// use channel=10000 by default, consistent with sliceFile limit
	cw.async = NewAsyncWriter(target, flusher,
		append([]AsyncWriterOption{WithChannelCapacity(10000)}, opts...)...)
	return cw
}

func (cw *CheckpointAsyncWriter) Submit(item AsyncItem) {
	cw.async.Submit(item)
}

func (cw *CheckpointAsyncWriter) Shutdown() {
	cw.async.Shutdown()
}

// SyncFlush forces a synchronous flush of all pending items to disk.
// Called before shutdown to ensure no data loss on crash.
func (cw *CheckpointAsyncWriter) SyncFlush() {
	cw.async.SyncFlush()
}

// checkpointFlusher implements AsyncFlusher interface
type checkpointFlusher struct {
	target interface{}
	path   string
}

func (cf *checkpointFlusher) Flush(items []AsyncItem) error {
	// apply all updates to target first
	for _, item := range items {
		item.Apply(cf.target)
	}
	// then write to file
	return updateCheckpointFile(cf.target, cf.path)
}
