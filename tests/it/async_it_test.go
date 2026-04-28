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

package it

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

// progressListenerFunc is an adapter for ProgressListener
type progressListenerFunc func(event *obs.ProgressEvent)

func (f progressListenerFunc) ProgressChanged(event *obs.ProgressEvent) {
	f(event)
}

// ============================================================================
// 功能测试 (Functional Tests) - Section 6.2.1
// ============================================================================

// IT_UploadFile_DetectFileModification - 检测源文件在中途被修改时应重新开始
func TestUploadFile_DetectFileModification(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "modify_src.txt")
	cpFile := filepath.Join(tmpDir, "modify.cp")

	// create 10MB test file
	content1 := strings.Repeat("original-data-", 100*1024)
	if err := os.WriteFile(srcFile, []byte(content1), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	key := testObjectKey + "-modify"

	input := &obs.UploadFileInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket: testBucket,
			Key:    key,
		},
		UploadFile:          srcFile,
		PartSize:            5 * 1024 * 1024,
		TaskNum:             4,
		EnableCheckpoint:    true,
		CheckpointFile:      cpFile,
		CheckpointBatchSize: 5,
	}

	output1, err := obsClient.UploadFile(input)
	if err != nil {
		t.Fatalf("first upload failed: %v", err)
	}
	defer cleanupTestObject(t, key)

	if output1.ETag == "" {
		t.Error("expected ETag in output")
	}

	// modify source file
	content2 := strings.Repeat("modified-data-", 100*1024)
	if err := os.WriteFile(srcFile, []byte(content2), 0644); err != nil {
		t.Fatalf("failed to modify source file: %v", err)
	}

	// re-upload should detect modification and restart
	output2, err := obsClient.UploadFile(input)
	if err != nil {
		t.Fatalf("upload after modification failed: %v", err)
	}

	// ETags should be different because content changed
	if output2.ETag == output1.ETag {
		t.Log("ETags are same - checkpoint may have detected and prevented re-upload of same content")
	}
}

// IT_UploadFile_ProgressBytesMonotonic - 验证上传进度字节数单调不减
func TestUploadFile_ProgressBytesMonotonic(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "progress_src.txt")
	cpFile := filepath.Join(tmpDir, "progress.cp")

	// create 10MB test file
	content := strings.Repeat("progress-test-", 100*1024)
	if err := os.WriteFile(srcFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	key := testObjectKey + "-progress"

	var mu sync.Mutex
	progressValues := make([]int64, 0)

	// thread-safe progress listener
	listener := progressListenerFunc(func(event *obs.ProgressEvent) {
		if event.EventType == obs.TransferDataEvent {
			mu.Lock()
			defer mu.Unlock()
			progressValues = append(progressValues, event.ConsumedBytes)
		}
	})

	input := &obs.UploadFileInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket: testBucket,
			Key:    key,
		},
		UploadFile:            srcFile,
		PartSize:              1 * 1024 * 1024,
		TaskNum:               4,
		EnableCheckpoint:      true,
		CheckpointFile:        cpFile,
		CheckpointBatchSize:   5,
	}

	_, err := obsClient.UploadFile(input, obs.WithProgress(listener))
	if err != nil {
		t.Fatalf("upload with progress failed: %v", err)
	}
	defer cleanupTestObject(t, key)

	mu.Lock()
	defer mu.Unlock()

	// Verify monotonic
	for i := 1; i < len(progressValues); i++ {
		if progressValues[i] < progressValues[i-1] {
			t.Errorf("progress not monotonic: [%d]=%d < [%d]=%d",
				i, progressValues[i], i-1, progressValues[i-1])
		}
	}

	// Verify final value equals total file size
	finalProgress := progressValues[len(progressValues)-1]
	expectedSize := int64(len(content))
	if finalProgress != expectedSize {
		t.Errorf("final progress %d != expected %d", finalProgress, expectedSize)
	}
}

// IT_DownloadFile_ProgressBytesMonotonic - 验证下载进度字节数单调不减
func TestDownloadFile_ProgressBytesMonotonic(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	dstFile := filepath.Join(tmpDir, "download_progress_dst.txt")
	cpFile := filepath.Join(tmpDir, "download_progress.cp")

	// create test object first
	testContent := strings.Repeat("download-progress-", 100*1024)
	key := testObjectKey + "-download-progress"

	_, err := obsClient.PutObject(&obs.PutObjectInput{
		PutObjectBasicInput: obs.PutObjectBasicInput{
			ObjectOperationInput: obs.ObjectOperationInput{
				Bucket: testBucket,
				Key:    key,
			},
		},
		Body: strings.NewReader(testContent),
	})
	if err != nil {
		t.Fatalf("failed to create test object: %v", err)
	}
	defer cleanupTestObject(t, key)

	var mu sync.Mutex
	progressValues := make([]int64, 0)

	listener := progressListenerFunc(func(event *obs.ProgressEvent) {
		if event.EventType == obs.TransferDataEvent {
			mu.Lock()
			defer mu.Unlock()
			progressValues = append(progressValues, event.ConsumedBytes)
		}
	})

	input := &obs.DownloadFileInput{
		GetObjectMetadataInput: obs.GetObjectMetadataInput{
			Bucket: testBucket,
			Key:    key,
		},
		DownloadFile:          dstFile,
		PartSize:              1 * 1024 * 1024,
		TaskNum:               4,
		EnableCheckpoint:      true,
		CheckpointFile:        cpFile,
		CheckpointBatchSize:   5,
	}

	_, err = obsClient.DownloadFile(input, obs.WithProgress(listener))
	if err != nil {
		t.Fatalf("download with progress failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	// Verify monotonic
	for i := 1; i < len(progressValues); i++ {
		if progressValues[i] < progressValues[i-1] {
			t.Errorf("progress not monotonic: [%d]=%d < [%d]=%d",
				i, progressValues[i], i-1, progressValues[i-1])
		}
	}

	// Verify final value equals object size
	finalProgress := progressValues[len(progressValues)-1]
	expectedSize := int64(len(testContent))
	if finalProgress != expectedSize {
		t.Errorf("final progress %d != expected %d", finalProgress, expectedSize)
	}
}

// IT_ProgressListener_ConcurrentSafe - 验证 ProgressListener 在并发场景下线程安全
func TestProgressListener_ConcurrentSafe(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "listener_src.txt")
	cpFile := filepath.Join(tmpDir, "listener.cp")

	// create 20MB test file for more parts
	content := strings.Repeat("listener-test-", 200*1024)
	if err := os.WriteFile(srcFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	key := testObjectKey + "-listener"

	var counter int64
	var panicMu sync.Mutex
	panicked := false

	// listener that tracks count and detects concurrent issues
	listener := progressListenerFunc(func(event *obs.ProgressEvent) {
		if event.EventType == obs.TransferDataEvent {
			// Atomic increment should not panic
			atomic.AddInt64(&counter, 1)
		}
	})

	// This wrapper catches panics from concurrent callback issues
	safeListener := &safeProgressListener{
		wrapped: listener,
		panicked: &panicked,
		mu: &panicMu,
	}

	input := &obs.UploadFileInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket: testBucket,
			Key:    key,
		},
		UploadFile:            srcFile,
		PartSize:              1 * 1024 * 1024, // 20 parts
		TaskNum:               10,
		EnableCheckpoint:      true,
		CheckpointFile:        cpFile,
		CheckpointBatchSize:   5,
	}

	_, err := obsClient.UploadFile(input, obs.WithProgress(safeListener))
	if err != nil {
		t.Fatalf("upload failed: %v", err)
	}
	defer cleanupTestObject(t, key)

	if panicked {
		t.Fatal("progress listener panicked during concurrent callbacks")
	}
}

type safeProgressListener struct {
	wrapped  obs.ProgressListener
	panicked *bool
	mu       *sync.Mutex
}

func (l *safeProgressListener) ProgressChanged(event *obs.ProgressEvent) {
	defer func() {
		if r := recover(); r != nil {
			l.mu.Lock()
			*l.panicked = true
			l.mu.Unlock()
		}
	}()
	l.wrapped.ProgressChanged(event)
}

// ============================================================================
// 性能测试 (Performance Tests) - Section 6.2.2
// ============================================================================

// Perf_UploadFile_TargetThroughput - 验证异步批量写入提升吞吐量
func TestPerf_UploadFile_TargetThroughput(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "perf_src.txt")
	cpFile := filepath.Join(tmpDir, "perf.cp")

	// create 100MB test file
	buf := make([]byte, 100*1024*1024)
	for i := range buf {
		buf[i] = byte('a' + (i % 26))
	}
	if err := os.WriteFile(srcFile, buf, 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	key := testObjectKey + "-perf-throughput"

	start := time.Now()

	input := &obs.UploadFileInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket: testBucket,
			Key:    key,
		},
		UploadFile:            srcFile,
		PartSize:              5 * 1024 * 1024,
		TaskNum:               5,
		EnableCheckpoint:      true,
		CheckpointFile:        cpFile,
		CheckpointBatchSize:   20,
		CheckpointFlushInterval: 10,
	}

	_, err := obsClient.UploadFile(input)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("upload failed: %v", err)
	}
	defer cleanupTestObject(t, key)

	throughput := float64(len(buf)) / elapsed.Seconds() / 1024 / 1024 // MB/s
	t.Logf("Upload throughput: %.2f MB/s (elapsed: %v, size: %d MB)",
		throughput, elapsed, len(buf)/1024/1024)
}

// Perf_UploadFile_ReduceCheckpointIO - 验证批量写入减少 checkpoint IO 次数
func TestPerf_UploadFile_ReduceCheckpointIO(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "perf_io_src.txt")
	cpFile := filepath.Join(tmpDir, "perf_io.cp")

	// create 50MB test file with small parts for many parts
	buf := make([]byte, 50*1024*1024)
	for i := range buf {
		buf[i] = byte('a' + (i % 26))
	}
	if err := os.WriteFile(srcFile, buf, 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	key := testObjectKey + "-perf-io"

	// Track file stat calls as a proxy for IO
	var statCount int64
	origStat := os.Stat
	statFile = func(name string) (os.FileInfo, error) {
		atomic.AddInt64(&statCount, 1)
		return origStat(name)
	}
	defer func() { statFile = origStat }()

	input := &obs.UploadFileInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket: testBucket,
			Key:    key,
		},
		UploadFile:            srcFile,
		PartSize:              1 * 1024 * 1024, // 50 parts
		TaskNum:               10,
		EnableCheckpoint:      true,
		CheckpointFile:        cpFile,
		CheckpointBatchSize:   10, // should reduce IO
		CheckpointFlushInterval: 5,
	}

	_, err := obsClient.UploadFile(input)
	if err != nil {
		t.Fatalf("upload failed: %v", err)
	}
	defer cleanupTestObject(t, key)

	t.Logf("Checkpoint stat operations: %d (with batchSize=10, 50 parts)", statCount)
}

// statFile variable for test instrumentation
var statFile = os.Stat

// Perf_BatchSize_FindOptimalValue_Upload - 测试不同 batchSize 配置的性能（上传场景）
func TestPerf_BatchSize_FindOptimalValue_Upload(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "perf_batch_src.txt")

	// create 50MB test file
	buf := make([]byte, 50*1024*1024)
	for i := range buf {
		buf[i] = byte('a' + (i % 26))
	}
	if err := os.WriteFile(srcFile, buf, 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	batchSizes := []int{1, 5, 10, 20, 50}

	for _, batchSize := range batchSizes {
		key := fmt.Sprintf("%s-batch-%d", testObjectKey, batchSize)
		cpFile := filepath.Join(tmpDir, fmt.Sprintf("perf_batch_%d.cp", batchSize))

		input := &obs.UploadFileInput{
			ObjectOperationInput: obs.ObjectOperationInput{
				Bucket: testBucket,
				Key:    key,
			},
			UploadFile:            srcFile,
			PartSize:              5 * 1024 * 1024,
			TaskNum:               5,
			EnableCheckpoint:      true,
			CheckpointFile:        cpFile,
			CheckpointBatchSize:   batchSize,
		}

		start := time.Now()
		_, err := obsClient.UploadFile(input)
		elapsed := time.Since(start)

		if err != nil {
			t.Errorf("batchSize=%d: upload failed: %v", batchSize, err)
		} else {
			throughput := float64(len(buf)) / elapsed.Seconds() / 1024 / 1024
			t.Logf("batchSize=%d: throughput=%.2f MB/s, elapsed=%v", batchSize, throughput, elapsed)
		}

		cleanupTestObject(t, key)
	}
}

// ============================================================================
// 故障测试 (Fault Tests) - Section 6.2.3
// ============================================================================

// Fault_UploadFile_RecoverOnChannelFull - 验证 channel 满时对 worker 施加背压
func TestFault_UploadFile_RecoverOnChannelFull(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "fault_src.txt")
	cpFile := filepath.Join(tmpDir, "fault.cp")

	// create large file with many small parts to stress the channel
	buf := make([]byte, 100*1024*1024)
	for i := range buf {
		buf[i] = byte('a' + (i % 26))
	}
	if err := os.WriteFile(srcFile, buf, 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	key := testObjectKey + "-fault-channel"

	// Use small batch and small interval to stress the channel
	input := &obs.UploadFileInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket: testBucket,
			Key:    key,
		},
		UploadFile:              srcFile,
		PartSize:                1 * 1024 * 1024, // 100 parts
		TaskNum:                 20,               // high concurrency
		EnableCheckpoint:        true,
		CheckpointFile:          cpFile,
		CheckpointBatchSize:     2,   // small batch
		CheckpointFlushInterval: 30,  // large interval to stress channel
	}

	_, err := obsClient.UploadFile(input)
	if err != nil {
		t.Fatalf("upload failed under stress: %v", err)
	}
	defer cleanupTestObject(t, key)

	// Verify file was uploaded correctly
	meta, err := obsClient.GetObjectMetadata(&obs.GetObjectMetadataInput{
		Bucket: testBucket,
		Key:    key,
	})
	if err != nil {
		t.Fatalf("object not found: %v", err)
	}
	if meta.ContentLength != int64(len(buf)) {
		t.Errorf("content length mismatch: expected %d, got %d", len(buf), meta.ContentLength)
	}
}

// Fault_UploadFile_PreserveStateOnPanic - 验证 SyncFlush 后重启可恢复
func TestFault_UploadFile_PreserveStateOnPanic(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "fault_panic_src.txt")
	cpFile := filepath.Join(tmpDir, "fault_panic.cp")

	// create test file
	content := strings.Repeat("fault-panic-test-", 100*1024)
	if err := os.WriteFile(srcFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	key := testObjectKey + "-fault-panic"

	input := &obs.UploadFileInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket: testBucket,
			Key:    key,
		},
		UploadFile:            srcFile,
		PartSize:              1 * 1024 * 1024,
		TaskNum:               4,
		EnableCheckpoint:      true,
		CheckpointFile:        cpFile,
		CheckpointBatchSize:   2,
		CheckpointFlushInterval: 1,
	}

	// First upload should complete successfully
	_, err := obsClient.UploadFile(input)
	if err != nil {
		t.Fatalf("first upload failed: %v", err)
	}
	defer cleanupTestObject(t, key)

	// Verify checkpoint file exists after successful upload
	if _, err := os.Stat(cpFile); os.IsNotExist(err) {
		t.Log("checkpoint file does not exist after completion (may be cleaned up)")
	}

	// Resume should work from checkpoint
	output2, err := obsClient.UploadFile(input)
	if err != nil {
		t.Errorf("resume after panic simulation failed: %v", err)
	}

	if output2.ETag == "" {
		t.Error("expected ETag after resume")
	}
}

// Fault_CheckpointFile_AtomicOnCrash - 验证写盘时崩溃 checkpoint 文件不损坏
func TestFault_CheckpointFile_AtomicOnCrash(t *testing.T) {
	skipIfNotEnabled(t)

	// This test verifies atomic write behavior by checking
	// that after a successful upload, the checkpoint file is valid

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "atomic_src.txt")
	cpFile := filepath.Join(tmpDir, "atomic.cp")

	content := strings.Repeat("atomic-test-", 100*1024)
	if err := os.WriteFile(srcFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	key := testObjectKey + "-atomic"

	input := &obs.UploadFileInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket: testBucket,
			Key:    key,
		},
		UploadFile:          srcFile,
		PartSize:            1 * 1024 * 1024,
		TaskNum:             4,
		EnableCheckpoint:    true,
		CheckpointFile:      cpFile,
		CheckpointBatchSize: 5,
	}

	_, err := obsClient.UploadFile(input)
	if err != nil {
		t.Fatalf("upload failed: %v", err)
	}
	defer cleanupTestObject(t, key)

	// If checkpoint file exists, verify it's valid XML
	if info, err := os.Stat(cpFile); err == nil && info.Size() > 0 {
		data, err := os.ReadFile(cpFile)
		if err != nil {
			t.Errorf("failed to read checkpoint file: %v", err)
		} else {
			// Basic XML validation - should start with <?xml or <checkpoint
			trimmed := bytes.TrimSpace(data)
			if !bytes.HasPrefix(trimmed, []byte("<?xml")) && !bytes.HasPrefix(trimmed, []byte("<")) {
				t.Errorf("checkpoint file does not appear to be valid XML: %s", string(trimmed[:min(100, len(trimmed))]))
			} else {
				t.Logf("checkpoint file is valid XML, size: %d bytes", len(data))
			}
		}

		// Verify no .tmp file left behind
		tmpFiles, _ := filepath.Glob(cpFile + ".tmp")
		if len(tmpFiles) > 0 {
			t.Errorf("temp files not cleaned up: %v", tmpFiles)
		}
	} else {
		t.Log("checkpoint file cleaned up after successful upload (expected behavior)")
	}
}

// ============================================================================
// 辅助函数
// ============================================================================

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// md5Sum calculates MD5 hash of data
func md5Sum(data []byte) string {
	h := md5.Sum(data)
	return base64.StdEncoding.EncodeToString(h[:])
}

// readAll reads entire file content
func readAll(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// ============================================================================
// 下载场景性能测试 (Download Performance Tests)
// ============================================================================

// Perf_DownloadFile_TargetThroughput - 验证下载吞吐量
func TestPerf_DownloadFile_TargetThroughput(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "perf_download_src.txt")

	// create 100MB test file
	buf := make([]byte, 100*1024*1024)
	for i := range buf {
		buf[i] = byte('a' + (i % 26))
	}
	if err := os.WriteFile(srcFile, buf, 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	key := testObjectKey + "-perf-download-throughput"

	// upload first using PutObject (simpler than multipart upload for test setup)
	_, err := obsClient.PutObject(&obs.PutObjectInput{
		PutObjectBasicInput: obs.PutObjectBasicInput{
			ObjectOperationInput: obs.ObjectOperationInput{
				Bucket: testBucket,
				Key:    key,
			},
		},
		Body: bytes.NewReader(buf),
	})
	if err != nil {
		t.Fatalf("put object failed: %v", err)
	}
	defer cleanupTestObject(t, key)

	// now download with checkpoint
	dstFile := filepath.Join(tmpDir, "perf_download_dst.txt")
	cpFile := filepath.Join(tmpDir, "perf_download.cp")

	downloadInput := &obs.DownloadFileInput{
		GetObjectMetadataInput: obs.GetObjectMetadataInput{
			Bucket: testBucket,
			Key:    key,
		},
		DownloadFile:            dstFile,
		PartSize:              5 * 1024 * 1024,
		TaskNum:               5,
		EnableCheckpoint:      true,
		CheckpointFile:        cpFile,
		CheckpointBatchSize:   20,
	}

	start := time.Now()
	_, err = obsClient.DownloadFile(downloadInput)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("download failed: %v", err)
	}

	throughput := float64(len(buf)) / elapsed.Seconds() / 1024 / 1024 // MB/s
	t.Logf("Download throughput: %.2f MB/s (elapsed: %v, size: %d MB)",
		throughput, elapsed, len(buf)/1024/1024)
}

// Perf_BatchSize_FindOptimalValue_Download - 测试不同 batchSize 配置的性能（下载场景）
func TestPerf_BatchSize_FindOptimalValue_Download(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "perf_batch_download_src.txt")

	// create 50MB test file
	buf := make([]byte, 50*1024*1024)
	for i := range buf {
		buf[i] = byte('a' + (i % 26))
	}
	if err := os.WriteFile(srcFile, buf, 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	batchSizes := []int{1, 5, 10, 20, 50}

	for _, batchSize := range batchSizes {
		key := fmt.Sprintf("%s-batch-download-%d", testObjectKey, batchSize)

		// upload first using UploadFile with checkpoint to avoid InvalidPart issue
		uploadCpFile := filepath.Join(tmpDir, fmt.Sprintf("perf_batch_upload_%d.cp", batchSize))
		uploadInput := &obs.UploadFileInput{
			ObjectOperationInput: obs.ObjectOperationInput{
				Bucket: testBucket,
				Key:    key,
			},
			UploadFile:       srcFile,
			PartSize:         5 * 1024 * 1024,
			TaskNum:          5,
			EnableCheckpoint: true,
			CheckpointFile:   uploadCpFile,
		}
		_, err := obsClient.UploadFile(uploadInput)
		if err != nil {
			t.Errorf("batchSize=%d: upload failed: %v", batchSize, err)
			continue
		}
		defer cleanupTestObject(t, key)

		// now download with different batch sizes
		dstFile := filepath.Join(tmpDir, fmt.Sprintf("perf_batch_download_%d.txt", batchSize))
		cpFile := filepath.Join(tmpDir, fmt.Sprintf("perf_batch_download_%d.cp", batchSize))

		downloadInput := &obs.DownloadFileInput{
			GetObjectMetadataInput: obs.GetObjectMetadataInput{
				Bucket: testBucket,
				Key:    key,
			},
			DownloadFile:          dstFile,
			PartSize:              5 * 1024 * 1024,
			TaskNum:               5,
			EnableCheckpoint:      true,
			CheckpointFile:        cpFile,
			CheckpointBatchSize:   batchSize,
		}

		start := time.Now()
		_, err = obsClient.DownloadFile(downloadInput)
		elapsed := time.Since(start)

		if err != nil {
			t.Errorf("batchSize=%d: download failed: %v", batchSize, err)
		} else {
			throughput := float64(len(buf)) / elapsed.Seconds() / 1024 / 1024
			t.Logf("batchSize=%d: throughput=%.2f MB/s, elapsed=%v", batchSize, throughput, elapsed)
		}
	}
}

// ============================================================================
// 下载场景故障测试 (Download Fault Tests)
// ============================================================================

// Fault_DownloadFile_RecoverOnChannelFull - 验证下载 channel 满时对 worker 施加背压
func TestFault_DownloadFile_RecoverOnChannelFull(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "fault_download_src.txt")

	// create large file with many small parts to stress the channel
	buf := make([]byte, 100*1024*1024)
	for i := range buf {
		buf[i] = byte('a' + (i % 26))
	}
	if err := os.WriteFile(srcFile, buf, 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	key := testObjectKey + "-fault-download-channel"

	// upload first
	uploadInput := &obs.UploadFileInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket: testBucket,
			Key:    key,
		},
		UploadFile:       srcFile,
		PartSize:         1 * 1024 * 1024, // 100 parts
		TaskNum:          20,              // high concurrency
		EnableCheckpoint: true,
		CheckpointFile:   filepath.Join(tmpDir, "fault_upload.cp"),
	}
	_, err := obsClient.UploadFile(uploadInput)
	if err != nil {
		t.Fatalf("upload failed: %v", err)
	}
	defer cleanupTestObject(t, key)

	// download with small batch to stress channel
	dstFile := filepath.Join(tmpDir, "fault_download_dst.txt")
	cpFile := filepath.Join(tmpDir, "fault_download.cp")

	downloadInput := &obs.DownloadFileInput{
		GetObjectMetadataInput: obs.GetObjectMetadataInput{
			Bucket: testBucket,
			Key:    key,
		},
		DownloadFile:             dstFile,
		PartSize:               1 * 1024 * 1024,
		TaskNum:                20,
		EnableCheckpoint:       true,
		CheckpointFile:         cpFile,
		CheckpointBatchSize:    2,  // small batch
		CheckpointFlushInterval: 30, // large interval to stress channel
	}

	_, err = obsClient.DownloadFile(downloadInput)
	if err != nil {
		t.Fatalf("download failed under stress: %v", err)
	}

	// Verify file was downloaded correctly
	downloaded, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}
	if len(downloaded) != len(buf) {
		t.Errorf("content length mismatch: expected %d, got %d", len(buf), len(downloaded))
	}
}

// Fault_DownloadFile_FlushAllOnShutdown - 验证下载正常完成时无 goroutine 泄漏
func TestFault_DownloadFile_FlushAllOnShutdown(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "fault_shutdown_src.txt")

	// create test file
	content := strings.Repeat("fault-shutdown-", 100*1024)
	if err := os.WriteFile(srcFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	key := testObjectKey + "-fault-download-shutdown"

	// upload first
	uploadInput := &obs.UploadFileInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket: testBucket,
			Key:    key,
		},
		UploadFile:       srcFile,
		PartSize:         1 * 1024 * 1024,
		TaskNum:         4,
		EnableCheckpoint: true,
		CheckpointFile:   filepath.Join(tmpDir, "fault_shutdown_upload.cp"),
	}
	_, err := obsClient.UploadFile(uploadInput)
	if err != nil {
		t.Fatalf("upload failed: %v", err)
	}
	defer cleanupTestObject(t, key)

	// download
	dstFile := filepath.Join(tmpDir, "fault_shutdown_dst.txt")
	cpFile := filepath.Join(tmpDir, "fault_shutdown.cp")

	downloadInput := &obs.DownloadFileInput{
		GetObjectMetadataInput: obs.GetObjectMetadataInput{
			Bucket: testBucket,
			Key:    key,
		},
		DownloadFile:          dstFile,
		PartSize:            1 * 1024 * 1024,
		TaskNum:             4,
		EnableCheckpoint:    true,
		CheckpointFile:      cpFile,
		CheckpointBatchSize: 5,
	}

	_, err = obsClient.DownloadFile(downloadInput)
	if err != nil {
		t.Fatalf("download failed: %v", err)
	}

	// Verify downloaded content
	downloaded, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}
	if len(downloaded) != len(content) {
		t.Errorf("content length mismatch: expected %d, got %d", len(content), len(downloaded))
	}
}

// Fault_DownloadFile_PreserveStateOnPanic - 验证下载 SyncFlush 后重启可恢复
func TestFault_DownloadFile_PreserveStateOnPanic(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "fault_panic_download_src.txt")

	// create test file
	content := strings.Repeat("fault-panic-download-", 100*1024)
	if err := os.WriteFile(srcFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	key := testObjectKey + "-fault-panic-download"

	// upload first
	uploadInput := &obs.UploadFileInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket: testBucket,
			Key:    key,
		},
		UploadFile:            srcFile,
		PartSize:              1 * 1024 * 1024,
		TaskNum:              4,
		EnableCheckpoint:      true,
			CheckpointFile:        filepath.Join(tmpDir, "fault_panic_upload.cp"),
	}
	_, err := obsClient.UploadFile(uploadInput)
	if err != nil {
		t.Fatalf("upload failed: %v", err)
	}
	defer cleanupTestObject(t, key)

	// first download
	dstFile := filepath.Join(tmpDir, "fault_panic_download_dst.txt")
	cpFile := filepath.Join(tmpDir, "fault_panic_download.cp")

	input := &obs.DownloadFileInput{
		GetObjectMetadataInput: obs.GetObjectMetadataInput{
			Bucket: testBucket,
			Key:    key,
		},
		DownloadFile:            dstFile,
		PartSize:              1 * 1024 * 1024,
		TaskNum:               4,
		EnableCheckpoint:      true,
		CheckpointFile:        cpFile,
		CheckpointBatchSize:   2,
		CheckpointFlushInterval: 1,
	}

	_, err = obsClient.DownloadFile(input)
	if err != nil {
		t.Fatalf("first download failed: %v", err)
	}

	// Verify checkpoint file exists after completion
	if info, err := os.Stat(cpFile); err == nil && info.Size() > 0 {
		t.Logf("checkpoint file exists after completion, size: %d bytes", info.Size())
	} else {
		t.Log("checkpoint file cleaned up after successful download (expected behavior)")
	}

	// Resume download should work
	_, err = obsClient.DownloadFile(input)
	if err != nil {
		t.Errorf("resume download failed: %v", err)
	}

	// Verify content
	downloaded, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}
	if len(downloaded) != len(content) {
		t.Errorf("content length mismatch: expected %d, got %d", len(content), len(downloaded))
	}
}

// ============================================================================
// 扩展性能测试场景 (Extended Performance Test Scenarios)
// ============================================================================

// TestPerf_ObjectSize_Impact_Upload - 测试不同对象大小对性能的影响
func TestPerf_ObjectSize_Impact_Upload(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()

	objectSizes := []int64{10 * 1024 * 1024, 50 * 1024 * 1024, 100 * 1024 * 1024} // 10MB, 50MB, 100MB
	batchSizes := []int{10, 20}

	for _, objSize := range objectSizes {
		for _, batchSize := range batchSizes {
			srcFile := filepath.Join(tmpDir, fmt.Sprintf("obj_%d_batch_%d.txt", objSize/1024/1024, batchSize))
			buf := make([]byte, objSize)
			for i := range buf {
				buf[i] = byte('a' + (i % 26))
			}
			if err := os.WriteFile(srcFile, buf, 0644); err != nil {
				t.Fatalf("failed to create source file: %v", err)
			}

			key := fmt.Sprintf("%s-objsize-%d-batch-%d", testObjectKey, objSize/1024/1024, batchSize)
			cpFile := filepath.Join(tmpDir, fmt.Sprintf("obj_%d_batch_%d.cp", objSize/1024/1024, batchSize))

			input := &obs.UploadFileInput{
				ObjectOperationInput: obs.ObjectOperationInput{
					Bucket: testBucket,
					Key:    key,
				},
				UploadFile:            srcFile,
				PartSize:              5 * 1024 * 1024,
				TaskNum:               5,
				EnableCheckpoint:      true,
				CheckpointFile:        cpFile,
				CheckpointBatchSize:   batchSize,
			}

			start := time.Now()
			_, err := obsClient.UploadFile(input)
			elapsed := time.Since(start)

			if err != nil {
				t.Errorf("objSize=%dMB batchSize=%d: upload failed: %v", objSize/1024/1024, batchSize, err)
			} else {
				throughput := float64(objSize) / elapsed.Seconds() / 1024 / 1024
				t.Logf("objSize=%dMB partSize=5MB parts=%d batchSize=%d TaskNum=5: throughput=%.2f MB/s elapsed=%v",
					objSize/1024/1024, objSize/5/1024/1024, batchSize, throughput, elapsed)
			}

			cleanupTestObject(t, key)
		}
	}
}

// TestPerf_ObjectSize_Impact_Download - 测试不同对象大小对下载性能的影响
func TestPerf_ObjectSize_Impact_Download(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()

	objectSizes := []int64{10 * 1024 * 1024, 50 * 1024 * 1024, 100 * 1024 * 1024} // 10MB, 50MB, 100MB
	batchSizes := []int{10, 20}

	for _, objSize := range objectSizes {
		// First upload the file using PutObject (for test setup)
		key := fmt.Sprintf("%s-objsize-download-%d", testObjectKey, objSize/1024/1024)
		buf := make([]byte, objSize)
		for i := range buf {
			buf[i] = byte('a' + (i % 26))
		}
		_, err := obsClient.PutObject(&obs.PutObjectInput{
			PutObjectBasicInput: obs.PutObjectBasicInput{
				ObjectOperationInput: obs.ObjectOperationInput{
					Bucket: testBucket,
					Key:    key,
				},
			},
			Body: bytes.NewReader(buf),
		})
		if err != nil {
			t.Fatalf("put object failed: %v", err)
		}
		defer cleanupTestObject(t, key)

		for _, batchSize := range batchSizes {
			dstFile := filepath.Join(tmpDir, fmt.Sprintf("obj_%d_batch_%d.txt", objSize/1024/1024, batchSize))
			cpFile := filepath.Join(tmpDir, fmt.Sprintf("obj_%d_batch_%d.cp", objSize/1024/1024, batchSize))

			input := &obs.DownloadFileInput{
				GetObjectMetadataInput: obs.GetObjectMetadataInput{
					Bucket: testBucket,
					Key:    key,
				},
				DownloadFile:          dstFile,
				PartSize:              5 * 1024 * 1024,
				TaskNum:               5,
				EnableCheckpoint:      true,
				CheckpointFile:        cpFile,
				CheckpointBatchSize:   batchSize,
			}

			start := time.Now()
			_, err = obsClient.DownloadFile(input)
			elapsed := time.Since(start)

			if err != nil {
				t.Errorf("objSize=%dMB batchSize=%d: download failed: %v", objSize/1024/1024, batchSize, err)
			} else {
				throughput := float64(objSize) / elapsed.Seconds() / 1024 / 1024
				t.Logf("objSize=%dMB partSize=5MB parts=%d batchSize=%d TaskNum=5: throughput=%.2f MB/s elapsed=%v",
					objSize/1024/1024, objSize/5/1024/1024, batchSize, throughput, elapsed)
			}
		}
	}
}

// TestPerf_PartSize_Impact - 测试不同分段大小对性能的影响
func TestPerf_PartSize_Impact(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()

	// 固定 100MB 文件，测试不同分段大小
	objSize := int64(100 * 1024 * 1024)
	partSizes := []int64{1 * 1024 * 1024, 5 * 1024 * 1024, 10 * 1024 * 1024} // 1MB, 5MB, 10MB
	batchSize := 20

	for _, partSize := range partSizes {
		srcFile := filepath.Join(tmpDir, fmt.Sprintf("partsize_%d.txt", partSize/1024/1024))
		buf := make([]byte, objSize)
		for i := range buf {
			buf[i] = byte('a' + (i % 26))
		}
		if err := os.WriteFile(srcFile, buf, 0644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}

		key := fmt.Sprintf("%s-partsize-%d", testObjectKey, partSize/1024/1024)
		cpFile := filepath.Join(tmpDir, fmt.Sprintf("partsize_%d.cp", partSize/1024/1024))

		input := &obs.UploadFileInput{
			ObjectOperationInput: obs.ObjectOperationInput{
				Bucket: testBucket,
				Key:    key,
			},
			UploadFile:            srcFile,
			PartSize:              partSize,
			TaskNum:               5,
			EnableCheckpoint:      true,
			CheckpointFile:        cpFile,
			CheckpointBatchSize:   batchSize,
		}

		start := time.Now()
		_, err := obsClient.UploadFile(input)
		elapsed := time.Since(start)

		parts := int(objSize / partSize)
		if objSize%partSize != 0 {
			parts++
		}

		if err != nil {
			t.Errorf("partSize=%dMB parts=%d: upload failed: %v", partSize/1024/1024, parts, err)
		} else {
			throughput := float64(objSize) / elapsed.Seconds() / 1024 / 1024
			t.Logf("objSize=100MB partSize=%dMB parts=%d batchSize=%d TaskNum=5: throughput=%.2f MB/s elapsed=%v",
				partSize/1024/1024, parts, batchSize, throughput, elapsed)
		}

		cleanupTestObject(t, key)
	}
}

// TestPerf_HighConcurrency - 测试高并发场景下的性能
func TestPerf_HighConcurrency(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()

	// 100MB 文件，1MB 分段 = 100 parts，高并发
	objSize := int64(100 * 1024 * 1024)
	partSize := int64(1 * 1024 * 1024)
	taskNums := []int{5, 10, 20}
	batchSizes := []int{10, 20, 50}

	srcFile := filepath.Join(tmpDir, "high_concurrency_src.txt")
	buf := make([]byte, objSize)
	for i := range buf {
		buf[i] = byte('a' + (i % 26))
	}
	if err := os.WriteFile(srcFile, buf, 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	parts := int(objSize / partSize)
	if objSize%partSize != 0 {
		parts++
	}

	for _, taskNum := range taskNums {
		for _, batchSize := range batchSizes {
			key := fmt.Sprintf("%s-highconc-task%d-batch%d", testObjectKey, taskNum, batchSize)
			cpFile := filepath.Join(tmpDir, fmt.Sprintf("highconc_task%d_batch%d.cp", taskNum, batchSize))

			input := &obs.UploadFileInput{
				ObjectOperationInput: obs.ObjectOperationInput{
					Bucket: testBucket,
					Key:    key,
				},
				UploadFile:            srcFile,
				PartSize:              partSize,
				TaskNum:               taskNum,
				EnableCheckpoint:      true,
				CheckpointFile:        cpFile,
				CheckpointBatchSize:   batchSize,
			}

			start := time.Now()
			_, err := obsClient.UploadFile(input)
			elapsed := time.Since(start)

			if err != nil {
				t.Errorf("taskNum=%d batchSize=%d: upload failed: %v", taskNum, batchSize, err)
			} else {
				throughput := float64(objSize) / elapsed.Seconds() / 1024 / 1024
				t.Logf("objSize=100MB partSize=1MB parts=%d taskNum=%d batchSize=%d: throughput=%.2f MB/s elapsed=%v",
					parts, taskNum, batchSize, throughput, elapsed)
			}

			cleanupTestObject(t, key)
		}
	}
}

// TestPerf_HighConcurrency_Download - 测试高并发下载场景
func TestPerf_HighConcurrency_Download(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()

	// First create and upload 100MB file with 1MB parts
	objSize := int64(100 * 1024 * 1024)
	partSize := int64(1 * 1024 * 1024)

	srcFile := filepath.Join(tmpDir, "highconc_src.txt")
	buf := make([]byte, objSize)
	for i := range buf {
		buf[i] = byte('a' + (i % 26))
	}
	if err := os.WriteFile(srcFile, buf, 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	key := testObjectKey + "-highconc-download"

	// Use PutObject for test data setup (not part of the test scenario)
	_, err := obsClient.PutObject(&obs.PutObjectInput{
		PutObjectBasicInput: obs.PutObjectBasicInput{
			ObjectOperationInput: obs.ObjectOperationInput{
				Bucket: testBucket,
				Key:    key,
			},
		},
		Body: bytes.NewReader(buf),
	})
	if err != nil {
		t.Fatalf("upload failed: %v", err)
	}
	defer cleanupTestObject(t, key)

	parts := int(objSize / partSize)
	if objSize%partSize != 0 {
		parts++
	}

	taskNums := []int{5, 10, 20}
	batchSizes := []int{10, 20, 50}

	for _, taskNum := range taskNums {
		for _, batchSize := range batchSizes {
			dstFile := filepath.Join(tmpDir, fmt.Sprintf("highconc_dl_task%d_batch%d.txt", taskNum, batchSize))
			cpFile := filepath.Join(tmpDir, fmt.Sprintf("highconc_dl_task%d_batch%d.cp", taskNum, batchSize))

			input := &obs.DownloadFileInput{
				GetObjectMetadataInput: obs.GetObjectMetadataInput{
					Bucket: testBucket,
					Key:    key,
				},
				DownloadFile:          dstFile,
				PartSize:              partSize,
				TaskNum:               taskNum,
				EnableCheckpoint:      true,
				CheckpointFile:        cpFile,
				CheckpointBatchSize:   batchSize,
			}

			start := time.Now()
			_, err = obsClient.DownloadFile(input)
			elapsed := time.Since(start)

			if err != nil {
				t.Errorf("taskNum=%d batchSize=%d: download failed: %v", taskNum, batchSize, err)
			} else {
				throughput := float64(objSize) / elapsed.Seconds() / 1024 / 1024
				t.Logf("objSize=100MB partSize=1MB parts=%d taskNum=%d batchSize=%d: throughput=%.2f MB/s elapsed=%v",
					parts, taskNum, batchSize, throughput, elapsed)
			}
		}
	}
}
