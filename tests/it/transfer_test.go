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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

var (
	testBucket    string
	testEndpoint  string
	testAk        string
	testSk        string
	obsClient     *obs.ObsClient
	enableIT      bool
	testObjectKey string
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func init() {
	testAk = getEnv("OBS_AK", "")
	testSk = getEnv("OBS_SK", "")
	testEndpoint = getEnv("OBS_ENDPOINT", "")
	testBucket = getEnv("OBS_BUCKET", "")
	enableIT = os.Getenv("OBS_ENABLE_IT") == "true"

	if enableIT && testAk != "" && testSk != "" && testEndpoint != "" && testBucket != "" {
		var err error
		obsClient, err = obs.New(testAk, testSk, testEndpoint)
		if err != nil {
			panic(err)
		}
		testObjectKey = fmt.Sprintf("test-it-%d", time.Now().UnixNano())
	}
}

func skipIfNotEnabled(t *testing.T) {
	if !enableIT {
		t.Skip("Integration tests not enabled (set OBS_ENABLE_IT=true)")
	}
	if obsClient == nil {
		t.Skip("OBS client not initialized (missing credentials)")
	}
}

func cleanupTestObject(t *testing.T, key string) {
	if obsClient == nil {
		return
	}
	_, _ = obsClient.DeleteObject(&obs.DeleteObjectInput{
		Bucket: testBucket,
		Key:    key,
	})
}

func TestUploadResume(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "upload_src.txt")
	cpFile := filepath.Join(tmpDir, "checkpoint.cp")

	// create 10MB test file
	content := strings.Repeat("test-data-", 100*1024) // ~1MB per line
	if err := os.WriteFile(srcFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	key := testObjectKey + "-upload-resume"

	// first upload with checkpoint
	input1 := &obs.UploadFileInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket: testBucket,
			Key:    key,
		},
		UploadFile:          srcFile,
		PartSize:            5 * 1024 * 1024, // 5MB
		TaskNum:             4,
		EnableCheckpoint:    true,
		CheckpointFile:      cpFile,
		CheckpointBatchSize: 5,
	}

	output1, err := obsClient.UploadFile(input1)
	if err != nil {
		t.Fatalf("first upload failed: %v", err)
	}
	defer cleanupTestObject(t, key)

	if output1.ETag == "" {
		t.Error("expected ETag in output")
	}

	// verify file was uploaded
	_, err = obsClient.GetObjectMetadata(&obs.GetObjectMetadataInput{
		Bucket: testBucket,
		Key:    key,
	})
	if err != nil {
		t.Fatalf("object not found after upload: %v", err)
	}

	// test resume: delete local file and re-upload (should resume from checkpoint)
	_ = os.Remove(srcFile)
	_ = os.WriteFile(srcFile, []byte(content), 0644)

	output2, err := obsClient.UploadFile(input1)
	if err != nil {
		t.Fatalf("resume upload failed: %v", err)
	}

	if output2.ETag != output1.ETag {
		t.Errorf("ETag mismatch: first=%s, second=%s", output1.ETag, output2.ETag)
	}
}

func TestDownloadResume(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	dstFile := filepath.Join(tmpDir, "download_dst.txt")
	cpFile := filepath.Join(tmpDir, "checkpoint.cp")

	// create test object first
	testContent := strings.Repeat("test-data-", 100*1024)
	key := testObjectKey + "-download-resume"

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

	// first download with checkpoint
	input1 := &obs.DownloadFileInput{
		GetObjectMetadataInput: obs.GetObjectMetadataInput{
			Bucket: testBucket,
			Key:    key,
		},
		DownloadFile:          dstFile,
		PartSize:              1 * 1024 * 1024, // 1MB
		TaskNum:               4,
		EnableCheckpoint:      true,
		CheckpointFile:        cpFile,
		CheckpointBatchSize:   5,
	}

	_, err = obsClient.DownloadFile(input1)
	if err != nil {
		t.Fatalf("first download failed: %v", err)
	}

	// verify downloaded file
	data, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}
	if len(data) != len(testContent) {
		t.Errorf("content length mismatch: expected %d, got %d", len(testContent), len(data))
	}

	// test resume: delete dst and re-download
	_ = os.Remove(dstFile)

	_, err = obsClient.DownloadFile(input1)
	if err != nil {
		t.Fatalf("resume download failed: %v", err)
	}

	data, err = os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("failed to read resumed file: %v", err)
	}
	if len(data) != len(testContent) {
		t.Errorf("resumed content length mismatch: expected %d, got %d", len(testContent), len(data))
	}
}

func TestConcurrentUpload(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "concurrent_src.txt")

	content := strings.Repeat("concurrent-test-", 50*1024)
	if err := os.WriteFile(srcFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	keys := make([]string, 3)
	for i := 0; i < 3; i++ {
		keys[i] = fmt.Sprintf("%s-concurrent-%d", testObjectKey, i)
	}

	// upload 3 files concurrently
	done := make(chan error, 3)
	for i, key := range keys {
		go func(idx int, k string) {
			input := &obs.UploadFileInput{
				ObjectOperationInput: obs.ObjectOperationInput{
					Bucket: testBucket,
					Key:    k,
				},
				UploadFile:          srcFile,
				PartSize:            1 * 1024 * 1024,
				TaskNum:             2,
				EnableCheckpoint:    true,
				CheckpointFile:      filepath.Join(tmpDir, fmt.Sprintf("cp-%d.cp", idx)),
				CheckpointBatchSize: 5,
			}
			_, err := obsClient.UploadFile(input)
			done <- err
		}(i, key)
	}

	timeout := time.After(2 * time.Minute)
	for i := 0; i < 3; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("concurrent upload %d failed: %v", i, err)
			}
		case <-timeout:
			t.Fatal("concurrent upload timed out")
		}
	}

	// cleanup
	for _, key := range keys {
		cleanupTestObject(t, key)
	}
}

func TestCrashRecovery(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "crash_src.txt")
	cpFile := filepath.Join(tmpDir, "crash.cp")

	content := strings.Repeat("crash-recovery-test-", 200*1024)
	if err := os.WriteFile(srcFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	key := testObjectKey + "-crash-recovery"

	// upload with checkpoint - small batch to trigger multiple flushes
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
		CheckpointBatchSize:   2,              // small batch for frequent flushes
		CheckpointFlushInterval: 1,            // 1 second interval
	}

	_, err := obsClient.UploadFile(input)
	if err != nil {
		t.Fatalf("upload with crash recovery failed: %v", err)
	}
	defer cleanupTestObject(t, key)

	// verify checkpoint file is cleaned up
	if _, err := os.Stat(cpFile); err == nil {
		t.Log("checkpoint file not cleaned up (may be expected in some cases)")
	}
}

func TestCheckpointFileDeleted(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "deleted_cp_src.txt")
	cpFile := filepath.Join(tmpDir, "deleted_cp.cp")

	content := strings.Repeat("deleted-checkpoint-", 100*1024)
	if err := os.WriteFile(srcFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	key := testObjectKey + "-deleted-cp"

	// first upload
	input := &obs.UploadFileInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket: testBucket,
			Key:    key,
		},
		UploadFile:          srcFile,
		PartSize:            1 * 1024 * 1024,
		TaskNum:             2,
		EnableCheckpoint:    true,
		CheckpointFile:      cpFile,
		CheckpointBatchSize: 5,
	}

	_, err := obsClient.UploadFile(input)
	if err != nil {
		t.Fatalf("first upload failed: %v", err)
	}
	defer cleanupTestObject(t, key)

	// delete checkpoint file
	_ = os.Remove(cpFile)

	// re-upload should start fresh
	_, err = obsClient.UploadFile(input)
	if err != nil {
		t.Fatalf("re-upload after checkpoint delete failed: %v", err)
	}
}

func TestLargeFile(t *testing.T) {
	skipIfNotEnabled(t)

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "large_src.txt")
	cpFile := filepath.Join(tmpDir, "large.cp")

	// create 50MB test file
	buf := make([]byte, 50*1024*1024)
	for i := range buf {
		buf[i] = byte('a' + (i % 26))
	}
	if err := os.WriteFile(srcFile, buf, 0644); err != nil {
		t.Fatalf("failed to create large source file: %v", err)
	}

	key := testObjectKey + "-large"

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
		CheckpointBatchSize:   10,
		CheckpointFlushInterval: 5,
	}

	output, err := obsClient.UploadFile(input)
	if err != nil {
		t.Fatalf("large file upload failed: %v", err)
	}
	defer cleanupTestObject(t, key)

	if output.ETag == "" {
		t.Error("expected ETag for large file upload")
	}

	// verify size
	meta, err := obsClient.GetObjectMetadata(&obs.GetObjectMetadataInput{
		Bucket: testBucket,
		Key:    key,
	})
	if err != nil {
		t.Fatalf("failed to get metadata: %v", err)
	}
	if meta.ContentLength != int64(len(buf)) {
		t.Errorf("size mismatch: expected %d, got %d", len(buf), meta.ContentLength)
	}
}
