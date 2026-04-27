package obs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestUpdateCheckpointFile_WritesCheckpointAndCleansTempFile(t *testing.T) {
	checkpointPath := filepath.Join(t.TempDir(), "upload.checkpoint")
	checkpoint := &UploadCheckpoint{
		Bucket:     "bucket",
		Key:        "key",
		UploadId:   "upload-id",
		UploadFile: "source.file",
		FileInfo: FileStatus{
			Size:         128,
			LastModified: 12345,
		},
		UploadParts: []UploadPartInfo{
			{
				PartNumber:  1,
				Etag:        "etag-1",
				PartSize:    128,
				IsCompleted: true,
			},
		},
	}

	if err := updateCheckpointFile(checkpoint, checkpointPath); err != nil {
		t.Fatalf("updateCheckpointFile returned error: %v", err)
	}

	var loaded UploadCheckpoint
	if err := loadCheckpointFile(checkpointPath, &loaded); err != nil {
		t.Fatalf("loadCheckpointFile returned error: %v", err)
	}

	if loaded.UploadId != checkpoint.UploadId {
		t.Fatalf("expected upload id %q, got %q", checkpoint.UploadId, loaded.UploadId)
	}
	if len(loaded.UploadParts) != 1 || loaded.UploadParts[0].Etag != "etag-1" {
		t.Fatalf("expected checkpoint part etag to round-trip, got %+v", loaded.UploadParts)
	}

	if _, err := os.Stat(checkpointPath + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("expected temp checkpoint file to be removed, stat err=%v", err)
	}
}
