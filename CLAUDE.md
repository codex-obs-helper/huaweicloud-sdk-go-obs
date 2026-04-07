# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the Huawei Cloud OBS (Object Storage Service) Go SDK. It provides a Go client for interacting with Huawei Cloud's Object Storage Service.

## Build and Test

To build the SDK:
```bash
go build ./...
```

To run tests:
```bash
go test ./...
```

To run a single test:
```bash
go test -run TestName ./...
```

To run the sample code:
```bash
cd main && go run obs_go_sample.go
```

## Package Structure

- `obs/` - Core SDK package
- `main/` - Sample/demo code
- `examples/` - Additional example code

## Core Architecture

The SDK is organized around `ObsClient` in `obs/client_base.go`. Entry point:

```go
obs.New(ak, sk, endpoint, configurers...)
```

### Key Components

**Configuration (`obs/conf.go`)**
- Functional options pattern with `With*` configurers
- Supports V2, V4, and OBS signature types

**Security Providers (`obs/provider.go`)**
- `BasicSecurityProvider` - AK/SK auth
- `EnvSecurityProvider` - Reads `OBS_ACCESS_KEY_ID`, `OBS_SECRET_ACCESS_KEY`, `OBS_SECURITY_TOKEN`
- `EcsSecurityProvider` - ECS metadata credentials

**HTTP Layer (`obs/http.go`)**
- Request/response processing, retry logic, redirect handling

**API Organization**
- `client_bucket.go` - Bucket operations
- `client_object.go` - Object operations
- `client_part.go` - Multipart upload
- `client_resume.go` - Resumable upload/download with pause/cancel/resume
- `temporary_*.go` - Signed URL operations
- `trait_*.go` - Protocol-specific implementations

**Signature Types**
- `SignatureV4` (default), `SignatureV2`, `SignatureObs`
- Path style auto-enabled for IP endpoints, or via `WithPathStyle(true)`

## Resume Transfer

Large file uploads/downloads support pause/cancel/resume via the `TransferController` interface. Use `CreateUploadTask` or `CreateDownloadTask` to create a controller, then call `Start()`. Progress can be tracked via `TransferCallback` or the `Progress()` method.
