# Checkpoint Async Writer 优化设计

## 1. 问题背景

### 1.1 问题描述

在上传/下载大文件时，SDK 使用分片（part）并发上传/下载，并通过 checkpoint 文件记录每个分片的完成状态，以支持断点续传。

原实现存在以下问题：

| 问题 | 描述 | 影响 |
|------|------|------|
| **同步锁竞争** | 每次 part 上传完成后需要 `lock.Lock()` 更新内存并同步写盘 | 高并发场景下锁竞争严重，RT 增加 |
| **高频同步写盘** | 每个 part 完成后立即调用 `updateCheckpointFile()` 同步写 XML 文件 | 磁盘 IO 成为瓶颈，尤其小 part 高频场景 |
| **崩溃丢状态** | part 完成后、数据尚未刷盘时进程崩溃 | 重启后该 part 需要重新上传，浪费带宽 |
| **原子性问题** | 直接 `ioutil.WriteFile()` 写盘，崩溃可能导致 checkpoint 文件损坏 | |

### 1.2 根因分析

```
原流程：
1. part 上传完成
2. lock.Lock()
3. 更新内存中 part 状态
4. lock.Unlock()
5. 同步写盘（ioutil.WriteFile）
6. 发进度事件
```

每个 part 完成都需要同步写盘，在 1000+ parts 的场景下写盘次数过多。

### 1.3 AsyncWriter 解决思路

1. **解耦 worker 和 I/O**：worker 提交后立即返回，不阻塞
2. **批量写入**：累积多个分片后一次性写入，减少 I/O 次数
3. **异步写入**：专门的 writer goroutine 处理写入，不阻塞业务

---

## 2. 修复方案

### 2.1 方案概述

引入**异步批量写入器** (`AsyncWriter`)，将内存更新和文件写入从同步模式改为异步批量模式：

| 阶段 | 改进前 | 改进后 |
|------|--------|--------|
| **内存更新** | 同步 lock | 异步提交到 channel |
| **文件写入** | 同步每次 flush | 批量 flush（batchSize 或 interval 触发） |
| **崩溃恢复** | 依赖上次成功刷盘 | SyncFlush + ListParts 对账 |

### 2.2 上传流程（带 checkpoint）

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         UploadFile Flow                                  │
└─────────────────────────────────────────────────────────────────────────┘

                               start
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │ getCheckpointFile()   │
                    └────────────────────────┘
                                 │
                    ┌────────────┴────────────┐
                    │                         │
               checkpoint               checkpoint
                不存在                    存在
                    │                         │
                    ▼                         ▼
           prepareUpload()           ┌───────────────┐
                    │               │   isValid()   │
                    │               └───────────────┘
                    │                    │
                    │          ┌─────────┴─────────┐
                    │          │                   │
                    │        valid              invalid
                    │          │                   │
                    │          │                   ▼
                    │          │        abortTask() + remove
                    │          │              │
                    │          │              ▼
                    │          │      prepareUpload()
                    │          │              │
                    └──────────┴──────────────┘
                                 │
                                 ▼
              ┌─────────────────────────────────┐
              │ CheckpointReconcile == true ?   │
              └─────────────────────────────────┘
                           │
                 ┌──────────┴──────────┐
                 │                     │
                 yes                   no
                 │                     │
                 ▼                     │
        ┌───────────────┐              │
        │  ListParts   │              │
        │  (分页获取)   │              │
        │  reconcile   │              │
        └───────────────┘              │
                 │                     │
                 └──────────┬──────────┘
                            │
                            ▼
                   resumeUpload()
                            │
                            ▼
              ┌──────────────────────────┐
              │   uploadPartConcurrent    │
              │  ──────────────────────  │
              │  1. 创建 CheckpointAsyncWriter │
              │  2. 并发执行所有 parts       │
              │  3. part完成 → Submit()     │
              │     到 channel            │
              └──────────────────────────┘
                            │
                            ▼
                   pool.ShutDown()
                            │
                            ▼
                    cw.SyncFlush()
                 (强制同步刷盘)
                            │
                            ▼
              completeMultipartUpload()
                            │
                            ▼
                              end
```

**流程说明**：

1. **Checkpoint 加载阶段**：读取本地 checkpoint 文件，验证有效性
2. **Reconcile 阶段**（可选）：调用 OBS ListParts 与服务端状态比对
3. **并发上传阶段**：创建 CheckpointAsyncWriter，并发执行所有 parts
4. **刷盘阶段**：pool.ShutDown() 后强制 SyncFlush，确保所有 pending items 落盘
5. **完成阶段**：调用 CompleteMultipartUpload 完成上传

### 2.3 SyncFlush 时序

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       SyncFlush Timeline                                 │
└─────────────────────────────────────────────────────────────────────────┘

  Worker1: ──[part1 done]───────────────────────────[Submit]──────────┐
  Worker2: ──[part2 done]───────────────────────────[Submit]────────┤
  Worker3: ──[part3 done]───────────────────────────[Submit]────────┤
  ...                                                             │
                                                                    │
  Channel:  [item1] [item2] [item3] ...                            │
                                                                    │
  pool.ShutDown():              [drain]────────────────────────────┤
                                                                    │
  cw.SyncFlush():   ────────────[强制刷盘]─────────────────────────┤
                                                                    │
  checkpoint文件:   ────────────[已更新]────────────────────────────┘

Timeline:
  T=0: part1 完成, Submit() → channel
  T=1: part2 完成, Submit() → channel
  T=2: part3 完成, Submit() → channel
  T=3: pool.ShutDown() 开始
  T=4: 所有 task 完成
  T=5: pool.ShutDown() 完成
  T=6: cw.SyncFlush() 被调用（所有 pending items 刷盘）
  T=7: cw.Shutdown() 完成
```

---

## 3. 核心设计

### 3.1 AsyncWriter 抽象

```
┌─────────────────────────────────────────────────────────────────┐
│                      AsyncWriter Architecture                    │
└─────────────────────────────────────────────────────────────────┘

                        ┌─────────────────┐
                        │   AsyncWriter    │
                        │  ┌───────────┐  │
  Submit(item) ────────▶│  │   channel  │  │───────┐
                        │  │  (buffer)  │  │       │
                        │  └───────────┘  │       │
                        │       │         │       │
                        │       ▼         │       │
                        │  ┌─────────┐    │       │
                        │  │  items  │    │       │  doFlush()
                        │  │ (slice) │    │       │──────────▶ Flusher
                        │  └─────────┘    │       │              │
                        │       │         │       │              ▼
                        │       │         │       │    ┌───────────────┐
                        │  ┌────┴────┐    │       │    │ updateCheckpoint│
                        │  │  mu     │    │       │    │     File()      │
                        │  │ (mutex) │    │       │    └───────────────┘
                        │  └─────────┘    │       │
                        └─────────────────┘       │
                               │                 │
                               ▼                 │
                        ┌─────────────┐         │
                        │  run() goroutine     │
                        │  ─────────  │         │
                        │  select {   │         │
                        │    channel  │         │
                        │    ticker   │         │
                        │    done     │         │
                        │  }          │         │
                        └─────────────┘         │
                               │                 │
                               ▼                 │
                        ┌─────────────┐         │
                        │  Shutdown() │─────────┘
                        └─────────────┘
```

**设计说明**：

| 组件 | 说明 |
|------|------|
| **channel** | 异步接收 Submit 的更新，提供有界缓冲；满时对调用方施加背压而不丢数据 |
| **items slice** | 累积待刷新的更新批次，仅由 `run()` goroutine 持有 |
| **flushReq channel** | `SyncFlush()` 通过该通道向 `run()` 发送同步刷盘请求，并等待 ACK |
| **ticker** | 定时器，定期触发 flush 兜底 |
| **run() goroutine** | 后台串行处理 `channel`、`ticker`、`flushReq`、`done` 事件，保证 `items` 和 target 的单 owner 访问 |
| **Flusher** | 批量应用更新到内存并写入文件 |

### 3.2 CheckpointAsyncWriter

```
┌─────────────────────────────────────────────────────────────────┐
│               CheckpointAsyncWriter Architecture                 │
└─────────────────────────────────────────────────────────────────┘

  ┌───────────────────────────────────────────────────────┐
  │            CheckpointAsyncWriter                      │
  │  ┌─────────────────────────────────────────────────┐ │
  │  │              AsyncWriter                         │ │
  │  │  ┌─────────┐   ┌─────────┐   ┌───────────────┐  │ │
  │  │  │ channel │──▶│  items  │──▶│  doFlush()    │  │ │
  │  │  └─────────┘   └─────────┘   └───────┬───────┘  │ │
  │  └─────────────────────────────────────│──────────┘ │
  │                                          │            │
  └──────────────────────────────────────────│────────────┘
                                               │
                                               ▼
                                    ┌─────────────────────┐
                                    │ checkpointFlusher    │
                                    │  ────────────────   │
                                    │  1. Apply(items)    │
                                    │  2. updateCheckpoint│
                                    │      File()         │
                                    └─────────────────────┘
```

**设计说明**：

- `CheckpointAsyncWriter` 组合 `AsyncWriter` 和 `checkpointFlusher`
- `checkpointFlusher` 先将所有更新 Apply 到内存，再调用 `updateCheckpointFile()` 写入文件
- `updateCheckpointFile()` 使用临时文件 + `fsync` + `rename` 提升原子性和持久化保证

### 3.3 updateCheckpointFile 原子性

```
┌─────────────────────────────────────────────────────────────────┐
│                   updateCheckpointFile Flow                      │
└─────────────────────────────────────────────────────────────────┘

                  xml.Marshal(target)
                        │
                        ▼
              ┌──────────────────┐
              │ Open(.tmp)       │
              │ + Write          │
              └──────────────────┘
                        │
                        ▼
              ┌──────────────────┐
              │ file.Sync()      │ ──── 内容持久化
              └──────────────────┘
                        │
                        ▼
              ┌──────────────────┐
              │   os.Rename()    │ ──── 原子替换
              └──────────────────┘
                        │
                        ▼
              ┌──────────────────┐
              │ parent dir Sync  │ ──── 非 Windows best-effort
              └──────────────────┘
                        │
                        ▼
                  checkpoint.xml
```

---

## 4. SDK 参数说明

### 4.1 UploadFileInput 新增字段

| 参数名称 | 参数类型 | 是否必选 | 描述 |
|---------|----------|----------|------|
| `CheckpointBatchSize` | int | 否 | 异步写入器的批次大小。累积至此数量后触发一次 flush。**取值范围**：>0。**默认取值**：5（待性能测试确定）。**建议值**：10~100 |
| `CheckpointFlushInterval` | int64 | 否 | 强制 flush 间隔秒数。即使未达到 batchSize，每隔此时间也会触发一次 flush。**取值范围**：>0。**默认取值**：5（待性能测试确定）。**建议值**：3~10 |
| `CheckpointReconcile` | bool | 否 | 是否在加载时与 OBS ListParts 对账。开启后会在加载 checkpoint 时调用 ListParts 与服务端状态比对，适用于超大文件（10000+ parts）和高可靠性场景。**取值范围**：true/false。**默认取值**：false |

### 4.2 DownloadFileInput 新增字段

| 参数名称 | 参数类型 | 是否必选 | 描述 |
|---------|----------|----------|------|
| `CheckpointBatchSize` | int | 否 | 异步写入器的批次大小。累积至此数量后触发一次 flush。**取值范围**：>0。**默认取值**：5（待性能测试确定）。**建议值**：10~100 |
| `CheckpointFlushInterval` | int64 | 否 | 强制 flush 间隔秒数。即使未达到 batchSize，每隔此时间也会触发一次 flush。**取值范围**：>0。**默认取值**：5（待性能测试确定）。**建议值**：3~10 |

---

## 5. 方案流程

### 5.1 AsyncWriter 内部流程

```
┌─────────────────────────────────────────────────────────────────┐
│                   AsyncWriter Internal Flow                      │
└─────────────────────────────────────────────────────────────────┘

	                        Submit(item)
	                            │
	                            ▼
	                    ┌────────────────────┐
	                    │ bounded channel     │
	                    │ full -> block       │
	                    │ caller until drain  │
	                    └────────────────────┘
	                               │
	                               ▼
	                    ┌─────────────────────────┐
	                    │ run() goroutine          │
	                    │ ───────────────────────  │
	                    │ select {                 │
	                    │   item := <-channel      │
	                    │     append to items      │
	                    │     len >= batchSize     │
	                    │       -> doFlush()       │
	                    │   <-ticker               │
	                    │       -> doFlush()       │
	                    │   flushReq <- ack        │
	                    │       -> drain channel   │
	                    │       -> doFlush()       │
	                    │       -> ack             │
	                    │   <-done                 │
	                    │       -> drain channel   │
	                    │       -> doFlush()       │
	                    │       -> exit            │
	                    │ }                        │
	                    └─────────────────────────┘
	                               │
	                               ▼
	                      ┌─────────────────────┐
	                      │ checkpointFlusher   │
	                      │ Flush(items)        │
	                      └─────────────────────┘
```

**流程说明**：

1. **Submit**：提交到有界 channel；channel 满时对调用方施加背压，保证更新不丢失
2. **batchSize 触发**：累积达到阈值时自动 flush
3. **ticker 触发**：定时器兜底，确保即使流量低也能定期刷盘
4. **SyncFlush 请求**：通过 `flushReq` 让 `run()` 串行执行 `drain channel + flush`，确保 ACK 返回前所有 pending items 已处理
5. **done 信号**：Shutdown 时 drain channel 并最后 flush 一次

### 5.2 flush 触发条件

| 触发条件 | 说明 | 优先级 |
|---------|------|--------|
| batchSize 阈值 | 累积达到配置值时触发 | 高 |
| flushInterval 定时器 | 达到间隔时间时触发 | 中 |
| Shutdown drain | 退出时触发最后一次 flush | 高 |
| SyncFlush | 代码主动调用强制刷盘 | 高 |

### 5.3 风险评估与缓解

| 风险 | 影响 | 缓解方案 |
|------|------|---------|
| channel 满导致数据丢失 | 丢失 part 完成状态，上传场景可能缺失 ETag，影响 `CompleteMultipartUpload` | 改为有界缓冲 + 背压，不再丢弃更新；默认容量保持 `10000`，并支持 `CheckpointBatchSize` / `CheckpointFlushInterval` 调优 |
| `SyncFlush()` 与 `run()` 并发操作 `items` | 可能产生数据竞争或遗漏 channel backlog | 改为 `flushReq` 控制通道，所有 `items` 读写、flush、target 更新均由 `run()` goroutine 串行执行 |
| `EnableCheckpoint=false` 的兼容性回归 | 未启用 checkpoint 的调用方可能触发 nil pointer panic | 在上传/下载收尾路径增加 `cw != nil` 守卫，保持原有非 checkpoint 路径行为不变 |
| 进度字节数并发读取 | 多 worker 并发上报进度时，`ConsumedBytes` 可能出现竞争读取 | 使用 `atomic.AddInt64()` 的返回值构造事件，避免对共享计数做非原子读取 |
| 掉电导致 checkpoint 回退 | 仅有 `rename` 不能保证内容和目录项在 power loss 后持久化 | 写临时文件后执行 `file.Sync()`；`rename` 后在非 Windows 平台对父目录做 `Sync()` |

---

## 6. 测试策略

### 6.1 UT 测试覆盖（已实现 14 cases）

| 测试用例 | 验证点 | 代码覆盖率 |
|---------|--------|-----------|
| `TestAsyncWriter_ShouldFlushWhenBatchSizeReached` | 累积达到 batchSize 时应触发 flush | 覆盖 doFlush() |
| `TestAsyncWriter_ShouldFlushOnShutdownWithPendingItems` | 关闭时应 flush 所有待处理 items | 覆盖 Shutdown drain |
| `TestAsyncWriter_ShouldMakeProgressUnderLoad` | Submit 在正常负载下应持续推进 | 覆盖 Submit 常规路径 |
| `TestAsyncWriter_ShouldNotPanicWhenFlushingEmptyItems` | 无 items 时 flush 不应 panic | 覆盖 doFlush() |
| `TestAsyncWriter_ShouldFlushOnTickerInterval` | ticker 达到间隔时应触发 flush | 覆盖 ticker 逻辑 |
| `TestAsyncWriter_ShouldFlushAllItemsConcurrently` | 并发 Submit 时应正确处理所有 items | 覆盖并发安全 |
| `TestAsyncWriter_ShouldBlockWhenChannelIsFull` | channel 满时应施加背压而不是丢弃 | 覆盖 channel 满逻辑 |
| `TestAsyncWriter_ShouldNotLeakGoroutineOnShutdown` | 关闭后不应存在 goroutine 泄漏 | 覆盖 run() 退出 |
| `TestCheckpointAsyncWriter_ShouldUpdateCheckpointOnBatchFlush` | CheckpointAsyncWriter 应正确更新 checkpoint | 覆盖 Apply 逻辑 |
| `TestCheckpointAsyncWriter_ShouldFlushOnInterval` | CheckpointAsyncWriter ticker 应正确触发 flush | 覆盖 ticker |
| `TestAsyncWriter_ShouldSyncFlushDrainQueuedItems` | SyncFlush 应 drain channel 并刷出所有待处理项 | 覆盖 SyncFlush |
| `TestUploadPartConcurrent_ShouldWorkWithoutCheckpoint` | `EnableCheckpoint=false` 上传路径不应 panic | 覆盖兼容性修复 |
| `TestDownloadFileConcurrent_ShouldWorkWithoutCheckpoint` | `EnableCheckpoint=false` 下载路径不应 panic | 覆盖兼容性修复 |
| `TestUpdateCheckpointFile_WritesCheckpointAndCleansTempFile` | checkpoint 文件应正确落盘且不残留 `.tmp` 文件 | 覆盖 durability 基础路径 |

### 6.1.1 建议补充的 UT 用例

| 测试用例 | 验证点 | 价值 |
|---------|--------|------|
| `TestUploadProgress_ShouldUseAtomicCompletedBytes` | 并发上传完成多个 part 时，进度事件中的 `ConsumedBytes` 应来自 `atomic.AddInt64()` 返回值，避免非原子读取 | 覆盖上传进度计数修复 |
| `TestDownloadProgress_ShouldUseAtomicCompletedBytes` | 并发下载完成多个 part 时，进度事件中的 `ConsumedBytes` 应保持一致且无共享计数竞争 | 覆盖下载进度计数修复 |
| `TestProgressListener_ShouldDocumentConcurrentCallbacks` | 通过线程安全 listener 样例验证 multipart 场景下 callback 可被并发调用且不应 panic | 固化 P2 契约，避免误用 |

### 6.2 集成测试

#### 6.2.1 功能测试

| 测试用例 | 输入 | 预期结果 |
|---------|------|---------|
| `IT_UploadFile_CompleteSuccessfully` | 100MB, partSize=5MB, TaskNum=5 | 最终文件 MD5 正确 |
| `IT_UploadFile_ResumeFromCheckpoint` | 上传 50% 后中断，重启继续 | 从断点继续，最终文件正确 |
| `IT_UploadFile_DetectFileModification` | 文件在中途被修改 | 应检测并重新开始 |
| `IT_DownloadFile_CompleteSuccessfully` | 100MB, partSize=5MB, TaskNum=5 | 最终文件 MD5 正确 |
| `IT_DownloadFile_ResumeFromCheckpoint` | 下载 50% 后中断，重启继续 | 从断点继续，最终文件正确 |
| `IT_ConcurrentUpload_NotInterfere` | TaskNum=10 并发上传不同文件 | 各文件独立，无竞争 |
| `IT_CheckpointReconcile_RecoverFromOBS` | 模拟 OBS 端多 part 状态 | 应从 OBS 恢复状态 |
| `IT_UploadFile_ProgressBytesMonotonic` | 100MB, partSize=1MB, TaskNum=10, 线程安全 listener | `ConsumedBytes` 单调不减，最终值等于文件大小 |
| `IT_DownloadFile_ProgressBytesMonotonic` | 100MB, partSize=1MB, TaskNum=10, 线程安全 listener | `ConsumedBytes` 单调不减，最终值等于对象大小 |
| `IT_ProgressListener_ConcurrentSafe` | TaskNum=10, 使用带锁 listener 收集事件 | SDK 在并发 callback 契约下正常工作，无 panic/死锁 |

#### 6.2.2 性能测试

| 测试用例 | 输入 | 验证指标 |
|---------|------|---------|
| `Perf_UploadFile_TargetThroughput` | 100MB, batchSize=20 | 吞吐量 > 基准 2x |
| `Perf_UploadFile_ReduceCheckpointIO` | 100MB, partSize=1MB, TaskNum=10 | checkpoint 写 IO 次数 < 基准 1/10 |
| `Perf_CheckpointWrite_ReduceLatency` | 1000 次 Submit | 平均延迟 < 同步方案 |
| `Perf_BatchSize_FindOptimalValue` | 不同 batchSize 配置 | 找到最优 batchSize 组合 |

#### 6.2.3 故障测试

| 测试用例 | 场景 | 预期结果 |
|---------|------|---------|
| `Fault_UploadFile_RecoverOnChannelFull` | 极大并发，channel 满 | 对 worker 施加背压，不丢 ETag，最终正常完成 |
| `Fault_UploadFile_FlushAllOnShutdown` | 正常完成 | 无 goroutine 泄漏，checkpoint 完整 |
| `Fault_UploadFile_PreserveStateOnPanic` | 模拟崩溃 | SyncFlush 后重启可恢复 |
| `Fault_CheckpointFile_AtomicOnCrash` | 写盘时模拟崩溃 | checkpoint 文件不损坏 |
| `Fault_CheckpointFile_SyncFailurePropagates` | checkpoint 目录不可写 / `Sync()` 失败 | 初始化写盘阶段返回错误；异步 flush 阶段记录 ERROR 日志并保持主流程错误模型一致 |
| `Fault_CheckpointDirSync_BestEffortOnWindows` | Windows 平台跳过目录 `fsync` | 文件内容 `Sync()` 正常执行，目录级 durability 退化为 best-effort，但功能不回归 |

### 6.3 已知问题与限制

1. `AsyncWriter` 为了保证 checkpoint correctness，在 channel 打满时会对提交方施加背压；这意味着极端慢磁盘或超小 batch 配置下，part 完成回调可能短暂阻塞。
2. `SyncFlush()` 的正确使用前提仍是调用方先停止产生新更新；当前上传/下载流程通过 `pool.ShutDown()` 后再 `SyncFlush()` 满足这一约束。
3. 下载分片数理论上可能大于默认 channel 容量 `10000`；若业务使用极小 `PartSize`，建议同时调大 `CheckpointBatchSize` 或分片大小，避免不必要的背压。
4. `ProgressListener` 在 multipart 上传/下载场景下可能被多个 worker 并发调用；SDK 不做串行化包装，listener 实现方需要自行保证并发安全。
5. Windows 平台当前只保证临时文件 `Sync()`；目录级 `fsync` 作为平台相关能力在非 Windows 平台执行，Windows 上采用 best-effort 语义。

---

## 7. 完整使用用例

### 7.1 基础用法（默认配置）

```go
input := UploadFileInput{
    Bucket: "my-bucket",
    Key:    "large-file.zip",
    UploadFile:       "/path/to/large-file.zip",
    PartSize:         5 * 1024 * 1024, // 5MB
    TaskNum:          5,
    EnableCheckpoint: true,
    CheckpointFile:   "/tmp/upload.cpt",
}

output, err := obsClient.UploadFile(input)
```

### 7.2 自定义 batchSize 和 flushInterval

```go
input := UploadFileInput{
    Bucket:                    "my-bucket",
    Key:                      "large-file.zip",
    UploadFile:               "/path/to/large-file.zip",
    PartSize:                 5 * 1024 * 1024,
    TaskNum:                  10,
    EnableCheckpoint:        true,
    CheckpointFile:           "/tmp/upload.cpt",
    CheckpointBatchSize:      20,   // 每 20 个 part 刷一次盘
    CheckpointFlushInterval:  10,   // 每 10 秒强制刷盘
}

output, err := obsClient.UploadFile(input)
```

### 7.3 开启 ListParts 对账（高可靠性场景）

```go
input := UploadFileInput{
    Bucket:                    "my-bucket",
    Key:                      "large-file.zip",
    UploadFile:               "/path/to/large-file.zip",
    PartSize:                 5 * 1024 * 1024,
    TaskNum:                  10,
    EnableCheckpoint:        true,
    CheckpointFile:           "/tmp/upload.cpt",
    CheckpointBatchSize:      20,
    CheckpointFlushInterval:  10,
    CheckpointReconcile:      true,  // 开启对账
}

output, err := obsClient.UploadFile(input)
```

### 7.4 下载用法

```go
input := DownloadFileInput{
    Bucket:                    "my-bucket",
    Key:                      "large-file.zip",
    DownloadFile:             "/path/to/downloaded-file.zip",
    PartSize:                 5 * 1024 * 1024,
    TaskNum:                  5,
    EnableCheckpoint:        true,
    CheckpointFile:           "/tmp/download.cpt",
    CheckpointBatchSize:      20,
    CheckpointFlushInterval:  10,
}

output, err := obsClient.DownloadFile(input)
```

---

## 8. 文件改动清单

| 文件 | 改动类型 | 说明 |
|------|----------|------|
| `obs/async.go` | **新增** | 通用 AsyncWriter 组件 |
| `obs/async_checkpoint.go` | **新增** | Checkpoint 相关类型和 Flusher |
| `obs/async_test.go` | **新增** | 单元测试（13 cases，覆盖 backpressure / SyncFlush / 兼容性） |
| `obs/transfer_test.go` | **新增** | checkpoint durability 单元测试 |
| `obs/transfer.go` | 大改 | 使用 CheckpointAsyncWriter + SyncFlush |
| `obs/model_object.go` | 中改 | 新增 CheckpointBatchSize, CheckpointFlushInterval, CheckpointReconcile |
| `tests/benchmark/*` | **新增** | 性能测试 |
| `tests/it/*` | **新增** | 集成测试 |

---

## 9. 未来演进

### 9.1 Log 模块优化

`log.go` 中的 `loggerWrapper` 使用阻塞发送，高频日志会阻塞业务。可利用 AsyncWriter 进行优化，统一公共组件，非阻塞发送，不影响业务线程。

### 9.2 其他潜在优化点

| 优化方向 | 说明 | 优先级 |
|----------|------|--------|
| Log 模块优化 | 重构为使用 AsyncWriter | 低 |
| 配置参数推荐 | 根据性能测试结果提供推荐值 | 中 |

---

## 10. 归档信息

| 项目 | 内容 |
|------|------|
| **文档版本** | v1.0 |
| **创建日期** | 2026-04-27 |
| **状态** | 已完成 Phase 1 + Phase 2 |
| **相关文件** | `obs/async.go`, `obs/async_checkpoint.go`, `obs/transfer.go`, `obs/model_object.go` |
| **分支** | `feature/optimize-checkpoint-update` |
| **测试覆盖** | UT: 已实现 14 cases，另有 progress / listener 契约补充用例建议；集成测试覆盖功能+性能+故障 |
