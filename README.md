# MIT 6.824 Raft 分布式一致性协议实现 (Go)

本项目是 MIT 6.824 (Distributed Systems) 课程中 Raft 共识算法的完整实现。目前已完成 **Lab 2A (Election)**、**Lab 2B (Log Replication)** 和 **Lab 2C (Persistence)**。

## 🛠 已实现核心功能

### 1. 领导者选举 (Leader Election - Lab 2A)
- **状态机实现**：实现了 Follower、Candidate、Leader 三种状态的切换逻辑。
- **选举超时**：通过 `ticker` 协程维持随机选举超时（300ms - 500ms），有效避免选票瓜分。
- **心跳维持**：Leader 定期发送心跳（AppendEntries）以维持领导地位。
- **安全性检查**：在投票阶段通过 `isLogUpToDate` 确保只有日志足够“新”的节点才能被选为 Leader。

### 2. 日志同步 (Log Replication - Lab 2B)
- **强一致性复制**：Leader 接收客户端指令并将其同步至集群半数以上节点。
- **日志一致性检查**：严格执行 `PrevLogIndex` 和 `PrevLogTerm` 的匹配校验。
- **异步日志应用**：使用 `sync.Cond` 条件变量驱动 `applier` 协程，在 CommitIndex 更新后高效地将日志推送到状态机。

### 3. 持久化与快速恢复 (Persistence & Fast Rollback - Lab 2C)
- **状态持久化**：使用 `labgob` 对 `CurrentTerm`、`VotedFor` 和 `Log` 进行序列化存储，确保节点宕机重启后能恢复状态。
- **快速回退优化**：在 `AppendEntries` 失败时，利用 `ConflictTerm` 和 `ConflictIndex` 快速定位冲突位置，大幅减少日志对齐时的 RPC 通信次数。

## 📂 项目结构

- `raft.go`: 核心协议实现（RPC 处理、选举循环、心跳、日志同步逻辑）。
- `persister.go`: 模拟持久化存储层。
- `labrpc.go`: 模拟不可靠网络环境（存在丢包、延迟、乱序）。

## 🚀 运行测试

在 `src/raft` 目录下，可以使用以下命令验证实现是否正确：

```bash
# 测试选举逻辑
go test -run 2A

# 测试日志复制逻辑
go test -run 2B

# 测试持久化与恢复逻辑
go test -run 2C

# 运行所有 Raft 测试
go test -v
