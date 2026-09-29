# local-chain-indexer-go

本地区块数据索引器。用本地模拟的区块源（`source` 包）代替真实节点，
在**分叉重组**、**重复/乱序投递**、**长时间中断后追赶**三种情况下，
保证索引结果一致且行为可解释。

## 包结构

| 包 | 职责 |
|---|---|
| `chain` | 数据模型（`Block`/`Tx`/`Hash`）与带分类原因的拒绝错误 `RejectError` |
| `indexer` | 索引器核心：校验、应用、重组、查询、批量追赶 |
| `source` | 本地模拟区块源：确定性造链、构造分叉，实现 `indexer.BlockSource` |

## 数据模型

- `chain.Block`：`Height` / `Hash` / `ParentHash` / `Txs`，按高度推进、哈希链接。
- `chain.Tx`：`Hash` / `From` / `To` / `Amount`。
- 索引器内部维护三类状态（`indexer/store.go`）：
  - **规范链** `blocks[]`：下标即高度；
  - **交易索引** `txIndex`：交易哈希 → （高度， 区块哈希）；
  - **派生数据** `balances`：地址余额，随区块应用/撤销增减。
- 每个区块应用时生成一条**撤销日志**（`blockUndo`：新增的交易哈希 + 余额反向增量），
  与区块一一对应，是回滚的依据。
- 所有变更先收集进 `batch`，`commit` 时一次性落到内部 map——
  提交点之外不存在"写了一半"的状态。

## 投递校验与拒绝原因

`ApplyBlock` 拒绝时返回 `*chain.RejectError`，`Reason` 字段区分四种情况，
不会统一报笼统错误：

| Reason | 触发条件 |
|---|---|
| `ReasonDuplicate` | 同高度同哈希的区块已索引（含已入分叉池的重复分叉块） |
| `ReasonGap` | 高度跳过未索引区间（首块不在高度 0，或跳到 tip+2 及以后） |
| `ReasonParentMismatch` | 父哈希既不是规范链对应高度的块，也不是已知分叉块 |
| `ReasonHeightRegression` | 落后尖端超过最大重组深度（默认 128，可用 `WithMaxReorgDepth` 调整） |

被拒绝的投递不修改任何状态。重复与乱序投递因此安全：
重复的被忽略，早到的（空洞）被拒绝后可由投递方稍后重试。

## 重组与回滚规则

- 高度 ≤ 尖端且与规范链不同的区块是**分叉块**：父区块已知则进入分叉池缓存，
  否则按 `ReasonParentMismatch` 拒绝。
- 当某分叉链的尖端高度**严格大于**规范链尖端时触发重组：
  1. 从分叉尖端沿父链回溯，找到与规范链的汇合点；
  2. 用撤销日志逐高度回滚汇合点之上的规范块——删掉其交易索引、
     反向应用余额增量，派生数据全部撤掉；
  3. 按正序重放分叉块。
- **原子性**：回滚与重放收集在同一个 `batch` 里，一次 `commit` 生效；
  校验（父链完整、路径连续）在进入批次前全部完成，提交本身不会失败，
  因此不存在"半新半旧"的中间态。所有校验失败都发生在任何状态修改之前。
- 重组深度有上界（`maxReorgDepth`），超出深度的分叉块被明确拒绝，
  同时给分叉池的内存占用一个上界。

## 并发与背压策略

- **读写一致性**：写路径持独占锁，读路径持读锁。单条查询（`GetTx`/`Balance`/
  `BlockAt`/`Tip`）各自原子；需要跨多次读取的一致视图时用 `View(func(s *Snapshot))`，
  回调期间持有读锁，看到的一定是某个完整提交后的状态——包括重组，
  读者只会看到重组前或重组后的整条链，不会看到回滚到一半的链。
- **背压**：`ApplyBlock` 是同步调用，调用方自然被应用速度限流，
  不存在无界堆积的内部队列。
- **批量追赶**：`CatchUp(src, batchSize)` 每批最多 `batchSize` 个区块驻留内存，
  应用完一批才拉下一批，内存占用 O(batchSize)；处理耗时随区块数线性增长
  （底层都是 O(1) 的 map/切片操作），有测试断言不出现超线性劣化。

## 本地验证

```bash
# 全部单测（含重组、重复/乱序、并发、追赶）
go test ./... -v

# 并发读写需配合竞态检测
go test ./indexer/ -race -run TestConcurrentReadWrite -v
```

测试覆盖与断言：

| 测试 | 覆盖场景 |
|---|---|
| `TestSequentialApply` | 按高度推进，余额与逐笔重放一致 |
| `TestRejectReasons` | 四类拒绝原因可区分，拒绝后状态不被污染 |
| `TestReorgRollbackAndReplay` | 分叉缓存 → 触发重组 → 旧交易撤销、新链重放，最终状态与分叉链一致 |
| `TestReorgAtomicityUnderFailure` | 父链断裂的投递被拒绝且不留下任何部分状态 |
| `TestDuplicateAndOutOfOrder` | 重复/乱序投递后状态与按序投递完全一致，无重复记账 |
| `TestConcurrentReadWrite` | 写入（含重组）与多读者并发，快照内链连续性与余额守恒恒成立（`-race`） |
| `TestCatchUpAfterOutage` | 中断后分批追赶 4900 块，批次大小有上界，结果与顺序索引一致 |
| `TestCatchUpLinearScaling` | 4 倍数据量下每块耗时不明显劣化 |

单测日志会打印触发条件与关键状态变化（拒绝原因、重组前后的尖端、
追赶批次数与每块耗时等），`go test -v` 可见。
