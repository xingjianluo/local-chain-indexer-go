# local-chain-indexer-go

本地区块数据索引器。用本地模拟的区块源（`chain.LocalSource`）代替真实节点，
在**分叉重组**、**重复/乱序投递**、**长时间中断后追赶**三种情况下，
索引结果都是一致的、可解释的。

## 结构

```
chain/    核心数据类型 + 本地模拟区块源
indexer/  索引器：原子批量写入、侧链缓存与重组、有界批追赶、一致快照读
```

## 数据模型

- `chain.Block`：`{Height, Hash, ParentHash, Txs}`，按高度推进，经父哈希链接。
- `chain.Tx`：`{ID, From, To, Amount}`。
- 索引器内部维护三张表（`indexer.store`）：
  - **规范链区块表**：高度 → 区块（只保留主链，分叉块进侧链缓存）；
  - **交易索引**：交易 ID → `{Tx, BlockHeight, BlockHash}`；
  - **派生数据**：账户 → 余额（由交易重放得出，随回滚/重放同步增减）。
- `chain.BlockSource` 抽象区块来源：`Tip()` / `BlockByHeight()` / `Subscribe()`。
  `LocalSource` 是其本地模拟实现，可注入分叉（`ForkBlocks`+`ReorgTo`）、
  重复与乱序投递（`Emit`），订阅通道缓冲固定为 16，**满了即丢**（模拟真实
  节点不保证送达），缺口由 `CatchUp` 补齐。

## 投递规则与拒绝原因

`Indexer.ApplyBlock` 对每笔投递按顺序判定，拒绝时返回 `*RejectError`，
可用 `errors.Is` 区分原因（不会报笼统错误）：

| 情形 | 处理 |
|---|---|
| 高度 = tip+1 且父哈希 = tip | 直接应用 |
| 同哈希块已在主链/侧链 | 拒绝 `ErrDuplicate` |
| 高度 > tip+1（乱序超前，父未索引） | 拒绝 `ErrParentMismatch`，等 `CatchUp` 补齐 |
| 高度 ≤ tip 且父在主链/侧链（分叉块） | 入侧链缓存，暂不生效 |
| 高度 ≤ tip 且父哈希对不上 | 拒绝 `ErrParentMismatch` |
| 替换创世块等明显回退 | 拒绝 `ErrHeightRegression` |
| 分叉点超出保留窗口（默认 64） | 拒绝 `ErrReorgTooDeep` |

重复或乱序投递因此**不会造成重复记账或数据错位**：重复的被拒绝，
超前的被拒绝后由追赶路径按高度顺序补齐。

## 重组与回滚规则

- 只切换**更长**的链：侧链块先缓存在 `side` 表，当侧链尖端高度超过主链
  tip 且能沿父哈希连回主链分叉点时，触发重组。
- 重组 = 先按逆序 `removeBlock` 回滚 `[fork+1..tip]` 的主链块（撤销其交易
  索引与余额变动），再按顺序 `applyBlock` 重放侧链段。
- **原子性**：回滚+重放在同一个批（`store.batch`）内完成。批持有写锁，
  每个修改都记录逆操作；中途任何失败执行 `abort` 按相反顺序回放逆操作，
  存储完全还原，绝不留下半新半旧的索引。`commit` 前读者看不到任何中间态。
- 追赶路径同样覆盖重组：`CatchUp` 每轮先从本地 tip 回退、与源逐高度比对
  哈希找到共同祖先，再从分叉点+1 拉取；拉到的新链块自然走侧链→重组路径。

## 并发与背压策略

- **读写并发**：写者经 `batch` 持写锁串行修改；读者经 `Snapshot()` 持读锁
  获得一致视图，视图存续期间看到的是一个完整状态（要么整批生效、要么完全
  不可见）。快照用完必须 `Close()`。
- **内存上界**：`CatchUp(ctx, source, batchSize)` 每批最多拉 `batchSize`
  个区块（默认 64），批与批之间不累积额外缓冲；订阅通道缓冲固定 16 且
  非阻塞投递（溢出即丢，靠追赶补齐），因此积压再大涨内存占用也有界。
- **复杂度**：应用单块是常数次 map 读写（均摊 O(1)），追赶 N 块为 O(N)；
  `TestCatchUpScalesLinearly` 断言 8 倍数据量下均摊耗时增长 < 4 倍，
  防止引入超线性劣化。

## 本地验证

```bash
go build ./...
go test ./... -v          # 全部单测，日志打印触发条件与关键状态变化
go test ./... -race       # 并发读写竞态检测
go test ./... -short      # 跳过扩展性耗时测试
```

测试覆盖：

- `TestReorgRollback`：分叉回滚——被替换高度的交易与余额被撤销、
  新链生效、分叉点之前不受影响；
- `TestRejectReasons`：五类拒绝原因可区分，且拒绝不改变索引状态；
- `TestDuplicateAndOutOfOrderDelivery`：重复+乱序投递后追赶补齐，最终一致；
- `TestCatchUpAfterLongOutage`：500 块积压按 32/批追赶，最终一致；
- `TestCatchUpAcrossSourceReorg`：中断期间源重组，恢复后追赶触发重组并追平；
- `TestConcurrentReadWrite`：多写者（含重组）多读者并发，快照内无中间态；
- `TestCatchUpScalesLinearly`：处理耗时随数据量近似线性。
