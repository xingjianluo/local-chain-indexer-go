package indexer

import (
	"context"
	"errors"
	"log"

	"github.com/xingjianluo/local-chain-indexer-go/chain"
)

// 默认配置。
const (
	defaultMaxReorgDepth = 64 // 允许回滚的最大高度数
	defaultBatchSize     = 64 // 追赶时每批拉取的区块数上界
)

// Indexer 是本地区块数据索引器。写路径（ApplyBlock / CatchUp）
// 与读路径（Snapshot）可并发，读侧永远看到完整状态。
//
// 分叉处理策略（与"只切换更长链"一致）：
//   - 落在主链 tip 之后的下一块：校验父哈希后直接应用；
//   - 高度不超过 tip 的异哈希块：作为侧链候选缓存，不立即生效；
//   - 侧链接到主链分叉点、且侧链尖端高度超过主链 tip 时触发重组：
//     在同一原子批内回滚主链尾段并重放侧链段；
//   - 超前块（父尚未索引）拒绝，由 CatchUp 按高度补齐。
type Indexer struct {
	st            *store
	side          map[chain.Hash]chain.Block // 侧链缓存（仅写锁内访问）
	maxReorgDepth uint64
	logf          func(format string, args ...any)
}

// New 创建索引器并以 genesis 作为索引起点（高度 0）。
func New(genesis chain.Block) *Indexer {
	ix := &Indexer{
		st:            newStore(),
		side:          make(map[chain.Hash]chain.Block),
		maxReorgDepth: defaultMaxReorgDepth,
		logf:          log.Printf,
	}
	bt := ix.st.begin()
	bt.applyBlock(genesis)
	bt.commit()
	return ix
}

// SetLogger 替换日志函数（测试可注入以打印触发条件与状态变化）。
func (ix *Indexer) SetLogger(f func(format string, args ...any)) { ix.logf = f }

// SetMaxReorgDepth 设置允许的最大重组深度。
func (ix *Indexer) SetMaxReorgDepth(d uint64) { ix.maxReorgDepth = d }

// ApplyBlock 应用一个区块投递。重复、乱序、落后、父区块对不上
// 的投递会被拒绝并返回可区分的 *RejectError；触发重组时先回滚
// 被替换高度再重放新链，整个过程原子生效。
func (ix *Indexer) ApplyBlock(b chain.Block) error {
	bt := ix.st.begin()
	st := ix.st

	reject := func(reason error, detail string) error {
		err := &RejectError{Reason: reason, Height: b.Height, Hash: b.Hash.String(), Detail: detail}
		ix.logf("[indexer] 拒绝区块 h=%d %s 原因=%v 说明=%s", b.Height, b.Hash, reason, detail)
		bt.abort()
		return err
	}

	tipH, tipHash, _ := st.tipHeight, st.tipHash, st.hasTip

	// 1. 重复投递：主链或侧链中已有同哈希块。
	if cur, ok := st.blocks[b.Height]; ok && cur.Hash == b.Hash {
		return reject(ErrDuplicate, "该哈希已在主链中")
	}
	if _, ok := ix.side[b.Hash]; ok {
		return reject(ErrDuplicate, "该哈希已在侧链缓存中")
	}

	// 2. 主链直接延伸。
	if b.Height == tipH+1 && b.ParentHash == tipHash {
		bt.applyBlock(b)
		bt.commit()
		ix.logf("[indexer] 应用主链块 h=%d %s 交易数=%d", b.Height, b.Hash, len(b.Txs))
		return nil
	}

	// 3. 超前块（乱序投递）：父尚未索引，拒绝并等待追赶补齐。
	if b.Height > tipH+1 {
		return reject(ErrParentMismatch, "父区块尚未索引（投递超前，需 CatchUp 补齐）")
	}

	// 4. b.Height <= tipH+1 但未直接延伸：分叉候选或落后块。
	if b.Height == 0 {
		return reject(ErrHeightRegression, "创世块已确定，不接受替代")
	}
	depth := tipH - b.Height + 1
	if depth > ix.maxReorgDepth {
		return reject(ErrReorgTooDeep, "分叉点超出保留历史窗口")
	}
	// 父必须在主链（分叉点）或侧链（延伸侧链）上。
	_, parentOnSide := ix.side[b.ParentHash]
	parent, parentAtHeight := st.blocks[b.Height-1]
	parentOnMain := parentAtHeight && parent.Hash == b.ParentHash
	if !parentOnMain && !parentOnSide {
		if parentAtHeight {
			return reject(ErrParentMismatch, "父哈希与主链同高度块不一致")
		}
		return reject(ErrParentMismatch, "父区块未知（既不在主链也不在侧链）")
	}

	// 缓存进侧链。
	ix.side[b.Hash] = b
	ix.logf("[indexer] 侧链缓存 h=%d %s（主链 tip=%d）", b.Height, b.Hash, tipH)

	// 5. 侧链尖端未超过主链 tip：仅缓存，等待链变长。
	if b.Height <= tipH {
		bt.commit()
		return nil
	}

	// 6. 触发重组：从 b 沿侧链上溯到主链分叉点，收集待重放段。
	var seg []chain.Block
	cur := b
	var forkH uint64
	for {
		seg = append([]chain.Block{cur}, seg...)
		parent, ok := ix.side[cur.ParentHash]
		if ok {
			cur = parent
			continue
		}
		// 父必须在主链上（加入侧链时已归纳保证）。
		mainParent := st.blocks[cur.Height-1]
		if mainParent.Hash != cur.ParentHash {
			return reject(ErrParentMismatch, "侧链无法连回主链")
		}
		forkH = cur.Height - 1
		break
	}
	if tipH-forkH > ix.maxReorgDepth {
		return reject(ErrReorgTooDeep, "重组深度超过上限")
	}

	ix.logf("[indexer] 触发重组：分叉点 h=%d，回滚 %d 块，重放 %d 块",
		forkH, tipH-forkH, len(seg))
	// 同一原子批内：先回滚主链尾段，再重放侧链段。
	for h := tipH; h > forkH; h-- {
		bt.removeBlock(st.blocks[h])
	}
	for _, blk := range seg {
		bt.applyBlock(blk)
		delete(ix.side, blk.Hash)
	}
	bt.commit()
	ix.logf("[indexer] 重组完成：新 tip h=%d %s", b.Height, b.Hash)
	return nil
}

// Snapshot 返回当前索引状态的一致只读视图，可安全并发使用。
// 用完必须 Close。
func (ix *Indexer) Snapshot() *snapshot { return ix.st.snapshot() }

// CatchUp 从 source 追赶积压：从当前索引高度+1 起按批拉取区块，
// 每批最多 batchSize 个（内存占用有界），直到追上 source 的 Tip
// 或 ctx 取消。返回本次追赶应用的区块数。
func (ix *Indexer) CatchUp(ctx context.Context, source chain.BlockSource, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	total := 0
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		sn := ix.st.snapshot()
		tipH, _, _ := sn.Tip()
		sn.Close()
		srcTipH, _ := source.Tip()
		if tipH >= srcTipH {
			return total, nil
		}
		end := tipH + uint64(batchSize)
		if end > srcTipH {
			end = srcTipH
		}
		applied := 0
		for h := tipH + 1; h <= end; h++ {
			blk, ok := source.BlockByHeight(h)
			if !ok {
				break // 源侧链变化，跳出重估
			}
			if err := ix.ApplyBlock(blk); err != nil {
				if errors.Is(err, ErrDuplicate) {
					continue // 与并发订阅投递重叠，安全跳过
				}
				return total, err
			}
			applied++
		}
		total += applied
		ix.logf("[indexer] 追赶批次 [%d..%d] 应用 %d 块，累计 %d", tipH+1, end, applied, total)
		if applied == 0 {
			return total, nil // 无法推进（如源重组中），避免空转
		}
	}
}
