package indexer

import (
	"errors"
	"fmt"

	"github.com/xingjianluo/local-chain-indexer-go/chain"
)

// ReconcileOptions 配置一次对账式追赶的行为。
type ReconcileOptions struct {
	// BatchSize 是每次从源拉取的最大区块数（必须 > 0），
	// 同时是反向扫描与正向追赶的瞬时内存上界。
	BatchSize int
	// MaxDivergence 是允许的最大分叉深度（本地尖端与共同祖先的高度差）。
	// 超过该深度视为差异过大，明确报错而不动索引。0 表示用默认值 1024。
	MaxDivergence uint64
	// MaxRounds 是允许的最大对账轮数；源持续变动导致每轮都对不齐时，
	// 超过该轮数后以 ReconcileSourceUnstable 放弃。0 表示用默认值 3。
	MaxRounds int
}

// 对账参数的默认值。
const (
	defaultMaxDivergence = 1024
	defaultMaxRounds     = 3
)

// ReconcileReason 区分对账失败的原因。
type ReconcileReason int

const (
	// ReconcileFetchFailed 拉取失败：源返回错误，对账中止。
	ReconcileFetchFailed ReconcileReason = iota
	// ReconcileDivergenceTooDeep 差异过大：在允许的分叉深度内
	// 找不到本地与源的共同祖先。
	ReconcileDivergenceTooDeep
	// ReconcileSourceUnstable 源持续变动：多轮对账都无法对齐，
	// 或本地状态在对账期间被并发修改。
	ReconcileSourceUnstable
)

// String 返回失败原因的可读描述。
func (r ReconcileReason) String() string {
	switch r {
	case ReconcileFetchFailed:
		return "fetch-failed"
	case ReconcileDivergenceTooDeep:
		return "divergence-too-deep"
	case ReconcileSourceUnstable:
		return "source-unstable"
	default:
		return "unknown"
	}
}

// ReconcileError 是带原因分类的对账失败错误。
type ReconcileError struct {
	Reason ReconcileReason
	Detail string
	Err    error // 底层错误（如源的拉取错误），可为 nil
}

// Error 实现 error 接口。
func (e *ReconcileError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("reconcile failed (%s): %s: %v", e.Reason, e.Detail, e.Err)
	}
	return fmt.Sprintf("reconcile failed (%s): %s", e.Reason, e.Detail)
}

// Unwrap 返回底层错误，供 errors.Is/As 使用。
func (e *ReconcileError) Unwrap() error { return e.Err }

// errSourceChanged 是内部信号：一轮对账中途发现源（或本地状态）变了，
// 触发新一轮重试；不会暴露给调用方。
var errSourceChanged = errors.New("source changed during reconcile")

// ReconcileReport 描述一次成功对账的结果。
type ReconcileReport struct {
	ForkPoint     int64 // 共同祖先高度；-1 表示本地为空或从创世就分叉
	RolledBack    int   // 回滚掉的本地规范区块数
	Applied       int   // 从源新应用的区块数
	Rounds        int   // 实际使用的对账轮数
	AlreadySynced bool  // 本地与源本就在同一条链上，未做任何修改
}

// Reconcile 对账式追赶：对照源检查本地规范链，发现两边已分叉时
// （哪怕源的高度并不比本地高）自动回滚到共同祖先、改挂源当前的规范链，
// 最终尖端、逐高度区块、交易位置与派生余额都与源一致。
//
// 与 CatchUp 的关系：CatchUp 只做"从尖端往后顺序补齐"，源换链时会卡在
// 旧链上；Reconcile 先对账再追赶，是自愈版本。本地为空时两者等价。
//
// 原子性：整个过程在克隆出的影子状态上回滚+重放，全部完成且确认源
// 没有再变动后，才在写锁内一次性换入；任何中途失败（拉取失败、源变动、
// 差异过大）都直接丢弃影子状态，对读者可见的索引一点都不动。
// 并发读者在对账全程看到的要么是完整的旧状态，要么是完整的新状态。
//
// 有界性：反向扫描共同祖先的深度不超过 MaxDivergence；每次拉取最多
// BatchSize 个区块驻留内存；源持续变动时最多重试 MaxRounds 轮，
// 以 ReconcileSourceUnstable 明确放弃，不会无限重试。
func (ix *Indexer) Reconcile(src BlockSource, opts ReconcileOptions) (ReconcileReport, error) {
	if opts.BatchSize <= 0 {
		return ReconcileReport{}, fmt.Errorf("batchSize must be positive, got %d", opts.BatchSize)
	}
	if opts.MaxDivergence == 0 {
		opts.MaxDivergence = defaultMaxDivergence
	}
	if opts.MaxRounds == 0 {
		opts.MaxRounds = defaultMaxRounds
	}

	var lastChange error
	for round := 1; round <= opts.MaxRounds; round++ {
		rep, err := ix.reconcileRound(src, opts)
		rep.Rounds = round
		if err == nil {
			return rep, nil
		}
		if !errors.Is(err, errSourceChanged) {
			return rep, err // 拉取失败、差异过大：明确失败，不重试
		}
		lastChange = err // 源在变动：再来一轮
	}
	return ReconcileReport{Rounds: opts.MaxRounds}, &ReconcileError{
		Reason: ReconcileSourceUnstable,
		Detail: fmt.Sprintf("source or local state kept changing across %d rounds, last: %v",
			opts.MaxRounds, lastChange),
	}
}

// reconcileRound 执行一轮对账。返回的错误若以 errSourceChanged 包装，
// 表示对账中途源（或本地状态）发生变化，调用方可重试。
func (ix *Indexer) reconcileRound(src BlockSource, opts ReconcileOptions) (ReconcileReport, error) {
	var rep ReconcileReport

	// 1. 在读锁内克隆当前状态为影子 store；之后所有改动只落在影子上。
	ix.mu.RLock()
	localLen := len(ix.st.blocks)
	var localTipHash chain.Hash
	if localLen > 0 {
		localTipHash = ix.st.blocks[localLen-1].Hash
	}
	shadow := cloneStore(ix.st)
	ix.mu.RUnlock()

	localTipH := int64(localLen) - 1 // 本地为空时为 -1

	// 2. 反向扫描：在允许的分叉深度内找本地与源的共同祖先。
	//    哈希链接保证某一高度哈希相同 ⇒ 该高度之前整条链一致。
	forkPoint, err := ix.findForkPoint(src, shadow, localTipH, opts)
	if err != nil {
		return rep, err
	}
	rep.ForkPoint = forkPoint

	// 3. 影子上回滚到共同祖先（分叉点在创世之前则清空整条链）。
	if forkPoint < localTipH {
		bt := newBatch(shadow)
		if forkPoint < 0 {
			bt.rollbackAll()
		} else {
			bt.rollback(uint64(forkPoint))
		}
		bt.commit()
		rep.RolledBack = int(localTipH - forkPoint)
	}

	// 4. 正向追赶：从分叉点下一高度起分批拉取并应用到影子，
	//    每批驻留内存不超过 BatchSize；批间连续性断裂视为源在变动。
	from := uint64(forkPoint + 1)
	for {
		blocks, err := src.Blocks(from, opts.BatchSize)
		if err != nil {
			return rep, &ReconcileError{
				Reason: ReconcileFetchFailed,
				Detail: fmt.Sprintf("fetch from height %d", from),
				Err:    err,
			}
		}
		if len(blocks) == 0 {
			break // 到达源当前尖端
		}
		if err := checkContiguous(shadow, blocks, from); err != nil {
			return rep, err
		}
		bt := newBatch(shadow)
		for _, b := range blocks {
			bt.apply(b)
		}
		bt.commit()
		from += uint64(len(blocks))
		rep.Applied += len(blocks)
		if len(blocks) < opts.BatchSize {
			break // 源已耗尽
		}
	}

	// 5. 稳定性复核：确认源在我们追赶期间没有又变。
	//    哈希链接保证：分叉点与尖端的哈希都没变 ⇒ 中间整段也没变。
	if err := verifyStable(src, shadow, forkPoint); err != nil {
		return rep, err
	}

	// 6. 本来就在源的链上：无需换入，直接报告。
	if rep.RolledBack == 0 && rep.Applied == 0 {
		rep.AlreadySynced = true
		return rep, nil
	}

	// 7. 一次性换入。若本地状态在对账期间被并发写改过，放弃本轮并重试，
	//    避免覆盖掉别人的写入。
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if len(ix.st.blocks) != localLen ||
		(localLen > 0 && ix.st.blocks[localLen-1].Hash != localTipHash) {
		return rep, fmt.Errorf("local state modified concurrently: %w", errSourceChanged)
	}
	ix.st = shadow
	ix.hasTip = len(shadow.blocks) > 0
	return rep, nil
}

// findForkPoint 从本地尖端向下分批扫描，返回本地与源的共同祖先高度；
// 本地为空返回 -1；扫描到创世仍无共同高度也返回 -1（整链替换）。
// 在 MaxDivergence 深度内找不到时返回 ReconcileDivergenceTooDeep。
func (ix *Indexer) findForkPoint(src BlockSource, shadow *store, localTipH int64, opts ReconcileOptions) (int64, error) {
	if localTipH < 0 {
		return -1, nil
	}
	lowest := localTipH - int64(opts.MaxDivergence) // 允许的最浅共同祖先
	if lowest < 0 {
		lowest = 0
	}
	high := localTipH
	for {
		lo := high - int64(opts.BatchSize) + 1
		if lo < lowest {
			lo = lowest
		}
		blocks, err := src.Blocks(uint64(lo), int(high-lo+1))
		if err != nil {
			return -1, &ReconcileError{
				Reason: ReconcileFetchFailed,
				Detail: fmt.Sprintf("scan window [%d, %d]", lo, high),
				Err:    err,
			}
		}
		// 源可能比本地短：返回的批次不足时，已返回部分仍是 [lo, lo+len)。
		// 从高端向低端比较，找到的第一个匹配即最浅的共同祖先。
		for i := len(blocks) - 1; i >= 0; i-- {
			b := blocks[i]
			if b.Height != uint64(lo)+uint64(i) || int64(b.Height) > localTipH {
				return -1, fmt.Errorf("source returned non-sequential block at scan window [%d, %d]: %w",
					lo, high, errSourceChanged)
			}
			if shadow.blocks[b.Height].Hash == b.Hash {
				return int64(b.Height), nil
			}
		}
		if lo == 0 {
			return -1, nil // 扫到创世都不一致：整链替换（深度在允许范围内）
		}
		if lo == lowest {
			return -1, &ReconcileError{
				Reason: ReconcileDivergenceTooDeep,
				Detail: fmt.Sprintf("no common ancestor within %d heights below local tip %d",
					opts.MaxDivergence, localTipH),
			}
		}
		high = lo - 1
	}
}

// checkContiguous 校验一批拉取到的区块高度连续、父哈希链接，
// 且首块能接上影子链的当前尖端；不满足说明源在拉取过程中变了。
func checkContiguous(shadow *store, blocks []chain.Block, from uint64) error {
	for i, b := range blocks {
		wantH := from + uint64(i)
		if b.Height != wantH {
			return fmt.Errorf("source returned height %d, want %d: %w", b.Height, wantH, errSourceChanged)
		}
		if wantH == 0 {
			continue // 创世块无父可校验
		}
		var wantParent chain.Hash
		if i > 0 {
			wantParent = blocks[i-1].Hash
		} else {
			wantParent = shadow.blocks[len(shadow.blocks)-1].Hash
		}
		if b.ParentHash != wantParent {
			return fmt.Errorf("block at height %d does not link to predecessor: %w", wantH, errSourceChanged)
		}
	}
	return nil
}

// verifyStable 在正向追赶结束后复核源没有变动：
// 尖端之后没有更新区块、共同祖先与尖端的哈希都仍与源一致。
func verifyStable(src BlockSource, shadow *store, forkPoint int64) error {
	shadowTipH := int64(len(shadow.blocks)) - 1

	// 源没有继续增长。
	probe, err := src.Blocks(uint64(shadowTipH+1), 1)
	if err != nil {
		return &ReconcileError{
			Reason: ReconcileFetchFailed,
			Detail: fmt.Sprintf("stability probe at height %d", shadowTipH+1),
			Err:    err,
		}
	}
	if len(probe) > 0 {
		return fmt.Errorf("source advanced past height %d during reconcile: %w",
			shadowTipH, errSourceChanged)
	}

	// 共同祖先与尖端两个端点的哈希都没变（哈希链接保证中间整段也没变）。
	for _, h := range []int64{forkPoint, shadowTipH} {
		if h < 0 {
			continue
		}
		probe, err := src.Blocks(uint64(h), 1)
		if err != nil {
			return &ReconcileError{
				Reason: ReconcileFetchFailed,
				Detail: fmt.Sprintf("stability probe at height %d", h),
				Err:    err,
			}
		}
		if len(probe) != 1 || probe[0].Hash != shadow.blocks[h].Hash {
			return fmt.Errorf("source block at height %d changed during reconcile: %w",
				h, errSourceChanged)
		}
	}
	return nil
}
