package indexer

import (
	"errors"
	"fmt"

	"github.com/xingjianluo/local-chain-indexer-go/chain"
)

// ReconcileErrorKind 区分对账失败的两类可控原因。
type ReconcileErrorKind int

const (
	// KindDivergenceTooDeep 本地与源的差异（需要回滚的本地高度数）
	// 超过配置的上限 maxReconcileDepth。
	KindDivergenceTooDeep ReconcileErrorKind = iota + 1
	// KindSourceUnstable 源在对账过程中持续变动（尖端高度/哈希对不齐），
	// 重试耗尽后仍无法收敛。
	KindSourceUnstable
)

// String 返回对账失败原因的可读描述。
func (k ReconcileErrorKind) String() string {
	switch k {
	case KindDivergenceTooDeep:
		return "divergence-too-deep"
	case KindSourceUnstable:
		return "source-unstable"
	default:
		return "unknown"
	}
}

// ReconcileError 是对账失败的可分类错误；用 errors.As 取出后按 Kind 区分。
type ReconcileError struct {
	Kind   ReconcileErrorKind
	Detail string
}

// Error 实现 error 接口。
func (e *ReconcileError) Error() string {
	return fmt.Sprintf("reconcile failed (%s): %s", e.Kind, e.Detail)
}

// ReconcileReport 描述一次对账的结果，便于调用方观测触发了哪种情况。
type ReconcileReport struct {
	AlreadyInSync bool         // 本地与源已在同一条链上，未做任何修改
	FromGenesis   bool         // 汇合点在创世之前（本地为空，或整条链都被替换）
	ForkHeight    uint64       // 汇合点高度（FromGenesis 时无意义）
	RolledBack    int          // 被撤销的本地区块数
	Applied       int          // 新落上的源区块数
	OldTip        chain.Header // 对账前的本地尖端（原本为空则为零值）
	NewTip        chain.Header // 对账后的本地尖端（= 源尖端；源为空则为零值）
	Attempts      int          // 实际尝试次数（源/本地中途变动会导致重试）
}

// errReconcileChanged 是内部哨兵：对账过程中检测到源或本地链发生变动，
// 本次尝试作废（状态未被修改），由外层决定是否重试。
var errReconcileChanged = errors.New("chain changed during reconcile")

// Reconcile 对账式追赶：对比本地与源的规范链，发现不在同一条链上时
// （包括源换链后高度并不比本地高的情况），自动回滚到汇合点并换上源的链，
// 最终尖端、逐高度区块、交易位置与余额都与源一致；已一致则直接返回。
//
// 原子性：撤旧链与落新链在影子状态上构建完成后一次原子换入，
// 中途任何失败（拉取失败、源变动、差异超限）都不会改动当前索引；
// 并发读者只会看到完整的旧状态或完整的新状态。
//
// 资源上界：拉取按 batchSize 分批，瞬时拉取缓冲 O(batchSize)；
// 影子状态与最终索引同阶（索引本身全量驻留内存），与源总数据量无关；
// 各阶段都是逐块 O(1) 操作，总耗时随数据量线性增长。
func (ix *Indexer) Reconcile(src BlockSource, batchSize int) (ReconcileReport, error) {
	if batchSize <= 0 {
		return ReconcileReport{}, fmt.Errorf("batchSize must be positive, got %d", batchSize)
	}
	maxAttempts := 1 + ix.reconcileRetries
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		rep, err := ix.reconcileOnce(src, batchSize)
		if err == nil {
			rep.Attempts = attempt
			return rep, nil
		}
		if !errors.Is(err, errReconcileChanged) {
			// 拉取失败、差异过大等：立即返回，状态未动。
			return rep, err
		}
		lastErr = err
	}
	return ReconcileReport{}, &ReconcileError{
		Kind:   KindSourceUnstable,
		Detail: fmt.Sprintf("chain kept changing across %d attempts: %v", maxAttempts, lastErr),
	}
}

// reconcileOnce 执行一轮对账；检测到中途变动时返回 errReconcileChanged。
func (ix *Indexer) reconcileOnce(src BlockSource, batchSize int) (ReconcileReport, error) {
	var rep ReconcileReport

	// 1. 本地尖端快照（整个对账过程以此为准，提交前会再校验它没变）。
	ix.mu.RLock()
	oldTip, hasLocal := ix.tipLocked()
	ix.mu.RUnlock()
	rep.OldTip = oldTip
	startFrom := uint64(0)
	if hasLocal {
		startFrom = oldTip.Height + 1
	}

	// 2. 探测源尖端。
	srcTip, hasSrc, err := probeSourceTip(src, startFrom, batchSize)
	if err != nil {
		return rep, fmt.Errorf("probe source tip: %w", err)
	}

	// 3. 已一致：双方都空，或尖端哈希相同（高度必然也相同）。
	//    注意必须比哈希：源换链但高度不更高时，单看高度会误判为无需对账。
	if !hasSrc && !hasLocal {
		rep.AlreadyInSync = true
		return rep, nil
	}
	if hasSrc && hasLocal && srcTip.Hash == oldTip.Hash {
		rep.AlreadyInSync = true
		rep.NewTip = srcTip
		return rep, nil
	}

	// 4. 找汇合点，算出需要回滚的本地高度数（差异深度）。
	fromGenesis := !hasLocal
	var forkH, divergence uint64
	switch {
	case !hasLocal:
		// 本地为空：从创世构建，无回滚。
	case !hasSrc:
		// 源为空：整条本地链都要撤掉。
		fromGenesis = true
		divergence = oldTip.Height + 1
	default:
		fp, all, err := ix.findForkPoint(src, min(oldTip.Height, srcTip.Height), batchSize)
		if err != nil {
			return rep, err
		}
		fromGenesis = all
		forkH = fp
		if all {
			divergence = oldTip.Height + 1
		} else {
			divergence = oldTip.Height - fp
		}
	}
	if divergence > ix.maxReconcileDepth {
		return rep, &ReconcileError{
			Kind: KindDivergenceTooDeep,
			Detail: fmt.Sprintf("local tip %d diverges from source by %d heights, beyond max reconcile depth %d",
				oldTip.Height, divergence, ix.maxReconcileDepth),
		}
	}
	rep.FromGenesis = fromGenesis
	rep.ForkHeight = forkH
	rep.RolledBack = int(divergence)

	// 5. 影子起点：把当前状态拷贝并截断到汇合点（或从空开始）。
	//    拷贝在读锁内完成；之后分批拉取不持锁，不阻塞读写。
	ix.mu.RLock()
	curTip, curHas := ix.tipLocked()
	if curHas != hasLocal || (hasLocal && curTip.Hash != oldTip.Hash) {
		ix.mu.RUnlock()
		return rep, errReconcileChanged // 本地被并发修改
	}
	var shadow *store
	if fromGenesis {
		shadow = newStore()
	} else {
		shadow = ix.truncatedCopyLocked(forkH)
	}
	ix.mu.RUnlock()

	// 6. 分批拉取源区块落到影子：每批校验高度连续、父哈希链接，
	//    任何断裂都说明源在变动，本次尝试作废。
	next := uint64(0)
	var prevHash chain.Hash
	if !fromGenesis {
		next = forkH + 1
		prevHash = shadow.blocks[forkH].Hash
	}
	for hasSrc && next <= srcTip.Height {
		blocks, err := src.Blocks(next, batchSize)
		if err != nil {
			return rep, fmt.Errorf("fetch from height %d: %w", next, err)
		}
		if len(blocks) == 0 {
			return rep, errReconcileChanged // 源缩短了
		}
		bt := newBatch(shadow)
		for _, b := range blocks {
			if b.Height != next || (b.Height > 0 && b.ParentHash != prevHash) {
				return rep, errReconcileChanged
			}
			bt.apply(b)
			prevHash = b.Hash
			next++
		}
		bt.commit()
		rep.Applied += len(blocks)
	}

	// 7. 影子尖端必须与先前探测到的源尖端一致。
	if hasSrc && (next == 0 || next-1 != srcTip.Height || prevHash != srcTip.Hash) {
		return rep, errReconcileChanged
	}

	// 8. 稳定性复检：源尖端在对账期间没有继续移动。
	stable, err := sourceTipUnchanged(src, srcTip, hasSrc)
	if err != nil {
		return rep, fmt.Errorf("recheck source tip: %w", err)
	}
	if !stable {
		return rep, errReconcileChanged
	}

	// 9. 原子换入：确认本地未被并发修改后整体替换状态。
	ix.mu.Lock()
	curTip, curHas = ix.tipLocked()
	if curHas != hasLocal || (hasLocal && curTip.Hash != oldTip.Hash) {
		ix.mu.Unlock()
		return rep, errReconcileChanged // 本地被并发修改
	}
	ix.st = shadow
	ix.hasTip = hasSrc
	ix.mu.Unlock()

	rep.NewTip = srcTip
	return rep, nil
}

// probeSourceTip 探测源当前尖端。startFrom 是本地尖端的下一高度（本地为空为 0）：
// 该高度为空则向下二分找尖端，非空则向上批扫描到短批为止。
func probeSourceTip(src BlockSource, startFrom uint64, batchSize int) (chain.Header, bool, error) {
	one, err := src.Blocks(startFrom, 1)
	if err != nil {
		return chain.Header{}, false, err
	}
	if len(one) == 0 {
		// 源尖端低于 startFrom（或源为空）：二分最大的非空高度。
		first, err := src.Blocks(0, 1)
		if err != nil {
			return chain.Header{}, false, err
		}
		if len(first) == 0 {
			return chain.Header{}, false, nil // 源为空
		}
		lo, hi := uint64(0), startFrom-1 // 不变式：lo 非空，hi+1 为空
		for lo < hi {
			mid := (lo + hi + 1) / 2
			b, err := src.Blocks(mid, 1)
			if err != nil {
				return chain.Header{}, false, err
			}
			if len(b) == 0 {
				hi = mid - 1
			} else {
				lo = mid
			}
		}
		b, err := src.Blocks(lo, 1)
		if err != nil {
			return chain.Header{}, false, err
		}
		return b[0].Header(), true, nil
	}
	// 源尖端不低于 startFrom：向上批扫描直到空批或短批。
	from, last := startFrom, one[0]
	for {
		blocks, err := src.Blocks(from, batchSize)
		if err != nil {
			return chain.Header{}, false, err
		}
		if len(blocks) == 0 {
			break // 尖端是上一批的末块
		}
		last = blocks[len(blocks)-1]
		if len(blocks) < batchSize {
			break
		}
		from += uint64(len(blocks))
	}
	return last.Header(), true, nil
}

// findForkPoint 从高度 h 向下按批比对本地与源的区块哈希，返回汇合点高度；
// 第二个返回值为 true 表示连创世都对不上（整条本地链都要替换）。
func (ix *Indexer) findForkPoint(src BlockSource, h uint64, batchSize int) (uint64, bool, error) {
	for {
		from := uint64(0)
		if h+1 > uint64(batchSize) {
			from = h + 1 - uint64(batchSize)
		}
		blocks, err := src.Blocks(from, int(h-from+1))
		if err != nil {
			return 0, false, fmt.Errorf("fetch fork window [%d,%d]: %w", from, h, err)
		}
		if uint64(len(blocks)) != h-from+1 {
			return 0, false, errReconcileChanged // 窗口拉不满：源在变动
		}
		for i := len(blocks) - 1; i >= 0; i-- {
			b := blocks[i]
			if b.Height != from+uint64(i) {
				return 0, false, errReconcileChanged // 高度不连续：源在变动
			}
			local, ok := ix.BlockAt(b.Height)
			if !ok {
				return 0, false, errReconcileChanged // 本地被并发改短
			}
			if local.Hash == b.Hash {
				return b.Height, false, nil
			}
		}
		if from == 0 {
			return 0, true, nil // 创世都对不上
		}
		h = from - 1
	}
}

// truncatedCopyLocked 拷贝当前状态并截断到 height（含），作为影子构建的起点。
// 派生数据（交易索引、余额）用撤销日志退回到汇合点。
func (ix *Indexer) truncatedCopyLocked(height uint64) *store {
	st := ix.st
	sh := newStore()
	sh.blocks = append(sh.blocks, st.blocks[:height+1]...)
	sh.undos = append(sh.undos, st.undos[:height+1]...)
	for k, v := range st.txIndex {
		sh.txIndex[k] = v
	}
	for k, v := range st.balances {
		sh.balances[k] = v
	}
	for h := uint64(len(st.blocks)) - 1; h > height; h-- {
		u := st.undos[h]
		for _, th := range u.txs {
			delete(sh.txIndex, th)
		}
		for _, d := range u.deltas {
			sh.balances[d.addr] += d.delta
		}
	}
	return sh
}

// sourceTipUnchanged 复检源尖端是否仍是先前探测到的值。
func sourceTipUnchanged(src BlockSource, tip chain.Header, hasTip bool) (bool, error) {
	if !hasTip {
		any, err := src.Blocks(0, 1)
		if err != nil {
			return false, err
		}
		return len(any) == 0, nil
	}
	after, err := src.Blocks(tip.Height+1, 1)
	if err != nil {
		return false, err
	}
	if len(after) != 0 {
		return false, nil // 源又长高了
	}
	cur, err := src.Blocks(tip.Height, 1)
	if err != nil {
		return false, err
	}
	return len(cur) == 1 && cur[0].Hash == tip.Hash, nil
}
