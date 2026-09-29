// Package indexer 提供本地区块数据索引器：按高度应用区块、
// 支持链重组回滚与重放、并发读写一致性和有界积压追赶。
package indexer

import (
	"fmt"
	"sync"

	"github.com/xingjianluo/local-chain-indexer-go/chain"
)

// Indexer 是本地区块索引器，并发安全。
// 写路径（ApplyBlock）持独占锁，读路径（查询）持读锁，
// 因此读者永远看到某个完整提交后的状态，不会看到写了一半的中间态。
type Indexer struct {
	mu     sync.RWMutex
	st     *store
	hasTip bool
	// maxReorgDepth 是允许的最大重组深度；落后于此深度的区块
	// 会被以 ReasonHeightRegression 明确拒绝，同时给分叉池内存一个上界。
	maxReorgDepth uint64
}

// Option 是 Indexer 的可选配置。
type Option func(*Indexer)

// WithMaxReorgDepth 设置允许的最大重组深度（默认 128）。
func WithMaxReorgDepth(d uint64) Option {
	return func(ix *Indexer) { ix.maxReorgDepth = d }
}

// New 创建一个空索引器。
func New(opts ...Option) *Indexer {
	ix := &Indexer{st: newStore(), maxReorgDepth: 128}
	for _, o := range opts {
		o(ix)
	}
	return ix
}

// Tip 返回当前索引尖端；第二个返回值表示是否已有任何区块。
func (ix *Indexer) Tip() (chain.Header, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.tipLocked()
}

func (ix *Indexer) tipLocked() (chain.Header, bool) {
	if !ix.hasTip {
		return chain.Header{}, false
	}
	return ix.st.blocks[len(ix.st.blocks)-1].Header(), true
}

// Height 返回当前尖端高度；无区块时返回 0 且 ok=false。
func (ix *Indexer) Height() (uint64, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if !ix.hasTip {
		return 0, false
	}
	return uint64(len(ix.st.blocks)) - 1, true
}

// ApplyBlock 校验并应用一个区块。
// 拒绝时返回 *chain.RejectError，其 Reason 区分：
// 高度回退、父区块对不上、重复投递、高度空洞。
// 触发重组时，回滚与重放在同一把写锁内完成，对外表现为原子切换。
func (ix *Indexer) ApplyBlock(b chain.Block) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.applyLocked(b)
}

func (ix *Indexer) applyLocked(b chain.Block) error {
	st := ix.st

	// 第一个区块：必须落在高度 0，作为索引起点。
	if !ix.hasTip {
		if b.Height != 0 {
			return &chain.RejectError{
				Reason: chain.ReasonGap, Height: b.Height, Hash: b.Hash,
				Detail: fmt.Sprintf("first block must be at height 0, got %d", b.Height),
			}
		}
		bt := newBatch(st)
		bt.apply(b)
		bt.commit()
		ix.hasTip = true
		return nil
	}

	tipH := uint64(len(st.blocks)) - 1
	tipHash := st.blocks[tipH].Hash

	switch {
	case b.Height == tipH+1 && b.ParentHash == tipHash:
		// 常规推进：直接延长规范链。
		bt := newBatch(st)
		bt.apply(b)
		bt.commit()
		return nil

	case b.Height > tipH+1:
		// 高度空洞：跳过了尚未索引的高度。
		return &chain.RejectError{
			Reason: chain.ReasonGap, Height: b.Height, Hash: b.Hash,
			Detail: fmt.Sprintf("tip at %d, block jumps to %d", tipH, b.Height),
		}

	case b.Height == tipH+1:
		// 高度紧邻尖端但父哈希不是尖端：分叉链的延伸。
		if !ix.parentKnownLocked(b) {
			return &chain.RejectError{
				Reason: chain.ReasonParentMismatch, Height: b.Height, Hash: b.Hash,
				Detail: fmt.Sprintf("parent %s is neither tip %s nor a known fork block",
					b.ParentHash, tipHash),
			}
		}
		st.forkPool[b.Hash] = b
		return ix.maybeReorgLocked(b)
	}

	// 以下 b.Height <= tipH：可能是重复投递或分叉区块。
	canonical := st.blocks[b.Height]
	if canonical.Hash == b.Hash {
		return &chain.RejectError{
			Reason: chain.ReasonDuplicate, Height: b.Height, Hash: b.Hash,
			Detail: "identical block already indexed",
		}
	}

	// 高度回退：落后尖端超过最大重组深度，明确拒绝。
	if tipH-b.Height > ix.maxReorgDepth {
		return &chain.RejectError{
			Reason: chain.ReasonHeightRegression, Height: b.Height, Hash: b.Hash,
			Detail: fmt.Sprintf("block is %d heights behind tip %d, beyond max reorg depth %d",
				tipH-b.Height, tipH, ix.maxReorgDepth),
		}
	}

	// 分叉区块：已入池的同哈希区块算重复投递。
	if _, ok := st.forkPool[b.Hash]; ok {
		return &chain.RejectError{
			Reason: chain.ReasonDuplicate, Height: b.Height, Hash: b.Hash,
			Detail: "identical fork block already buffered",
		}
	}

	// 校验其父区块已知（规范链上或分叉池里）。
	if !ix.parentKnownLocked(b) {
		return &chain.RejectError{
			Reason: chain.ReasonParentMismatch, Height: b.Height, Hash: b.Hash,
			Detail: fmt.Sprintf("parent %s unknown at height %d", b.ParentHash, b.Height-1),
		}
	}

	// 入分叉池；若该分叉因此变得比规范链更长，则触发重组。
	st.forkPool[b.Hash] = b
	return ix.maybeReorgLocked(b)
}

// parentKnownLocked 检查分叉区块的父区块是否已知。
func (ix *Indexer) parentKnownLocked(b chain.Block) bool {
	if b.Height == 0 {
		return true // 分叉创世块
	}
	st := ix.st
	if b.Height-1 < uint64(len(st.blocks)) && st.blocks[b.Height-1].Hash == b.ParentHash {
		return true
	}
	if p, ok := st.forkPool[b.ParentHash]; ok && p.Height == b.Height-1 {
		return true
	}
	return false
}

// maybeReorgLocked 在以 b 为尖端的分叉严格长于规范链时执行重组。
// 整个回滚+重放在同一个 batch 中收集、一次 commit，要么全部生效要么不动。
func (ix *Indexer) maybeReorgLocked(tip chain.Block) error {
	st := ix.st
	tipH := uint64(len(st.blocks)) - 1
	if tip.Height <= tipH {
		return nil // 分叉尚不够长，仅缓存
	}

	// 从分叉尖端沿父链回溯，收集整条分叉路径，直到落在规范链上。
	path := []chain.Block{tip}
	for {
		cur := path[len(path)-1]
		if cur.Height == 0 {
			break // 回溯到创世
		}
		if cur.Height-1 < uint64(len(st.blocks)) && st.blocks[cur.Height-1].Hash == cur.ParentHash {
			break // 找到与规范链的汇合点
		}
		parent, ok := st.forkPool[cur.ParentHash]
		if !ok || parent.Height != cur.Height-1 {
			// 路径断裂：前置校验已保证不会发生，防御性返回。
			return &chain.RejectError{
				Reason: chain.ReasonParentMismatch, Height: cur.Height, Hash: cur.Hash,
				Detail: "fork path broken during reorg",
			}
		}
		path = append(path, parent)
	}

	// 汇合点高度 = 路径末端高度 - 1（或分叉创世为 0 时无回滚）。
	forkPoint := uint64(0)
	last := path[len(path)-1]
	if last.Height > 0 {
		forkPoint = last.Height - 1
	}

	// 一个批次完成：回滚到汇合点，再按正序重放分叉区块。
	bt := newBatch(st)
	if last.Height > 0 {
		bt.rollback(forkPoint)
	} else {
		bt.rollbackAll() // 分叉从创世开始：清空整条链
	}
	for i := len(path) - 1; i >= 0; i-- {
		bt.apply(path[i])
	}
	bt.commit()

	// 成为规范链的区块移出分叉池。
	for _, blk := range path {
		delete(st.forkPool, blk.Hash)
	}
	return nil
}
