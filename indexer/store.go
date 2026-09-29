package indexer

import (
	"sync"

	"github.com/xingjianluo/local-chain-indexer-go/chain"
)

// txRecord 是交易索引中的一条记录。
type txRecord struct {
	Tx          chain.Tx
	BlockHeight uint64
	BlockHash   chain.Hash
}

// store 是索引器的底层存储：规范链区块、交易索引与派生数据（余额）。
//
// 一致性模型：
//   - 所有修改通过 begin 开启的批（batch）进行，批持有写锁；
//     一批修改（含重组的回滚+重放）要么 commit 整体生效，
//     要么 abort 通过逆操作完全还原，绝不留下半新半旧的状态。
//   - 读侧通过 snapshot 获取读锁保护的一致视图，
//     视图存续期间写者被阻塞，读者看到的永远是完整状态。
type store struct {
	mu        sync.RWMutex
	blocks    map[uint64]chain.Block // 规范链：高度 -> 区块
	txs       map[chain.Hash]txRecord
	balances  map[string]int64
	tipHeight uint64
	tipHash   chain.Hash
	hasTip    bool
}

func newStore() *store {
	return &store{
		blocks:   make(map[uint64]chain.Block),
		txs:      make(map[chain.Hash]txRecord),
		balances: make(map[string]int64),
	}
}

// batch 是一批原子修改。begin 后持有写锁，
// 必须恰好调用一次 commit 或 abort。
type batch struct {
	s    *store
	undo []func() // 逆操作，abort 时按相反顺序执行
	done bool
}

// begin 开启一批修改（获取写锁，阻塞其他写者与读者）。
func (s *store) begin() *batch {
	s.mu.Lock()
	return &batch{s: s}
}

// commit 使本批修改整体生效并释放写锁。
func (b *batch) commit() {
	if b.done {
		panic("batch already finished")
	}
	b.done = true
	b.s.mu.Unlock()
}

// abort 按相反顺序执行逆操作，把存储还原到 begin 之前的状态。
func (b *batch) abort() {
	if b.done {
		panic("batch already finished")
	}
	b.done = true
	for i := len(b.undo) - 1; i >= 0; i-- {
		b.undo[i]()
	}
	b.s.mu.Unlock()
}

// applyBlock 把区块写入规范链并应用其派生数据（调用前须已完成校验）。
func (b *batch) applyBlock(blk chain.Block) {
	s := b.s
	if old, ok := s.blocks[blk.Height]; ok {
		b.removeBlock(old) // 同高度旧块先撤掉（重组路径）
	}
	// 在修改前捕获旧 tip，供逆操作恢复。
	prevH, prevHash, prevHas := s.tipHeight, s.tipHash, s.hasTip
	s.blocks[blk.Height] = blk
	s.tipHeight, s.tipHash, s.hasTip = blk.Height, blk.Hash, true
	for _, tx := range blk.Txs {
		if _, exists := s.txs[tx.ID]; !exists {
			s.txs[tx.ID] = txRecord{Tx: tx, BlockHeight: blk.Height, BlockHash: blk.Hash}
			b.undo = append(b.undo, func() { delete(s.txs, tx.ID) })
		}
		s.balances[tx.From] -= tx.Amount
		s.balances[tx.To] += tx.Amount
		from, to, amt := tx.From, tx.To, tx.Amount
		b.undo = append(b.undo, func() {
			s.balances[from] += amt
			s.balances[to] -= amt
		})
	}
	b.undo = append(b.undo, func() {
		delete(s.blocks, blk.Height)
		s.tipHeight, s.tipHash, s.hasTip = prevH, prevHash, prevHas
	})
}

// removeBlock 撤销一个已索引区块及其派生数据。
func (b *batch) removeBlock(blk chain.Block) {
	s := b.s
	for i := len(blk.Txs) - 1; i >= 0; i-- {
		tx := blk.Txs[i]
		s.balances[tx.From] += tx.Amount
		s.balances[tx.To] -= tx.Amount
		delete(s.txs, tx.ID)
	}
	delete(s.blocks, blk.Height)
	// 恢复 tip 到父块。
	if blk.Height == 0 {
		s.hasTip = false
		s.tipHash = chain.Hash{}
		s.tipHeight = 0
	} else if parent, ok := s.blocks[blk.Height-1]; ok {
		s.tipHeight, s.tipHash, s.hasTip = parent.Height, parent.Hash, true
	}
	// 逆操作：把该块原样加回（不经过 applyBlock，避免重复记账检查干扰）。
	undoBlk := blk
	b.undo = append(b.undo, func() {
		s.blocks[undoBlk.Height] = undoBlk
		for _, tx := range undoBlk.Txs {
			s.txs[tx.ID] = txRecord{Tx: tx, BlockHeight: undoBlk.Height, BlockHash: undoBlk.Hash}
			s.balances[tx.From] -= tx.Amount
			s.balances[tx.To] += tx.Amount
		}
		s.tipHeight, s.tipHash, s.hasTip = undoBlk.Height, undoBlk.Hash, true
	})
}

// snapshot 是某一时刻存储的只读一致视图，持有读锁，用完必须 Close。
type snapshot struct {
	s      *store
	closed bool
}

// snapshot 获取当前状态的一致只读视图（阻塞至写者释放写锁）。
func (s *store) snapshot() *snapshot {
	s.mu.RLock()
	return &snapshot{s: s}
}

// Close 释放视图持有的读锁。
func (sn *snapshot) Close() {
	if !sn.closed {
		sn.closed = true
		sn.s.mu.RUnlock()
	}
}

// Tip 返回视图中规范链顶端高度与哈希；空链时 ok=false。
func (sn *snapshot) Tip() (height uint64, hash chain.Hash, ok bool) {
	return sn.s.tipHeight, sn.s.tipHash, sn.s.hasTip
}

// BlockAt 返回视图中某高度的区块；不存在时 ok=false。
func (sn *snapshot) BlockAt(height uint64) (chain.Block, bool) {
	b, ok := sn.s.blocks[height]
	return b, ok
}

// Tx 按交易 ID 查询；未索引时 ok=false。
func (sn *snapshot) Tx(id chain.Hash) (txRecord, bool) {
	rec, ok := sn.s.txs[id]
	return rec, ok
}

// Balance 查询账户余额；无记录时返回 0。
func (sn *snapshot) Balance(account string) int64 {
	return sn.s.balances[account]
}
