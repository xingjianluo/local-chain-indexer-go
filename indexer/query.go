package indexer

import "github.com/xingjianluo/local-chain-indexer-go/chain"

// TxRecord 是一笔已索引交易及其所在位置。
type TxRecord struct {
	Tx      chain.Tx
	Height  uint64
	Block   chain.Hash
}

// GetTx 按哈希查询交易；返回该交易在规范链上的位置。
func (ix *Indexer) GetTx(h chain.Hash) (TxRecord, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	loc, ok := ix.st.txIndex[h]
	if !ok {
		return TxRecord{}, false
	}
	blk := ix.st.blocks[loc.Height]
	for _, tx := range blk.Txs {
		if tx.Hash == h {
			return TxRecord{Tx: tx, Height: loc.Height, Block: loc.Block}, true
		}
	}
	return TxRecord{}, false
}

// Balance 查询某地址的派生余额。
func (ix *Indexer) Balance(addr string) int64 {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.st.balances[addr]
}

// BlockAt 返回规范链上某高度的区块。
func (ix *Indexer) BlockAt(height uint64) (chain.Block, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if height >= uint64(len(ix.st.blocks)) {
		return chain.Block{}, false
	}
	return ix.st.blocks[height], true
}

// Snapshot 是某一时刻的一致性只读视图：
// 在 View 回调执行期间持有读锁，期间所有读取都落在同一个已提交状态上，
// 不会被并发的写入（包括重组）切开。
type Snapshot struct {
	ix *Indexer
}

// View 在一致性快照上执行 fn；fn 内可安全调用 Snapshot 的各个查询方法。
func (ix *Indexer) View(fn func(s *Snapshot)) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	fn(&Snapshot{ix: ix})
}

// Tip 返回快照时刻的尖端。
func (s *Snapshot) Tip() (chain.Header, bool) {
	return s.ix.tipLocked()
}

// Balance 返回快照时刻的地址余额。
func (s *Snapshot) Balance(addr string) int64 {
	return s.ix.st.balances[addr]
}

// BlockAt 返回快照时刻规范链上某高度的区块。
func (s *Snapshot) BlockAt(height uint64) (chain.Block, bool) {
	if height >= uint64(len(s.ix.st.blocks)) {
		return chain.Block{}, false
	}
	return s.ix.st.blocks[height], true
}

// GetTx 返回快照时刻的交易记录。
func (s *Snapshot) GetTx(h chain.Hash) (TxRecord, bool) {
	loc, ok := s.ix.st.txIndex[h]
	if !ok {
		return TxRecord{}, false
	}
	blk := s.ix.st.blocks[loc.Height]
	for _, tx := range blk.Txs {
		if tx.Hash == h {
			return TxRecord{Tx: tx, Height: loc.Height, Block: loc.Block}, true
		}
	}
	return TxRecord{}, false
}
