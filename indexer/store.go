package indexer

import "github.com/xingjianluo/local-chain-indexer-go/chain"

// txLocation 记录一笔交易在规范链上的位置。
type txLocation struct {
	Height uint64
	Block  chain.Hash
}

// balanceDelta 记录单个地址在一笔交易中的余额变化，用于撤销。
type balanceDelta struct {
	addr  string
	delta int64
}

// blockUndo 是一个区块应用所产生的全部可撤销变更。
type blockUndo struct {
	txs     []chain.Hash
	deltas  []balanceDelta
}

// store 是索引器的内部状态，只由 Indexer 在持锁时访问。
// 所有变更先写入 batch，commit 时一次性落盘到 map，
// 因此对读者而言状态要么全是旧的、要么全是新的。
type store struct {
	blocks   []chain.Block            // 规范链，下标即高度
	txIndex  map[chain.Hash]txLocation
	balances map[string]int64
	undos    []blockUndo              // 与 blocks 等长，逐高度的撤销日志
	forkPool map[chain.Hash]chain.Block // 已验证但尚未成为规范链的分叉区块
}

func newStore() *store {
	return &store{
		txIndex:  make(map[chain.Hash]txLocation),
		balances: make(map[string]int64),
		forkPool: make(map[chain.Hash]chain.Block),
	}
}

// cloneStore 深拷贝一份 store，作为对账期间的影子状态：
// 在其上回滚/重放不影响对读者可见的现役 store，
// 对账成功后整体换入，失败则直接丢弃。
// 分叉池不带入克隆——对账改挂新链后，旧链上缓存的分叉块不再有意义。
func cloneStore(st *store) *store {
	s := &store{
		blocks:   append([]chain.Block(nil), st.blocks...),
		undos:    append([]blockUndo(nil), st.undos...),
		txIndex:  make(map[chain.Hash]txLocation, len(st.txIndex)),
		balances: make(map[string]int64, len(st.balances)),
		forkPool: make(map[chain.Hash]chain.Block),
	}
	for k, v := range st.txIndex {
		s.txIndex[k] = v
	}
	for k, v := range st.balances {
		s.balances[k] = v
	}
	return s
}

// batch 收集一组待提交的变更；只有 commit 才会修改 store。
type batch struct {
	st       *store
	appended []chain.Block
	undos    []blockUndo
	// 回滚目标：>=0 表示先把规范链截断到该高度（含）再追加。
	rollbackTo int
}

func newBatch(st *store) *batch {
	return &batch{st: st, rollbackTo: -1}
}

// rollback 记录“截断到 height（含）”的意图，变更在 commit 时生效。
func (b *batch) rollback(height uint64) {
	b.rollbackTo = int(height)
	b.appended = nil
	b.undos = nil
}

// rollbackAll 记录“清空整条规范链”的意图（分叉从创世之前开始）。
func (b *batch) rollbackAll() {
	b.rollbackTo = -2
	b.appended = nil
	b.undos = nil
}

// apply 把一个区块加入批次，并计算其撤销日志。
func (b *batch) apply(blk chain.Block) {
	u := blockUndo{}
	for _, tx := range blk.Txs {
		u.txs = append(u.txs, tx.Hash)
		u.deltas = append(u.deltas,
			balanceDelta{addr: tx.From, delta: int64(tx.Amount)},
			balanceDelta{addr: tx.To, delta: -int64(tx.Amount)},
		)
	}
	b.appended = append(b.appended, blk)
	b.undos = append(b.undos, u)
}

// commit 原子地应用批次：先截断（撤销被替换高度的派生数据），再追加。
// 该函数不返回错误：所有校验在进入批次前完成，提交本身不会失败。
func (b *batch) commit() {
	st := b.st
	if b.rollbackTo == -2 {
		// 清空整条链：逐个撤销所有高度。
		for len(st.blocks) > 0 {
			u := st.undos[len(st.undos)-1]
			for _, h := range u.txs {
				delete(st.txIndex, h)
			}
			for _, d := range u.deltas {
				st.balances[d.addr] += d.delta
			}
			st.blocks = st.blocks[:len(st.blocks)-1]
			st.undos = st.undos[:len(st.undos)-1]
		}
	} else if b.rollbackTo >= 0 {
		target := uint64(b.rollbackTo)
		for uint64(len(st.blocks)) > target+1 {
			u := st.undos[len(st.undos)-1]
			for _, h := range u.txs {
				delete(st.txIndex, h)
			}
			for _, d := range u.deltas {
				st.balances[d.addr] += d.delta
			}
			st.blocks = st.blocks[:len(st.blocks)-1]
			st.undos = st.undos[:len(st.undos)-1]
		}
	}
	for i, blk := range b.appended {
		u := b.undos[i]
		for _, tx := range blk.Txs {
			st.txIndex[tx.Hash] = txLocation{Height: blk.Height, Block: blk.Hash}
			st.balances[tx.From] -= int64(tx.Amount)
			st.balances[tx.To] += int64(tx.Amount)
		}
		st.blocks = append(st.blocks, blk)
		st.undos = append(st.undos, u)
	}
}
