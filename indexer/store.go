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

// store 是索引器的底层存储：区块索引、交易索引与派生数据（余额）。
// 所有修改都通过 begin/commit 以整批方式应用，保证读侧看到的
// 永远是一个完整状态（要么整批生效，要么完全不可见）。
type store struct {
	mu sync.RWMutex
	// TODO: 实现字段
}

func newStore() *store { return &store{} }

// snapshot 是某一时刻存储的只读一致视图。
type snapshot struct {
	// TODO: 实现字段
}

// snapshot 获取当前状态的一致只读视图。
func (s *store) snapshot() *snapshot { return &snapshot{} }

// Tip 返回视图中规范链顶端高度与哈希；空链时 ok=false。
func (sn *snapshot) Tip() (height uint64, hash chain.Hash, ok bool) { return 0, chain.Hash{}, false }

// BlockAt 返回视图中某高度的区块；不存在时 ok=false。
func (sn *snapshot) BlockAt(height uint64) (chain.Block, bool) { return chain.Block{}, false }

// Tx 按交易 ID 查询；未索引时 ok=false。
func (sn *snapshot) Tx(id chain.Hash) (txRecord, bool) { return txRecord{}, false }

// Balance 查询账户余额；无记录时返回 0。
func (sn *snapshot) Balance(account string) int64 { return 0 }
