package indexer

import (
	"context"

	"github.com/xingjianluo/local-chain-indexer-go/chain"
)

// Indexer 是本地区块数据索引器。写路径（ApplyBlock / CatchUp）
// 与读路径（Snapshot）可并发，读侧永远看到完整状态。
type Indexer struct {
	// TODO: 实现字段
}

// New 创建索引器并以 genesis 作为索引起点（高度 0）。
func New(genesis chain.Block) *Indexer { return &Indexer{} }

// ApplyBlock 应用一个区块投递。重复、乱序、落后、父区块对不上
// 的投递会被拒绝并返回可区分的 *RejectError；触发重组时先回滚
// 被替换高度再重放新链，整个过程原子生效。
func (ix *Indexer) ApplyBlock(b chain.Block) error { return nil }

// Snapshot 返回当前索引状态的一致只读视图，可安全并发使用。
func (ix *Indexer) Snapshot() *snapshot { return nil }

// CatchUp 从 source 追赶积压：从当前索引高度+1 起按批拉取区块，
// 批大小有上界（内存占用有界），直到追上 source 的 Tip 或 ctx 取消。
// 返回本次追赶应用的区块数。
func (ix *Indexer) CatchUp(ctx context.Context, source chain.BlockSource, batchSize int) (int, error) {
	return 0, nil
}
