package chain

// BlockSource 抽象一个区块来源（真实节点或本地模拟器）。
// 索引器只依赖该接口，便于替换实现。
type BlockSource interface {
	// Tip 返回当前源头的最新高度与哈希。
	Tip() (height uint64, hash Hash)
	// BlockByHeight 按高度取规范链上的区块；不存在时 ok=false。
	BlockByHeight(height uint64) (Block, bool)
	// Subscribe 返回一个只读通道，源头把新区块（可能重复、乱序、
	// 属于分叉）投递到该通道；cancel 后通道最终会被关闭。
	Subscribe(cancel <-chan struct{}) <-chan Block
}

// LocalSource 是本地模拟的区块源：内部维护一条可重组的链，
// 支持注入分叉、重复投递、乱序投递与长时间中断，用于测试索引器。
type LocalSource struct {
	// TODO: 实现字段
}

// NewLocalSource 创建以 genesis 为创世块的本地模拟源。
func NewLocalSource(genesis Block) *LocalSource {
	return &LocalSource{}
}

// Tip 实现 BlockSource。
func (s *LocalSource) Tip() (uint64, Hash) { return 0, Hash{} }

// BlockByHeight 实现 BlockSource。
func (s *LocalSource) BlockByHeight(height uint64) (Block, bool) { return Block{}, false }

// Subscribe 实现 BlockSource。
func (s *LocalSource) Subscribe(cancel <-chan struct{}) <-chan Block {
	ch := make(chan Block)
	close(ch)
	return ch
}
