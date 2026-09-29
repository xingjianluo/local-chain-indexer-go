package chain

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"
)

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

// 订阅通道缓冲。投递是非阻塞的：缓冲满时丢弃，
// 模拟真实节点"不保证送达"，索引器需靠 CatchUp 补齐缺口。
const subBuffer = 16

// LocalSource 是本地模拟的区块源：内部维护一条可重组的规范链，
// 支持注入分叉、重复投递、乱序投递与长时间中断，用于测试索引器。
type LocalSource struct {
	mu     sync.Mutex
	blocks []Block // 规范链，下标即高度
	subs   map[chan Block]struct{}
}

// NewLocalSource 创建以 genesis 为创世块的本地模拟源。
// genesis 的高度必须为 0。
func NewLocalSource(genesis Block) *LocalSource {
	return &LocalSource{
		blocks: []Block{genesis},
		subs:   make(map[chan Block]struct{}),
	}
}

// MakeBlock 在 parent 之后构造一个区块（不修改链状态），
// 哈希由父哈希、高度与交易内容确定，便于测试复现。
func MakeBlock(parent Block, txs ...Tx) Block {
	b := Block{
		Height:     parent.Height + 1,
		ParentHash: parent.Hash,
		Txs:        txs,
	}
	h := sha256.New()
	h.Write(parent.ParentHash[:])
	h.Write(parent.Hash[:])
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], b.Height)
	h.Write(buf[:])
	for _, tx := range txs {
		h.Write(tx.ID[:])
	}
	copy(b.Hash[:], h.Sum(nil))
	return b
}

// MakeTx 构造一笔 ID 确定的交易；seed 在同一测试链内应唯一。
func MakeTx(seed uint32, from, to string, amount int64) Tx {
	var id Hash
	binary.BigEndian.PutUint32(id[:4], seed)
	id[31] = byte(seed) + 1
	return Tx{ID: id, From: from, To: to, Amount: amount}
}

// Tip 实现 BlockSource。
func (s *LocalSource) Tip() (uint64, Hash) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tip := s.blocks[len(s.blocks)-1]
	return tip.Height, tip.Hash
}

// BlockByHeight 实现 BlockSource。
func (s *LocalSource) BlockByHeight(height uint64) (Block, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if height >= uint64(len(s.blocks)) {
		return Block{}, false
	}
	return s.blocks[height], true
}

// Subscribe 实现 BlockSource。投递非阻塞，缓冲满即丢弃。
func (s *LocalSource) Subscribe(cancel <-chan struct{}) <-chan Block {
	ch := make(chan Block, subBuffer)
	s.mu.Lock()
	s.subs[ch] = struct{}{}
	s.mu.Unlock()
	go func() {
		select {
		case <-cancel:
		}
		s.mu.Lock()
		delete(s.subs, ch)
		s.mu.Unlock()
		close(ch)
	}()
	return ch
}

// broadcast 把区块非阻塞地投递给所有订阅者。
func (s *LocalSource) broadcast(bs ...Block) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ch := range s.subs {
		for _, b := range bs {
			select {
			case ch <- b:
			default: // 缓冲满，丢弃；订阅方可通过 CatchUp 补齐
			}
		}
	}
}

// Extend 在规范链顶端追加一个区块并广播，返回新块。
func (s *LocalSource) Extend(txs ...Tx) Block {
	s.mu.Lock()
	tip := s.blocks[len(s.blocks)-1]
	s.mu.Unlock()
	b := MakeBlock(tip, txs...)
	s.mu.Lock()
	s.blocks = append(s.blocks, b)
	s.mu.Unlock()
	s.broadcast(b)
	return b
}

// ReorgTo 从 forkHeight+1 起用 newBlocks 替换规范链尾部，
// 要求替换后链更长（否则索引器没有理由切换），并广播新链各块。
// 返回被应用的新链区块（从 forkHeight+1 开始）。
func (s *LocalSource) ReorgTo(forkHeight uint64, newBlocks []Block) []Block {
	s.mu.Lock()
	s.blocks = append(s.blocks[:forkHeight+1], newBlocks...)
	s.mu.Unlock()
	s.broadcast(newBlocks...)
	return newBlocks
}

// ForkBlocks 在高度 forkHeight 的区块之后构造 n 个分叉区块
// （不修改链状态），每个区块包含 txs[i] 给出的交易。
func (s *LocalSource) ForkBlocks(forkHeight uint64, txsPerBlock [][]Tx) []Block {
	s.mu.Lock()
	parent := s.blocks[forkHeight]
	s.mu.Unlock()
	out := make([]Block, 0, len(txsPerBlock))
	for _, txs := range txsPerBlock {
		b := MakeBlock(parent, txs...)
		out = append(out, b)
		parent = b
	}
	return out
}

// Emit 把任意区块再投递一次（用于模拟重复/乱序投递）。
func (s *LocalSource) Emit(bs ...Block) { s.broadcast(bs...) }
