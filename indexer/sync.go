package indexer

import (
	"fmt"

	"github.com/xingjianluo/local-chain-indexer-go/chain"
)

// BlockSource 抽象区块来源：从 from 高度起最多返回 limit 个连续区块。
type BlockSource interface {
	Blocks(from uint64, limit int) ([]chain.Block, error)
}

// ApplyBlocks 按顺序应用一批区块，返回成功应用的个数。
// 任一区块被拒绝时立即停下并返回错误，已应用的部分保持已提交状态。
func (ix *Indexer) ApplyBlocks(blocks []chain.Block) (int, error) {
	for i, b := range blocks {
		if err := ix.ApplyBlock(b); err != nil {
			return i, err
		}
	}
	return len(blocks), nil
}

// CatchUp 从当前尖端的下一高度开始，按 batchSize 分批从 src 拉取并应用区块，
// 直到源没有更多区块。返回本次追赶应用的区块总数。
//
// 背压与内存上界：每一批最多 batchSize 个区块驻留内存，拉下一批前
// 上一批已完整应用完毕；源端生产再快，内存占用也是 O(batchSize)，
// 处理耗时随区块数线性增长。ApplyBlock 是同步调用，调用方自然被
// 应用速度限流，不会无界堆积。
func (ix *Indexer) CatchUp(src BlockSource, batchSize int) (int, error) {
	if batchSize <= 0 {
		return 0, fmt.Errorf("batchSize must be positive, got %d", batchSize)
	}
	total := 0
	for {
		tip, ok := ix.Tip()
		from := uint64(0)
		if ok {
			from = tip.Height + 1
		}
		blocks, err := src.Blocks(from, batchSize)
		if err != nil {
			return total, fmt.Errorf("fetch from height %d: %w", from, err)
		}
		if len(blocks) == 0 {
			return total, nil
		}
		n, err := ix.ApplyBlocks(blocks)
		total += n
		if err != nil {
			return total, err
		}
		if len(blocks) < batchSize {
			return total, nil // 源已耗尽
		}
	}
}
