// Package source 提供本地模拟的区块源：确定性生成区块与交易，
// 可构造分叉链，用于在没有真实节点的情况下验证索引器行为。
package source

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/xingjianluo/local-chain-indexer-go/chain"
)

// Chain 是一条本地模拟链（或分叉），区块按高度连续排列。
type Chain struct {
	name   string
	blocks []chain.Block
}

// hashOf 确定性计算区块哈希：由链名、高度和父哈希决定。
func hashOf(name string, height uint64, parent chain.Hash) chain.Hash {
	h := sha256.New()
	h.Write([]byte(name))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], height)
	h.Write(buf[:])
	h.Write(parent[:])
	var out chain.Hash
	copy(out[:], h.Sum(nil))
	return out
}

// txHash 确定性计算交易哈希。
func txHash(blockHash chain.Hash, idx int) chain.Hash {
	h := sha256.New()
	h.Write(blockHash[:])
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(idx))
	h.Write(buf[:])
	var out chain.Hash
	copy(out[:], h.Sum(nil))
	return out
}

// makeBlock 构造一个确定性区块：固定 2 笔交易，地址与金额由高度推导。
func makeBlock(name string, height uint64, parent chain.Hash) chain.Block {
	hb := hashOf(name, height, parent)
	txs := make([]chain.Tx, 2)
	for i := range txs {
		txs[i] = chain.Tx{
			Hash:   txHash(hb, i),
			From:   fmt.Sprintf("addr-%d", (height+uint64(i))%8),
			To:     fmt.Sprintf("addr-%d", (height+uint64(i)+1)%8),
			Amount: 10 + height + uint64(i),
		}
	}
	return chain.Block{Height: height, Hash: hb, ParentHash: parent, Txs: txs}
}

// NewChain 生成一条从创世（高度 0）开始、长度为 length 的模拟链。
func NewChain(name string, length int) *Chain {
	c := &Chain{name: name}
	var parent chain.Hash
	for h := 0; h < length; h++ {
		blk := makeBlock(name, uint64(h), parent)
		c.blocks = append(c.blocks, blk)
		parent = blk.Hash
	}
	return c
}

// Fork 从 base 链的 fromHeight 高度（不含）分叉，生成一条新链：
// fromHeight 之前与 base 完全一致，之后用新名字重新派生，总长 length。
func Fork(base *Chain, fromHeight uint64, name string, length int) *Chain {
	c := &Chain{name: name}
	c.blocks = append(c.blocks, base.blocks[:fromHeight]...)
	parent := chain.Hash{}
	if fromHeight > 0 {
		parent = base.blocks[fromHeight-1].Hash
	}
	for h := int(fromHeight); h < length; h++ {
		blk := makeBlock(name, uint64(h), parent)
		c.blocks = append(c.blocks, blk)
		parent = blk.Hash
	}
	return c
}

// Len 返回链长度。
func (c *Chain) Len() int { return len(c.blocks) }

// Block 返回某高度的区块。
func (c *Chain) Block(height uint64) chain.Block { return c.blocks[height] }

// Blocks 实现 indexer.BlockSource：返回 [from, from+limit) 的区块。
func (c *Chain) Blocks(from uint64, limit int) ([]chain.Block, error) {
	if from >= uint64(len(c.blocks)) {
		return nil, nil
	}
	end := from + uint64(limit)
	if end > uint64(len(c.blocks)) {
		end = uint64(len(c.blocks))
	}
	out := make([]chain.Block, end-from)
	copy(out, c.blocks[from:end])
	return out, nil
}

// Tip 返回链尖端区块。
func (c *Chain) Tip() chain.Block { return c.blocks[len(c.blocks)-1] }
