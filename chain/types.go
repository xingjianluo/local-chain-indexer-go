// Package chain 定义区块、交易等核心数据模型。
package chain

import "fmt"

// Hash 是 32 字节的区块/交易哈希。
type Hash [32]byte

// String 返回哈希的短十六进制表示，便于日志阅读。
func (h Hash) String() string {
	return fmt.Sprintf("%x…%x", h[:4], h[28:])
}

// Tx 是一笔交易。
type Tx struct {
	Hash   Hash
	From   string
	To     string
	Amount uint64
}

// Block 是一个区块，按高度推进，通过 ParentHash 链接。
type Block struct {
	Height    uint64
	Hash      Hash
	ParentHash Hash
	Txs       []Tx
}

// Header 返回区块的轻量头部信息。
func (b Block) Header() Header {
	return Header{Height: b.Height, Hash: b.Hash, ParentHash: b.ParentHash}
}

// Header 是区块头，用于索引端的轻量引用。
type Header struct {
	Height     uint64
	Hash       Hash
	ParentHash Hash
}
