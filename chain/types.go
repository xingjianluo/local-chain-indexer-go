// Package chain 定义本地区块链索引器使用的核心数据类型，
// 并提供本地模拟的区块源（代替真实节点）。
package chain

import "fmt"

// Hash 是 32 字节的区块/交易哈希。
type Hash [32]byte

// String 返回哈希的短十六进制表示，便于日志阅读。
func (h Hash) String() string {
	return fmt.Sprintf("%x…%x", h[:4], h[28:])
}

// Tx 是一笔交易。索引器会为每笔交易维护派生数据（余额变动）。
type Tx struct {
	ID     Hash   // 交易唯一标识
	From   string // 付款方账户
	To     string // 收款方账户
	Amount int64  // 转账金额（最小单位）
}

// Block 是一个区块，按高度推进，通过 ParentHash 链接父区块。
type Block struct {
	Height     uint64 // 区块高度，创世块为 0
	Hash       Hash   // 本区块哈希
	ParentHash Hash   // 父区块哈希
	Txs        []Tx   // 区块内交易（有序）
}
