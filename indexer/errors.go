// Package indexer 实现本地区块数据索引器：按高度应用区块、
// 维护交易索引与派生数据（账户余额），并处理链重组。
package indexer

import (
	"errors"
	"fmt"
)

// 拒绝原因按类型区分，调用方可用 errors.Is 判定。
var (
	// ErrHeightRegression 区块高度明显落后于当前索引高度，且不构成合法重组。
	ErrHeightRegression = errors.New("height regression")
	// ErrParentMismatch 区块的父哈希与索引中其父高度上的区块哈希对不上。
	ErrParentMismatch = errors.New("parent hash mismatch")
	// ErrDuplicate 该区块（按哈希）已经被索引过，属于重复投递。
	ErrDuplicate = errors.New("duplicate block")
	// ErrReorgTooDeep 重组深度超过索引器保留的历史窗口，无法安全回滚。
	ErrReorgTooDeep = errors.New("reorg too deep")
)

// RejectError 描述一次被拒绝的区块投递，Reason 可用 errors.Is 区分。
type RejectError struct {
	Reason error  // 上述原因之一
	Height uint64 // 被拒区块高度
	Hash   string // 被拒区块哈希（短格式）
	Detail string // 补充说明
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("reject block h=%d %s: %v (%s)", e.Height, e.Hash, e.Reason, e.Detail)
}

// Unwrap 使 errors.Is(err, ErrHeightRegression) 等判定生效。
func (e *RejectError) Unwrap() error { return e.Reason }
