package chain

import "fmt"

// RejectReason 区分区块被拒绝的原因。
type RejectReason int

const (
	// ReasonHeightRegression 高度回退：区块高度不大于当前已索引的尖端高度，
	// 且无法构成有效分叉（明显落后）。
	ReasonHeightRegression RejectReason = iota
	// ReasonParentMismatch 父区块对不上：区块的 ParentHash 与对应高度的
	// 已知父区块哈希不一致。
	ReasonParentMismatch
	// ReasonDuplicate 重复投递：该区块（同高度同哈希）已被索引。
	ReasonDuplicate
	// ReasonGap 高度空洞：区块跳过了尚未索引的高度。
	ReasonGap
)

// String 返回拒绝原因的可读描述。
func (r RejectReason) String() string {
	switch r {
	case ReasonHeightRegression:
		return "height-regression"
	case ReasonParentMismatch:
		return "parent-mismatch"
	case ReasonDuplicate:
		return "duplicate"
	case ReasonGap:
		return "height-gap"
	default:
		return "unknown"
	}
}

// RejectError 是带原因分类的区块拒绝错误。
type RejectError struct {
	Reason RejectReason
	Height uint64
	Hash   Hash
	Detail string
}

// Error 实现 error 接口。
func (e *RejectError) Error() string {
	return fmt.Sprintf("block rejected (%s) at height %d hash %s: %s",
		e.Reason, e.Height, e.Hash, e.Detail)
}
