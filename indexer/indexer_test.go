package indexer_test

import (
	"errors"
	"testing"

	"github.com/xingjianluo/local-chain-indexer-go/chain"
	"github.com/xingjianluo/local-chain-indexer-go/indexer"
	"github.com/xingjianluo/local-chain-indexer-go/source"
)

// rejectReason 从 error 中取出拒绝原因；非拒绝错误则测试失败。
func rejectReason(t *testing.T, err error) chain.RejectReason {
	t.Helper()
	var re *chain.RejectError
	if !errors.As(err, &re) {
		t.Fatalf("expected *chain.RejectError, got %T: %v", err, err)
	}
	t.Logf("rejected as expected: reason=%s height=%d detail=%s", re.Reason, re.Height, re.Detail)
	return re.Reason
}

func TestSequentialApply(t *testing.T) {
	c := source.NewChain("main", 10)
	ix := indexer.New()
	for h := 0; h < c.Len(); h++ {
		if err := ix.ApplyBlock(c.Block(uint64(h))); err != nil {
			t.Fatalf("apply height %d: %v", h, err)
		}
	}
	tip, ok := ix.Tip()
	if !ok || tip.Height != 9 || tip.Hash != c.Tip().Hash {
		t.Fatalf("unexpected tip: %+v ok=%v", tip, ok)
	}
	// 派生数据：余额应与逐笔重放一致。
	want := map[string]int64{}
	for h := 0; h < c.Len(); h++ {
		for _, tx := range c.Block(uint64(h)).Txs {
			want[tx.From] -= int64(tx.Amount)
			want[tx.To] += int64(tx.Amount)
		}
	}
	for addr, bal := range want {
		if got := ix.Balance(addr); got != bal {
			t.Fatalf("balance %s: got %d want %d", addr, got, bal)
		}
	}
	t.Logf("applied %d blocks, tip=%s, %d balances verified", c.Len(), tip.Hash, len(want))
}

func TestRejectReasons(t *testing.T) {
	c := source.NewChain("main", 10)
	ix := indexer.New()

	// 高度空洞：第一个区块不在高度 0。
	if got := rejectReason(t, ix.ApplyBlock(c.Block(3))); got != chain.ReasonGap {
		t.Fatalf("want gap, got %s", got)
	}

	for h := 0; h < 5; h++ {
		if err := ix.ApplyBlock(c.Block(uint64(h))); err != nil {
			t.Fatalf("apply height %d: %v", h, err)
		}
	}

	// 重复投递：同高度同哈希。
	if got := rejectReason(t, ix.ApplyBlock(c.Block(4))); got != chain.ReasonDuplicate {
		t.Fatalf("want duplicate, got %s", got)
	}

	// 高度空洞：跳过高度 5 直接投 7。
	if got := rejectReason(t, ix.ApplyBlock(c.Block(7))); got != chain.ReasonGap {
		t.Fatalf("want gap, got %s", got)
	}

	// 父区块对不上：高度 5 但父哈希乱填。
	bad := c.Block(5)
	bad.ParentHash = chain.Hash{0xde, 0xad}
	if got := rejectReason(t, ix.ApplyBlock(bad)); got != chain.ReasonParentMismatch {
		t.Fatalf("want parent-mismatch, got %s", got)
	}

	// 高度回退：落后超过最大重组深度。
	ix2 := indexer.New(indexer.WithMaxReorgDepth(2))
	for h := 0; h < c.Len(); h++ {
		if err := ix2.ApplyBlock(c.Block(uint64(h))); err != nil {
			t.Fatalf("apply height %d: %v", h, err)
		}
	}
	fork := source.Fork(c, 0, "stale-fork", 10) // 从创世就不同的另一条链
	// 高度 2 的分叉块落后尖端 7 个高度，超过深度 2。
	if got := rejectReason(t, ix2.ApplyBlock(fork.Block(2))); got != chain.ReasonHeightRegression {
		t.Fatalf("want height-regression, got %s", got)
	}

	// 状态未被拒绝的投递污染。
	tip, _ := ix.Tip()
	if tip.Height != 4 || tip.Hash != c.Block(4).Hash {
		t.Fatalf("tip polluted: %+v", tip)
	}
	t.Logf("tip intact after rejects: height=%d hash=%s", tip.Height, tip.Hash)
}
