package indexer_test

import (
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/local-chain-indexer-go/chain"
	"github.com/xingjianluo/local-chain-indexer-go/indexer"
	"github.com/xingjianluo/local-chain-indexer-go/source"
)

// recordingSource 包装区块源，记录每次拉取的范围，用于验证批处理行为。
type recordingSource struct {
	inner *source.Chain
	mu    sync.Mutex
	calls []fetchCall
}

type fetchCall struct {
	from  uint64
	limit int
	got   int
}

func (r *recordingSource) Blocks(from uint64, limit int) ([]chain.Block, error) {
	bs, err := r.inner.Blocks(from, limit)
	r.mu.Lock()
	r.calls = append(r.calls, fetchCall{from: from, limit: limit, got: len(bs)})
	r.mu.Unlock()
	return bs, err
}

// TestCatchUpAfterOutage 模拟长时间中断：索引器停在高度 99，
// 源已推进到 4999，通过分批追赶追平，最终状态与从头顺序索引一致。
func TestCatchUpAfterOutage(t *testing.T) {
	c := source.NewChain("main", 5000)
	ix := indexer.New()

	// 中断前：索引到高度 99。
	for h := 0; h < 100; h++ {
		if err := ix.ApplyBlock(c.Block(uint64(h))); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("outage begins: indexer stuck at height 99, source advances to 4999")

	// 中断恢复：按 64 一批追赶。
	const batchSize = 64
	rec := &recordingSource{inner: c}
	start := time.Now()
	n, err := ix.CatchUp(rec, batchSize)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("catch up: %v", err)
	}
	if n != 4900 {
		t.Fatalf("caught up %d blocks, want 4900", n)
	}
	tip, _ := ix.Tip()
	if tip.Height != 4999 || tip.Hash != c.Tip().Hash {
		t.Fatalf("tip after catch-up: %+v", tip)
	}
	t.Logf("caught up %d blocks in %d batches, %v total (%v/block)",
		n, len(rec.calls), elapsed, elapsed/time.Duration(n))

	// 背压验证：每批请求都不超过 batchSize，内存占用 O(batchSize)。
	for i, call := range rec.calls {
		if call.limit > batchSize {
			t.Fatalf("batch %d requested %d > batchSize %d", i, call.limit, batchSize)
		}
		if call.got > batchSize {
			t.Fatalf("batch %d held %d blocks in memory > batchSize", i, call.got)
		}
	}
	wantCalls := 4900/batchSize + 1 // 4900 = 76*64 + 36，最后一批不满
	if len(rec.calls) != wantCalls {
		t.Fatalf("fetch calls: got %d want %d", len(rec.calls), wantCalls)
	}

	// 与从头一次性顺序索引的结果完全一致。
	ref := indexer.New()
	for h := 0; h < c.Len(); h++ {
		if err := ref.ApplyBlock(c.Block(uint64(h))); err != nil {
			t.Fatal(err)
		}
	}
	refTip, _ := ref.Tip()
	if tip.Hash != refTip.Hash {
		t.Fatalf("catch-up result diverges from sequential indexing")
	}
	for addr, want := range balancesOf(c) {
		if got := ix.Balance(addr); got != want {
			t.Fatalf("balance %s: got %d want %d", addr, got, want)
		}
	}
	t.Logf("catch-up state identical to sequential indexing: tip=%s", tip.Hash)
}

// TestCatchUpLinearScaling 验证处理耗时随数据量线性增长：
// 处理 4 倍区块的每块耗时不应明显劣化（阈值放宽以容忍环境抖动）。
func TestCatchUpLinearScaling(t *testing.T) {
	perBlock := func(n int) time.Duration {
		c := source.NewChain("main", n)
		ix := indexer.New()
		start := time.Now()
		if _, err := ix.CatchUp(c, 256); err != nil {
			t.Fatal(err)
		}
		return time.Since(start) / time.Duration(n)
	}
	small := perBlock(5000)
	large := perBlock(20000)
	t.Logf("per-block cost: %v @5k blocks, %v @20k blocks", small, large)
	if small > 0 && large > 4*small {
		t.Fatalf("super-linear degradation: %v/block @20k vs %v/block @5k", large, small)
	}
}
