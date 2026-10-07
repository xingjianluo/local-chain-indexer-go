package indexer_test

import (
	"errors"
	"sync"

	"github.com/xingjianluo/local-chain-indexer-go/chain"
	"testing"

	"github.com/xingjianluo/local-chain-indexer-go/indexer"
	"github.com/xingjianluo/local-chain-indexer-go/source"
)

// assertConverged 逐高度断言索引器与源链完全一致：
// 尖端、每个高度的区块、每笔交易的位置、以及逐笔重放算出的余额。
func assertConverged(t *testing.T, ix *indexer.Indexer, c *source.Chain) {
	t.Helper()
	tip, ok := ix.Tip()
	if !ok {
		t.Fatal("no tip after reconcile")
	}
	if tip.Height != uint64(c.Len()-1) || tip.Hash != c.Tip().Hash {
		t.Fatalf("tip: got height=%d hash=%s, want height=%d hash=%s",
			tip.Height, tip.Hash, c.Len()-1, c.Tip().Hash)
	}
	for h := 0; h < c.Len(); h++ {
		want := c.Block(uint64(h))
		blk, ok := ix.BlockAt(uint64(h))
		if !ok || blk.Hash != want.Hash {
			t.Fatalf("height %d: got %s, want %s", h, blk.Hash, want.Hash)
		}
		for _, tx := range want.Txs {
			rec, ok := ix.GetTx(tx.Hash)
			if !ok {
				t.Fatalf("tx %s at height %d not indexed", tx.Hash, h)
			}
			if rec.Height != uint64(h) || rec.Block != want.Hash {
				t.Fatalf("tx %s located at (%d, %s), want (%d, %s)",
					tx.Hash, rec.Height, rec.Block, h, want.Hash)
			}
		}
	}
	for addr, want := range balancesOf(c) {
		if got := ix.Balance(addr); got != want {
			t.Fatalf("balance %s: got %d want %d", addr, got, want)
		}
	}
}

// TestReconcileAfterSourceChainSwitch 覆盖核心场景：中断期间源没有接着
// 原来的链往后长，而是从中间某高度分叉出去、换成了一条更长的链。
// 索引器不做任何手工回滚，只靠一次 Reconcile 就该收敛到源现在的链。
func TestReconcileAfterSourceChainSwitch(t *testing.T) {
	main := source.NewChain("main", 120)               // 旧链：高度 0..119
	switched := source.Fork(main, 60, "switched", 150) // 源换的新链：从高度 60 分叉，更长

	ix := indexer.New()
	// 中断前：索引到旧链高度 99。
	for h := 0; h < 100; h++ {
		if err := ix.ApplyBlock(main.Block(uint64(h))); err != nil {
			t.Fatal(err)
		}
	}
	stuckTip, _ := ix.Tip()
	t.Logf("outage begins: indexer stuck on old chain tip height=%d hash=%s; "+
		"source switched to another chain forked at height 60 with tip height=%d",
		stuckTip.Height, stuckTip.Hash, switched.Len()-1)

	// 恢复后：一次对账收敛。旧的 CatchUp 在这种场景会卡死（源在高度 100
	// 给的块的父哈希对不上本地尖端），这里不需要任何手工干预。
	rec := &recordingSource{inner: switched}
	rep, err := ix.Reconcile(rec, indexer.ReconcileOptions{
		BatchSize:     32,
		MaxDivergence: 64,
		MaxRounds:     3,
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	t.Logf("reconciled in %d round(s): fork point=%d, rolled back %d old blocks, applied %d new blocks",
		rep.Rounds, rep.ForkPoint, rep.RolledBack, rep.Applied)

	if rep.ForkPoint != 59 {
		t.Fatalf("fork point: got %d, want 59", rep.ForkPoint)
	}
	if rep.RolledBack != 40 { // 旧链高度 60..99 被回滚
		t.Fatalf("rolled back: got %d, want 40", rep.RolledBack)
	}
	if rep.Applied != 90 { // 新链高度 60..149 被应用
		t.Fatalf("applied: got %d, want 90", rep.Applied)
	}
	if rep.AlreadySynced {
		t.Fatal("report claims already synced, but a rollback happened")
	}

	// 收敛后：逐高度的区块、交易位置、余额都和源一致。
	assertConverged(t, ix, switched)

	// 旧链被回滚区间（60..119）里的交易必须查不到。
	for h := 60; h < 120; h++ {
		for _, tx := range main.Block(uint64(h)).Txs {
			if _, ok := ix.GetTx(tx.Hash); ok {
				t.Fatalf("orphaned tx %s from old chain height %d still indexed", tx.Hash, h)
			}
		}
	}
	t.Logf("converged to source chain: tip height=%d, %d fetch calls, old-chain txs above height 59 all gone",
		switched.Len()-1, len(rec.calls))
}

// TestReconcileSourceShorterButDifferent 覆盖"源换链了、但高度并不比本地高"
// 的情况：不能因为没有新块就跳过对账，仍要纠正到源的链上。
func TestReconcileSourceShorterButDifferent(t *testing.T) {
	main := source.NewChain("main", 120)
	shorter := source.Fork(main, 60, "shorter", 90) // 从高度 60 分叉，尖端 89 < 本地 99

	ix := indexer.New()
	for h := 0; h < 100; h++ {
		if err := ix.ApplyBlock(main.Block(uint64(h))); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := ix.Tip()
	t.Logf("local tip height=%d, source tip height=%d (lower, but on a different chain)",
		before.Height, shorter.Len()-1)

	rep, err := ix.Reconcile(shorter, indexer.ReconcileOptions{BatchSize: 16})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	t.Logf("reconciled: fork point=%d, rolled back=%d, applied=%d",
		rep.ForkPoint, rep.RolledBack, rep.Applied)
	if rep.ForkPoint != 59 || rep.RolledBack != 40 || rep.Applied != 30 {
		t.Fatalf("unexpected report: %+v", rep)
	}
	assertConverged(t, ix, shorter)
	t.Logf("converged to shorter source chain: tip height=%d", shorter.Len()-1)
}

// TestReconcileAlreadySynced 本地与源本就在同一条链上时，
// 对账是空操作：不修改任何状态，报告 AlreadySynced。
func TestReconcileAlreadySynced(t *testing.T) {
	c := source.NewChain("main", 50)
	ix := indexer.New()
	for h := 0; h < c.Len(); h++ {
		if err := ix.ApplyBlock(c.Block(uint64(h))); err != nil {
			t.Fatal(err)
		}
	}
	rep, err := ix.Reconcile(c, indexer.ReconcileOptions{BatchSize: 16})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !rep.AlreadySynced || rep.RolledBack != 0 || rep.Applied != 0 {
		t.Fatalf("expected no-op reconcile, got %+v", rep)
	}
	t.Logf("already in sync with source: no rollback, no new blocks applied")
	assertConverged(t, ix, c)
}

// balancesOfPrefix 逐笔重放一条链的前 n 个区块，算出期望余额。
func balancesOfPrefix(c *source.Chain, n int) map[string]int64 {
	want := map[string]int64{}
	for h := 0; h < n; h++ {
		for _, tx := range c.Block(uint64(h)).Txs {
			want[tx.From] -= int64(tx.Amount)
			want[tx.To] += int64(tx.Amount)
		}
	}
	return want
}

// assertUnchanged 断言索引器仍停在预期的链前缀上（对账失败未留下任何改动）。
func assertUnchanged(t *testing.T, ix *indexer.Indexer, c *source.Chain, n int) {
	t.Helper()
	tip, ok := ix.Tip()
	if !ok || tip.Height != uint64(n-1) || tip.Hash != c.Block(uint64(n-1)).Hash {
		t.Fatalf("tip moved after failed reconcile: %+v", tip)
	}
	for addr, want := range balancesOfPrefix(c, n) {
		if got := ix.Balance(addr); got != want {
			t.Fatalf("balance %s changed after failed reconcile: got %d want %d", addr, got, want)
		}
	}
}

// TestReconcileDivergenceTooDeep 本地与源的共同祖先超出允许的分叉深度时，
// 对账必须以"difference too deep"明确失败，且索引保持不动。
func TestReconcileDivergenceTooDeep(t *testing.T) {
	main := source.NewChain("main", 120)
	other := source.NewChain("other", 150) // 从创世起就完全不同的另一条链

	ix := indexer.New()
	for h := 0; h < 100; h++ {
		if err := ix.ApplyBlock(main.Block(uint64(h))); err != nil {
			t.Fatal(err)
		}
	}

	rep, err := ix.Reconcile(other, indexer.ReconcileOptions{
		BatchSize:     16,
		MaxDivergence: 10, // 共同祖先在创世，深度 100 远超 10
		MaxRounds:     3,
	})
	if err == nil {
		t.Fatal("expected divergence-too-deep error, got nil")
	}
	var rerr *indexer.ReconcileError
	if !errors.As(err, &rerr) {
		t.Fatalf("error type: got %T, want *indexer.ReconcileError", err)
	}
	if rerr.Reason != indexer.ReconcileDivergenceTooDeep {
		t.Fatalf("reason: got %s, want %s", rerr.Reason, indexer.ReconcileDivergenceTooDeep)
	}
	t.Logf("reconcile stopped as expected: reason=%s, rounds=%d, detail=%s",
		rerr.Reason, rep.Rounds, rerr.Detail)

	// 索引保持不动：尖端、余额都还是旧链的。
	assertUnchanged(t, ix, main, 100)
	t.Logf("index untouched after divergence-too-deep: tip still on old chain")
}

// flipSource 每次调用 Blocks 都切换到另一条链，模拟"源一直在变"：
// 两条链共享前缀但在分叉点之后不同，任何一轮对账都没法对它收敛。
type flipSource struct {
	mu     sync.Mutex
	chains []*source.Chain
	calls  int
}

func (f *flipSource) Blocks(from uint64, limit int) ([]chain.Block, error) {
	f.mu.Lock()
	c := f.chains[f.calls%len(f.chains)]
	f.calls++
	f.mu.Unlock()
	return c.Blocks(from, limit)
}

// TestReconcileSourceUnstable 源在对账过程中持续变动（每次拉取看到的
// 链都不一样）时，对账必须在有限轮数内以"source unstable"明确放弃，
// 且索引保持不动，而不是无限重试或停在错误状态上。
func TestReconcileSourceUnstable(t *testing.T) {
	base := source.NewChain("base", 100)
	chainB := source.Fork(base, 95, "flip-b", 105) // 从高度 95 分叉的两条链
	chainC := source.Fork(base, 95, "flip-c", 105)

	ix := indexer.New()
	for h := 0; h < base.Len(); h++ {
		if err := ix.ApplyBlock(base.Block(uint64(h))); err != nil {
			t.Fatal(err)
		}
	}

	src := &flipSource{chains: []*source.Chain{chainB, chainC}}
	rep, err := ix.Reconcile(src, indexer.ReconcileOptions{
		BatchSize:     4, // 小批次：跨批的父哈希链接必然断裂，每轮都能发现源在变
		MaxDivergence: 16,
		MaxRounds:     3,
	})
	if err == nil {
		t.Fatal("expected source-unstable error, got nil")
	}
	var rerr *indexer.ReconcileError
	if !errors.As(err, &rerr) {
		t.Fatalf("error type: got %T, want *indexer.ReconcileError", err)
	}
	if rerr.Reason != indexer.ReconcileSourceUnstable {
		t.Fatalf("reason: got %s, want %s", rerr.Reason, indexer.ReconcileSourceUnstable)
	}
	if rep.Rounds != 3 {
		t.Fatalf("rounds: got %d, want 3", rep.Rounds)
	}
	t.Logf("reconcile gave up as expected: reason=%s after %d rounds, %d source fetches total",
		rerr.Reason, rep.Rounds, src.calls)

	// 索引保持不动：仍停在 base 链尖端。
	assertUnchanged(t, ix, base, 100)
	t.Logf("index untouched after source-unstable: tip still on original chain")
}

// TestReconcileConcurrentReaders 对账（含回滚+重放+换入）与并发读者同时进行，
// 读者在对账全程只能看到完整的旧状态或完整的新状态。用 go test -race 运行。
func TestReconcileConcurrentReaders(t *testing.T) {
	main := source.NewChain("main", 400)
	switched := source.Fork(main, 100, "switched", 600)

	ix := indexer.New()
	for h := 0; h < 300; h++ {
		if err := ix.ApplyBlock(main.Block(uint64(h))); err != nil {
			t.Fatal(err)
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			n := 0
			for {
				select {
				case <-stop:
					t.Logf("reader %d saw %d consistent snapshots during reconcile", id, n)
					return
				default:
					checkSnapshot(t, ix) // 链连续性 + 余额守恒，换入一半都会打破
					n++
				}
			}
		}(r)
	}

	rep, err := ix.Reconcile(switched, indexer.ReconcileOptions{BatchSize: 8})
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	t.Logf("reconcile under concurrent readers: fork point=%d, rolled back=%d, applied=%d",
		rep.ForkPoint, rep.RolledBack, rep.Applied)
	assertConverged(t, ix, switched)
}
