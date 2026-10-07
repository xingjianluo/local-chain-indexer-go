package indexer_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/xingjianluo/local-chain-indexer-go/chain"
	"github.com/xingjianluo/local-chain-indexer-go/indexer"
	"github.com/xingjianluo/local-chain-indexer-go/source"
)

// growingSource 是一个"停不下来"的源：每次 Blocks 调用有效链长都 +1，
// 因此对账期间探测到的尖端永远在变，稳定性复检必然失败。
type growingSource struct {
	chain *source.Chain
	mu    sync.Mutex
	calls int
	base  int
}

func (g *growingSource) Blocks(from uint64, limit int) ([]chain.Block, error) {
	g.mu.Lock()
	g.calls++
	n := g.base + g.calls
	g.mu.Unlock()
	if n > g.chain.Len() {
		n = g.chain.Len()
	}
	if int(from) >= n {
		return nil, nil
	}
	end := int(from) + limit
	if end > n {
		end = n
	}
	out := make([]chain.Block, 0, end-int(from))
	for i := int(from); i < end; i++ {
		out = append(out, g.chain.Block(uint64(i)))
	}
	return out, nil
}

// assertChainMatches 断言索引器逐高度的区块、抽样交易位置、余额都与源链一致。
func assertChainMatches(t *testing.T, ix *indexer.Indexer, c *source.Chain) {
	t.Helper()
	tip, ok := ix.Tip()
	if !ok || tip.Hash != c.Tip().Hash || tip.Height != c.Tip().Height {
		t.Fatalf("tip mismatch: got %+v ok=%v, want height=%d hash=%s",
			tip, ok, c.Tip().Height, c.Tip().Hash)
	}
	for h := 0; h < c.Len(); h++ {
		blk, ok := ix.BlockAt(uint64(h))
		if !ok || blk.Hash != c.Block(uint64(h)).Hash {
			t.Fatalf("height %d: got %+v ok=%v, want hash %s",
				h, blk.Hash, ok, c.Block(uint64(h)).Hash)
		}
	}
	// 抽样交易位置：交易必须在它所属的高度与区块里。
	for h := 0; h < c.Len(); h += 37 {
		want := c.Block(uint64(h))
		rec, ok := ix.GetTx(want.Txs[0].Hash)
		if !ok {
			t.Fatalf("tx from height %d not found", h)
		}
		if rec.Height != uint64(h) || rec.Block != want.Hash {
			t.Fatalf("tx location: got height=%d block=%s, want height=%d block=%s",
				rec.Height, rec.Block, h, want.Hash)
		}
	}
	for addr, want := range balancesOf(c) {
		if got := ix.Balance(addr); got != want {
			t.Fatalf("balance %s: got %d want %d", addr, got, want)
		}
	}
	t.Logf("state identical to source chain %q: %d heights, tip=%s",
		c.Tip().Hash, c.Len(), tip.Hash)
}

// TestReconcileAfterSourceChainSwitch 覆盖核心场景：
// 中断期间源从中间某高度分叉出去、换成了另一条链，索引器靠一次对账
// 自动收敛到源的规范链——包括源换链后高度并不比本地高的情况。
func TestReconcileAfterSourceChainSwitch(t *testing.T) {
	chainA := source.NewChain("main", 300)

	// 停机前：索引到高度 199。
	newStuckIndexer := func() *indexer.Indexer {
		ix := indexer.New()
		for h := 0; h < 200; h++ {
			if err := ix.ApplyBlock(chainA.Block(uint64(h))); err != nil {
				t.Fatal(err)
			}
		}
		return ix
	}

	t.Run("源换成更长的链", func(t *testing.T) {
		ix := newStuckIndexer()
		// 停机期间：源从高度 150 分叉，换成 500 长的另一条链。
		chainB := source.Fork(chainA, 150, "main-v2", 500)
		t.Logf("outage: local stuck at height 199 on %q, source switched to %q (fork at 150, tip 499)",
			"main", "main-v2")

		// 对账期间并发读者只能看到完整的旧链或新链。
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					checkSnapshot(t, ix)
				}
			}
		}()

		rep, err := ix.Reconcile(chainB, 32)
		close(stop)
		wg.Wait()
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		t.Logf("reconcile report: forkHeight=%d rolledBack=%d applied=%d attempts=%d",
			rep.ForkHeight, rep.RolledBack, rep.Applied, rep.Attempts)
		if rep.AlreadyInSync || rep.FromGenesis {
			t.Fatalf("unexpected report flags: %+v", rep)
		}
		if rep.ForkHeight != 149 || rep.RolledBack != 50 || rep.Applied != 350 {
			t.Fatalf("report: got fork=%d rolled=%d applied=%d, want 149/50/350",
				rep.ForkHeight, rep.RolledBack, rep.Applied)
		}

		// 旧链上被撤掉的交易（高度 150..199 的 main 块）必须查不到。
		orphaned := chainA.Block(160).Txs[0].Hash
		if _, ok := ix.GetTx(orphaned); ok {
			t.Fatalf("tx from replaced chain still indexed")
		}
		t.Logf("orphaned tx from old chain no longer indexed: %s", orphaned)

		assertChainMatches(t, ix, chainB)

		// 再对一次：已一致，不应有任何改动。
		rep2, err := ix.Reconcile(chainB, 32)
		if err != nil {
			t.Fatalf("second reconcile: %v", err)
		}
		if !rep2.AlreadyInSync || rep2.RolledBack != 0 || rep2.Applied != 0 {
			t.Fatalf("second reconcile should be a no-op: %+v", rep2)
		}
		t.Logf("second reconcile: already in sync, no changes")
	})

	t.Run("源换链但高度并不比本地高", func(t *testing.T) {
		ix := newStuckIndexer()
		// 源从高度 150 分叉，新链只有 180 长——比本地尖端 199 还低。
		chainC := source.Fork(chainA, 150, "main-v3", 180)
		t.Logf("outage: local stuck at height 199, source switched to SHORTER chain (tip 179)")

		rep, err := ix.Reconcile(chainC, 32)
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		t.Logf("reconcile report: forkHeight=%d rolledBack=%d applied=%d newTip=%d",
			rep.ForkHeight, rep.RolledBack, rep.Applied, rep.NewTip.Height)
		if rep.ForkHeight != 149 || rep.RolledBack != 50 || rep.Applied != 30 {
			t.Fatalf("report: got fork=%d rolled=%d applied=%d, want 149/50/30",
				rep.ForkHeight, rep.RolledBack, rep.Applied)
		}
		assertChainMatches(t, ix, chainC)
	})
}

// TestReconcileDivergenceAndUnstable 覆盖两类可控失败：
// 差异超过配置上限、源持续变动对不齐——都要明确报错且索引保持不动。
func TestReconcileDivergenceAndUnstable(t *testing.T) {
	chainA := source.NewChain("main", 300)

	// snapshotOf 记录索引关键状态，用于断言"失败后什么都没动"。
	type stateSnapshot struct {
		tip     chain.Header
		blk40   chain.Hash
		balance map[string]int64
		tx      chain.Hash
	}
	snapshotOf := func(ix *indexer.Indexer) stateSnapshot {
		tip, _ := ix.Tip()
		blk, _ := ix.BlockAt(40)
		bal := map[string]int64{}
		for i := 0; i < 8; i++ {
			bal[addrName(i)] = ix.Balance(addrName(i))
		}
		return stateSnapshot{tip: tip, blk40: blk.Hash, balance: bal, tx: chainA.Block(40).Txs[0].Hash}
	}
	assertUnchanged := func(ix *indexer.Indexer, before stateSnapshot) {
		t.Helper()
		tip, _ := ix.Tip()
		if tip != before.tip {
			t.Fatalf("tip moved after failed reconcile: %+v -> %+v", before.tip, tip)
		}
		blk, _ := ix.BlockAt(40)
		if blk.Hash != before.blk40 {
			t.Fatalf("block at height 40 changed after failed reconcile")
		}
		for addr, want := range before.balance {
			if got := ix.Balance(addr); got != want {
				t.Fatalf("balance %s changed: %d -> %d", addr, want, got)
			}
		}
		rec, ok := ix.GetTx(before.tx)
		if !ok || rec.Height != 40 {
			t.Fatalf("tx from old chain lost after failed reconcile")
		}
		t.Logf("index untouched after failed reconcile: tip height=%d hash=%s",
			tip.Height, tip.Hash)
	}

	t.Run("差异超过上限", func(t *testing.T) {
		// 允许的对账回滚深度只有 16；源从高度 10 分叉，差异 90，远超上限。
		ix := indexer.New(indexer.WithMaxReconcileDepth(16))
		for h := 0; h < 100; h++ {
			if err := ix.ApplyBlock(chainA.Block(uint64(h))); err != nil {
				t.Fatal(err)
			}
		}
		deepFork := source.Fork(chainA, 10, "deep-fork", 200)
		before := snapshotOf(ix)

		_, err := ix.Reconcile(deepFork, 32)
		var re *indexer.ReconcileError
		if !errors.As(err, &re) {
			t.Fatalf("expected *ReconcileError, got %T: %v", err, err)
		}
		if re.Kind != indexer.KindDivergenceTooDeep {
			t.Fatalf("kind: got %s, want %s", re.Kind, indexer.KindDivergenceTooDeep)
		}
		t.Logf("reconcile refused as expected: kind=%s detail=%s", re.Kind, re.Detail)
		assertUnchanged(ix, before)
	})

	t.Run("源持续变动对不齐", func(t *testing.T) {
		ix := indexer.New()
		for h := 0; h < 50; h++ {
			if err := ix.ApplyBlock(chainA.Block(uint64(h))); err != nil {
				t.Fatal(err)
			}
		}
		// 源每次被调用链长都变，尖端永远对不齐。
		unstable := &growingSource{chain: source.NewChain("growing", 100000), base: 100}
		before := snapshotOf(ix)

		_, err := ix.Reconcile(unstable, 32)
		var re *indexer.ReconcileError
		if !errors.As(err, &re) {
			t.Fatalf("expected *ReconcileError, got %T: %v", err, err)
		}
		if re.Kind != indexer.KindSourceUnstable {
			t.Fatalf("kind: got %s, want %s", re.Kind, indexer.KindSourceUnstable)
		}
		t.Logf("reconcile gave up as expected: kind=%s detail=%s", re.Kind, re.Detail)
		assertUnchanged(ix, before)
	})
}
