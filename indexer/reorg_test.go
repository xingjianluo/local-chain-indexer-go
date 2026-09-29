package indexer_test

import (
	"testing"

	"github.com/xingjianluo/local-chain-indexer-go/indexer"
	"github.com/xingjianluo/local-chain-indexer-go/source"
)

// balancesOf 逐笔重放一条链，算出期望余额。
func balancesOf(c *source.Chain) map[string]int64 {
	want := map[string]int64{}
	for h := 0; h < c.Len(); h++ {
		for _, tx := range c.Block(uint64(h)).Txs {
			want[tx.From] -= int64(tx.Amount)
			want[tx.To] += int64(tx.Amount)
		}
	}
	return want
}

func TestReorgRollbackAndReplay(t *testing.T) {
	main := source.NewChain("main", 10)          // 高度 0..9
	fork := source.Fork(main, 6, "fork", 12)     // 从高度 6 分叉，长度 12（更长）

	ix := indexer.New()
	for h := 0; h < main.Len(); h++ {
		if err := ix.ApplyBlock(main.Block(uint64(h))); err != nil {
			t.Fatalf("apply main %d: %v", h, err)
		}
	}
	// 记录被替换高度上的旧交易，重组后必须查不到。
	orphanedTxs := map[uint64][]string{}
	for h := 6; h < 10; h++ {
		for _, tx := range main.Block(uint64(h)).Txs {
			orphanedTxs[uint64(h)] = append(orphanedTxs[uint64(h)], tx.Hash.String())
		}
	}

	// 逐块投分叉：高度 6..9 只缓存不重组（分叉尚不更长）。
	for h := 6; h <= 9; h++ {
		if err := ix.ApplyBlock(fork.Block(uint64(h))); err != nil {
			t.Fatalf("buffer fork %d: %v", h, err)
		}
		tip, _ := ix.Tip()
		if tip.Hash != main.Tip().Hash {
			t.Fatalf("reorg too early at fork height %d", h)
		}
	}
	t.Logf("fork blocks 6..9 buffered, canonical tip still %s", main.Tip().Hash)

	// 高度 10 使分叉严格更长：触发重组。
	if err := ix.ApplyBlock(fork.Block(10)); err != nil {
		t.Fatalf("trigger reorg: %v", err)
	}
	tip, _ := ix.Tip()
	if tip.Height != 10 || tip.Hash != fork.Block(10).Hash {
		t.Fatalf("tip after reorg: %+v", tip)
	}
	t.Logf("reorg done: rolled back main 6..9, replayed fork 6..10, tip=%s", tip.Hash)

	// 被替换高度的区块与交易必须被撤掉。
	for h := 6; h < 10; h++ {
		blk, _ := ix.BlockAt(uint64(h))
		if blk.Hash != fork.Block(uint64(h)).Hash {
			t.Fatalf("height %d still on old chain", h)
		}
		for _, tx := range main.Block(uint64(h)).Txs {
			if _, ok := ix.GetTx(tx.Hash); ok {
				t.Fatalf("orphaned tx %s at height %d still indexed", tx.Hash, h)
			}
		}
	}

	// 继续推进到分叉尖端，最终状态必须与 fork 链完全一致。
	if err := ix.ApplyBlock(fork.Block(11)); err != nil {
		t.Fatalf("extend to fork tip: %v", err)
	}
	for addr, want := range balancesOf(fork) {
		if got := ix.Balance(addr); got != want {
			t.Fatalf("balance %s: got %d want %d", addr, got, want)
		}
	}
	for h := 0; h < fork.Len(); h++ {
		blk, ok := ix.BlockAt(uint64(h))
		if !ok || blk.Hash != fork.Block(uint64(h)).Hash {
			t.Fatalf("height %d mismatch after reorg", h)
		}
	}
	t.Logf("final state identical to fork chain: %d blocks, balances verified", fork.Len())
}

func TestReorgAtomicityUnderFailure(t *testing.T) {
	// 重组路径上的分叉块若父链断裂（防御性分支），整个重组不得生效。
	main := source.NewChain("main", 8)
	fork := source.Fork(main, 4, "fork", 10)
	ix := indexer.New()
	for h := 0; h < main.Len(); h++ {
		if err := ix.ApplyBlock(main.Block(uint64(h))); err != nil {
			t.Fatalf("apply main %d: %v", h, err)
		}
	}
	// 跳过高度 5 直接投 6：父未知，拒绝且状态不变。
	err := ix.ApplyBlock(fork.Block(6))
	if err == nil {
		t.Fatal("expected parent-mismatch for broken fork path")
	}
	tip, _ := ix.Tip()
	if tip.Hash != main.Tip().Hash {
		t.Fatalf("state changed after failed fork delivery: tip=%s", tip.Hash)
	}
	for addr, want := range balancesOf(main) {
		if got := ix.Balance(addr); got != want {
			t.Fatalf("balance %s polluted: got %d want %d", addr, got, want)
		}
	}
	t.Logf("failed fork delivery left no partial state: tip=%s", tip.Hash)
}
