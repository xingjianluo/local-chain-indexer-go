package indexer_test

import (
	"runtime"
	"sync"
	"testing"

	"github.com/xingjianluo/local-chain-indexer-go/indexer"
	"github.com/xingjianluo/local-chain-indexer-go/source"
)

// checkSnapshot 在一致性快照内校验不变量：
// 1) 每个高度的区块都与父区块正确链接（链连续性）；
// 2) 全部地址余额之和为 0（转账守恒，commit 原子性的直接体现）。
// 若读者能看到写了一半的中间态，这两个不变量都会被打破。
func checkSnapshot(t *testing.T, ix *indexer.Indexer) {
	t.Helper()
	ix.View(func(s *indexer.Snapshot) {
		tip, ok := s.Tip()
		if !ok {
			return
		}
		for h := uint64(1); h <= tip.Height; h++ {
			cur, _ := s.BlockAt(h)
			prev, _ := s.BlockAt(h - 1)
			if cur.ParentHash != prev.Hash {
				t.Errorf("broken chain in snapshot at height %d", h)
				return
			}
		}
		sum := int64(0)
		for i := 0; i < 8; i++ {
			sum += s.Balance(addrName(i))
		}
		if sum != 0 {
			t.Errorf("balance sum %d != 0: observed half-committed state", sum)
		}
	})
}

func addrName(i int) string {
	return "addr-" + string(rune('0'+i))
}

// TestConcurrentReadWrite 写入（含重组）与查询并发进行，
// 读者必须始终看到完整状态。用 go test -race 运行。
func TestConcurrentReadWrite(t *testing.T) {
	main := source.NewChain("main", 2000)
	fork := source.Fork(main, 1000, "fork", 2200) // 更长的分叉，将触发重组
	ix := indexer.New(indexer.WithMaxReorgDepth(2000))

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 4 个读者持续校验快照一致性。
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			n := 0
			for {
				select {
				case <-stop:
					t.Logf("reader %d did %d consistent snapshots", id, n)
					return
				default:
					checkSnapshot(t, ix)
					n++
				}
			}
		}(r)
	}

	// 写者：先推进主链，再投分叉触发重组，最后推进到分叉尖端。
	for h := 0; h < main.Len(); h++ {
		if err := ix.ApplyBlock(main.Block(uint64(h))); err != nil {
			t.Fatalf("apply main %d: %v", h, err)
		}
		runtime.Gosched() // 让读者有机会插入
	}
	for h := 1000; h < fork.Len(); h++ {
		if err := ix.ApplyBlock(fork.Block(uint64(h))); err != nil {
			t.Fatalf("apply fork %d: %v", h, err)
		}
		runtime.Gosched()
	}
	close(stop)
	wg.Wait()

	tip, _ := ix.Tip()
	if tip.Hash != fork.Tip().Hash {
		t.Fatalf("final tip %s, want fork tip %s", tip.Hash, fork.Tip().Hash)
	}
	t.Logf("concurrent phase done, final tip=%s height=%d", tip.Hash, tip.Height)
}
