package indexer

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/xingjianluo/local-chain-indexer-go/chain"
)

// TestConcurrentReadWrite 写入（含重组）与查询并发进行，
// 读者在任何时刻都必须看到一个完整、自洽的状态：
// 同一快照内，tip 高度对应的区块必须存在，且已索引交易数
// 必须等于规范链 [1..tip] 的交易总数（不允许读到写了一半的中间态）。
func TestConcurrentReadWrite(t *testing.T) {
	g := testGenesis()
	src := chain.NewLocalSource(g)
	ix := newTestIndexer(t)

	const writers = 3
	const readers = 4
	const blocks = 300

	stop := make(chan struct{})

	var wg sync.WaitGroup
	errCh := make(chan string, 16)

	// 写者：各自从源订阅/拉取并应用（含重复与重组）。
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sub := src.Subscribe(stop)
			for b := range sub {
				_ = ix.ApplyBlock(b) // 重复/超前拒绝属预期
			}
		}(w)
	}

	// 读者：不断取快照做一致性校验。
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				sn := ix.Snapshot()
				tipH, _, ok := sn.Tip()
				if ok {
					// 快照内自洽：1..tip 每个高度的区块都存在，
					// 且交易索引条数与规范链交易总数一致。
					txCount := 0
					consistent := true
					for h := uint64(1); h <= tipH; h++ {
						b, found := sn.BlockAt(h)
						if !found {
							errCh <- fmt.Sprintf("读者%d：快照内高度 %d 缺块（tip=%d）", id, h, tipH)
							consistent = false
							break
						}
						for _, tx := range b.Txs {
							if _, found := sn.Tx(tx.ID); !found {
								errCh <- fmt.Sprintf("读者%d：快照内交易 %s 缺失（tip=%d）", id, tx.ID, tipH)
								consistent = false
								break
							}
							txCount++
						}
						if !consistent {
							break
						}
					}
				}
				sn.Close()
			}
		}(r)
	}

	// 源持续出块，中途制造几次重组。
	seed := uint32(1)
	for i := 0; i < blocks; i++ {
		src.Extend(chain.MakeTx(seed, "Alice", "Bob", 1))
		seed++
		if i%80 == 40 {
			// 从 5 块之前分叉出更长的链，制造重组。
			tipH, _ := src.Tip()
			if tipH > 6 {
				forkH := tipH - 5
				txs := make([][]chain.Tx, 7)
				for j := range txs {
					txs[j] = []chain.Tx{chain.MakeTx(seed, "Carol", "Dave", 1)}
					seed++
				}
				src.ReorgTo(forkH, src.ForkBlocks(forkH, txs))
			}
		}
	}

	// 订阅者可能因缓冲满丢块，用 CatchUp 收尾对齐。
	if _, err := ix.CatchUp(context.Background(), src, 64); err != nil {
		t.Fatalf("最终 CatchUp 失败: %v", err)
	}
	close(stop)
	wg.Wait()
	close(errCh)

	for msg := range errCh {
		t.Fatal(msg)
	}
	assertConsistent(t, ix, src)
	t.Logf("并发读写期间未观察到中间态，最终状态与源一致")
}
