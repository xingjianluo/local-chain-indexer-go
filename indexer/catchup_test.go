package indexer

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/xingjianluo/local-chain-indexer-go/chain"
)

// applyChain 从源按高度顺序把 [1..n] 的块应用到索引器。
func applyChain(t *testing.T, ix *Indexer, src *chain.LocalSource, n uint64) {
	t.Helper()
	for h := uint64(1); h <= n; h++ {
		b, ok := src.BlockByHeight(h)
		if !ok {
			t.Fatalf("源缺少高度 %d 的块", h)
		}
		mustApply(t, ix, b)
	}
}

// assertConsistent 断言索引状态与源的规范链一致（含派生余额）。
func assertConsistent(t *testing.T, ix *Indexer, src *chain.LocalSource) {
	t.Helper()
	sn := ix.Snapshot()
	defer sn.Close()
	srcTipH, srcTipHash := src.Tip()
	h, hash, ok := sn.Tip()
	if !ok || h != srcTipH || hash != srcTipHash {
		t.Fatalf("tip 不一致：索引 h=%d %s，源 h=%d %s", h, hash, srcTipH, srcTipHash)
	}
	// 逐高度比对区块哈希，并重放交易计算期望余额。
	bal := make(map[string]int64)
	for hh := uint64(1); hh <= srcTipH; hh++ {
		want, _ := src.BlockByHeight(hh)
		got, ok := sn.BlockAt(hh)
		if !ok || got.Hash != want.Hash {
			t.Fatalf("高度 %d 区块不一致：索引 %s，源 %s", hh, got.Hash, want.Hash)
		}
		for _, tx := range want.Txs {
			rec, found := sn.Tx(tx.ID)
			if !found || rec.BlockHeight != hh {
				t.Fatalf("交易 %s 索引缺失或高度错误", tx.ID)
			}
			bal[tx.From] -= tx.Amount
			bal[tx.To] += tx.Amount
		}
	}
	for acct, want := range bal {
		if got := sn.Balance(acct); got != want {
			t.Fatalf("账户 %s 余额=%d，期望 %d", acct, got, want)
		}
	}
	t.Logf("一致性校验通过：tip h=%d，账户数=%d", srcTipH, len(bal))
}

func TestDuplicateAndOutOfOrderDelivery(t *testing.T) {
	g := testGenesis()
	src := chain.NewLocalSource(g)
	ix := newTestIndexer(t)

	// 源推进 5 块。
	var blocks []chain.Block
	for i := 0; i < 5; i++ {
		blocks = append(blocks, src.Extend(chain.MakeTx(uint32(i+1), "Alice", "Bob", int64(i+1))))
	}

	// 乱序 + 重复投递：3, 1, 2, 2, 5, 4, 1。
	order := []int{2, 0, 1, 1, 4, 3, 0}
	var dupCount, aheadCount int
	for _, i := range order {
		err := ix.ApplyBlock(blocks[i])
		switch {
		case err == nil:
			t.Logf("投递 h=%d：已应用", blocks[i].Height)
		case errors.Is(err, ErrDuplicate):
			dupCount++
			t.Logf("投递 h=%d：重复，拒绝", blocks[i].Height)
		case errors.Is(err, ErrParentMismatch):
			aheadCount++
			t.Logf("投递 h=%d：超前/乱序，拒绝待追赶", blocks[i].Height)
		default:
			t.Fatalf("投递 h=%d 出现意外错误: %v", blocks[i].Height, err)
		}
	}
	if dupCount == 0 || aheadCount == 0 {
		t.Fatalf("场景未覆盖：重复 %d 次，超前 %d 次", dupCount, aheadCount)
	}

	// 缺口由 CatchUp 按高度补齐。
	n, err := ix.CatchUp(context.Background(), src, 2) // 小批次验证分批推进
	if err != nil {
		t.Fatalf("CatchUp 失败: %v", err)
	}
	t.Logf("CatchUp 应用 %d 块", n)
	assertConsistent(t, ix, src)
}

func TestCatchUpAfterLongOutage(t *testing.T) {
	g := testGenesis()
	src := chain.NewLocalSource(g)
	ix := newTestIndexer(t)

	// 正常跟随 3 块。
	for i := 0; i < 3; i++ {
		mustApply(t, ix, src.Extend(chain.MakeTx(uint32(i+1), "Alice", "Bob", 1)))
	}
	t.Logf("模拟长时间中断：索引器停在 h=3，源继续出块")

	// 中断期间源积累 500 块（积压）。
	const backlog = 500
	for i := 0; i < backlog; i++ {
		src.Extend(chain.MakeTx(uint32(1000+i), fmt.Sprintf("U%d", i%7), fmt.Sprintf("U%d", (i+1)%7), 1))
	}

	// 恢复后分批追赶，批大小 32（内存上界），断言分批生效。
	n, err := ix.CatchUp(context.Background(), src, 32)
	if err != nil {
		t.Fatalf("CatchUp 失败: %v", err)
	}
	if n != backlog {
		t.Fatalf("CatchUp 应用块数=%d，期望 %d", n, backlog)
	}
	assertConsistent(t, ix, src)
}

func TestCatchUpAcrossSourceReorg(t *testing.T) {
	g := testGenesis()
	src := chain.NewLocalSource(g)
	ix := newTestIndexer(t)

	// 索引器跟随主链 4 块。
	for i := 0; i < 4; i++ {
		mustApply(t, ix, src.Extend(chain.MakeTx(uint32(i+1), "Alice", "Bob", 1)))
	}

	// 中断期间源发生重组：从 h=1 分叉出更长的链（h=2..6）。
	forkTxs := make([][]chain.Tx, 5)
	for i := range forkTxs {
		forkTxs[i] = []chain.Tx{chain.MakeTx(uint32(50+i), "Carol", "Dave", int64(i+1))}
	}
	newBlocks := src.ForkBlocks(1, forkTxs)
	src.ReorgTo(1, newBlocks)
	t.Logf("源在 h=1 处重组，新链长 %d；索引器仍持有旧链 h=4", len(newBlocks)+2)

	// 恢复后 CatchUp：拉到的新链块走侧链路径，最终触发重组并追平。
	n, err := ix.CatchUp(context.Background(), src, 3)
	if err != nil {
		t.Fatalf("CatchUp 失败: %v", err)
	}
	t.Logf("CatchUp 应用 %d 块（含重组重放）", n)
	assertConsistent(t, ix, src)
}
