package indexer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xingjianluo/local-chain-indexer-go/chain"
)

// catchUpCost 度量追赶 n 个区块的总耗时。
func catchUpCost(n int) time.Duration {
	g := testGenesis()
	src := chain.NewLocalSource(g)
	for i := 0; i < n; i++ {
		src.Extend(chain.MakeTx(uint32(1000+i), fmt.Sprintf("U%d", i%16), fmt.Sprintf("U%d", (i+1)%16), 1))
	}
	ix := New(g)
	ix.SetLogger(func(string, ...any) {}) // 静默，排除日志开销
	start := time.Now()
	if _, err := ix.CatchUp(context.Background(), src, 128); err != nil {
		panic(err)
	}
	return time.Since(start)
}

// TestCatchUpScalesLinearly 数据量放大 8 倍时，
// 单块均摊耗时的增长不得超过 4 倍（留足环境噪声余量），
// 以此捕获明显的超线性劣化（如每块 O(n) 的全量拷贝）。
func TestCatchUpScalesLinearly(t *testing.T) {
	if testing.Short() {
		t.Skip("short 模式跳过扩展性测试")
	}
	const small, large = 1000, 8000
	dSmall := catchUpCost(small)
	dLarge := catchUpCost(large)
	perSmall := dSmall / small
	perLarge := dLarge / large
	t.Logf("追赶 %d 块耗时 %v（均摊 %v/块）；%d 块耗时 %v（均摊 %v/块）",
		small, dSmall, perSmall, large, dLarge, perLarge)
	if perLarge > 4*perSmall {
		t.Fatalf("均摊耗时出现超线性劣化：%d 块时 %v/块，%d 块时 %v/块",
			small, perSmall, large, perLarge)
	}
}
