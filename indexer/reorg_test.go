package indexer

import (
	"errors"
	"testing"

	"github.com/xingjianluo/local-chain-indexer-go/chain"
)

// testGenesis 构造测试用创世块。
func testGenesis() chain.Block {
	var h chain.Hash
	h[0] = 0xAA
	return chain.Block{Height: 0, Hash: h}
}

// newTestIndexer 创建注入测试日志的索引器。
func newTestIndexer(t *testing.T) *Indexer {
	t.Helper()
	ix := New(testGenesis())
	ix.SetLogger(func(format string, args ...any) { t.Logf(format, args...) })
	return ix
}

// mustApply 应用区块，失败即终止测试。
func mustApply(t *testing.T, ix *Indexer, b chain.Block) {
	t.Helper()
	if err := ix.ApplyBlock(b); err != nil {
		t.Fatalf("应用区块 h=%d 失败: %v", b.Height, err)
	}
}

func TestReorgRollback(t *testing.T) {
	ix := newTestIndexer(t)
	g := testGenesis()

	// 主链：A1(Alice->Bob 10), A2(Bob->Carol 5), A3(Carol->Dave 3)
	a1 := chain.MakeBlock(g, chain.MakeTx(1, "Alice", "Bob", 10))
	a2 := chain.MakeBlock(a1, chain.MakeTx(2, "Bob", "Carol", 5))
	a3 := chain.MakeBlock(a2, chain.MakeTx(3, "Carol", "Dave", 3))
	mustApply(t, ix, a1)
	mustApply(t, ix, a2)
	mustApply(t, ix, a3)
	t.Logf("主链已推进到 h=3 %s", a3.Hash)

	// 分叉：在 A1 之后构造更长的 B 链 B2,B3,B4
	b2 := chain.MakeBlock(a1, chain.MakeTx(11, "Alice", "Eve", 7))
	b3 := chain.MakeBlock(b2, chain.MakeTx(12, "Eve", "Bob", 2))
	b4 := chain.MakeBlock(b3, chain.MakeTx(13, "Bob", "Frank", 1))

	// B2、B3 高度不超过 tip，应仅入侧链缓存，主链状态不变。
	mustApply(t, ix, b2)
	mustApply(t, ix, b3)
	sn := ix.Snapshot()
	if h, hash, _ := sn.Tip(); h != 3 || hash != a3.Hash {
		t.Fatalf("侧链块不应改变主链 tip，got h=%d %s", h, hash)
	}
	sn.Close()
	t.Logf("B2、B3 已入侧链缓存，主链 tip 保持 h=3")

	// B4 使侧链更长，触发重组：回滚 A2、A3，重放 B2、B3、B4。
	t.Logf("触发条件：侧链尖端 h=4 超过主链 tip h=3，触发重组")
	mustApply(t, ix, b4)

	sn = ix.Snapshot()
	defer sn.Close()
	h, hash, ok := sn.Tip()
	if !ok || h != 4 || hash != b4.Hash {
		t.Fatalf("重组后 tip 应为 h=4 %s，got h=%d %s", b4.Hash, h, hash)
	}
	// 被替换高度上的交易必须撤销。
	for _, id := range []chain.Hash{a2.Txs[0].ID, a3.Txs[0].ID} {
		if _, found := sn.Tx(id); found {
			t.Fatalf("被回滚的交易 %s 不应再被索引", id)
		}
	}
	// 新链交易必须生效，且归属正确高度。
	for _, blk := range []chain.Block{b2, b3, b4} {
		rec, found := sn.Tx(blk.Txs[0].ID)
		if !found {
			t.Fatalf("新链交易 %s 应被索引", blk.Txs[0].ID)
		}
		if rec.BlockHeight != blk.Height || rec.BlockHash != blk.Hash {
			t.Fatalf("交易归属错误: got h=%d %s, want h=%d %s",
				rec.BlockHeight, rec.BlockHash, blk.Height, blk.Hash)
		}
	}
	// 分叉点之前的交易不受影响。
	if _, found := sn.Tx(a1.Txs[0].ID); !found {
		t.Fatal("分叉点之前的交易 A1 不应受影响")
	}
	// 派生数据（余额）必须与最终链一致：
	// A1: Alice-10 Bob+10；B2: Alice-7 Eve+7；B3: Eve-2 Bob+2；B4: Bob-1 Frank+1
	want := map[string]int64{"Alice": -17, "Bob": 11, "Eve": 5, "Frank": 1, "Carol": 0, "Dave": 0}
	for acct, bal := range want {
		if got := sn.Balance(acct); got != bal {
			t.Fatalf("账户 %s 余额=%d，期望 %d", acct, got, bal)
		}
	}
	t.Logf("重组后状态校验通过：tip h=4，余额 %v", want)
}

func TestRejectReasons(t *testing.T) {
	ix := newTestIndexer(t)
	ix.SetMaxReorgDepth(2)
	g := testGenesis()

	a1 := chain.MakeBlock(g, chain.MakeTx(1, "Alice", "Bob", 10))
	a2 := chain.MakeBlock(a1, chain.MakeTx(2, "Bob", "Carol", 5))
	a3 := chain.MakeBlock(a2, chain.MakeTx(3, "Carol", "Dave", 3))
	mustApply(t, ix, a1)
	mustApply(t, ix, a2)
	mustApply(t, ix, a3)

	// 1. 重复投递。
	err := ix.ApplyBlock(a2)
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("重复投递应报 ErrDuplicate，got %v", err)
	}
	t.Logf("重复投递 h=%d -> %v", a2.Height, err)

	// 2. 高度回退：替换创世块。
	var fakeHash chain.Hash
	fakeHash[0] = 0xBB
	genesis2 := chain.Block{Height: 0, Hash: fakeHash}
	err = ix.ApplyBlock(genesis2)
	if !errors.Is(err, ErrHeightRegression) {
		t.Fatalf("替换创世块应报 ErrHeightRegression，got %v", err)
	}
	t.Logf("替换创世块 -> %v", err)

	// 3. 父区块对不上：高度 tip+1 但父哈希错误。
	bad := chain.MakeBlock(a3, chain.MakeTx(4, "A", "B", 1))
	bad.ParentHash = fakeHash
	err = ix.ApplyBlock(bad)
	if !errors.Is(err, ErrParentMismatch) {
		t.Fatalf("父哈希错误应报 ErrParentMismatch，got %v", err)
	}
	t.Logf("父哈希错误 -> %v", err)

	// 4. 超前乱序块：父尚未索引。
	a5 := chain.MakeBlock(chain.MakeBlock(a3), chain.MakeTx(5, "A", "B", 1))
	err = ix.ApplyBlock(a5)
	if !errors.Is(err, ErrParentMismatch) {
		t.Fatalf("超前块应报 ErrParentMismatch，got %v", err)
	}
	t.Logf("超前块 h=%d -> %v", a5.Height, err)

	// 5. 重组深度超限：maxReorgDepth=2，分叉点在 h=0（回退 3 块）。
	deep := chain.MakeBlock(g, chain.MakeTx(6, "X", "Y", 1))
	err = ix.ApplyBlock(deep)
	if !errors.Is(err, ErrReorgTooDeep) {
		t.Fatalf("深分叉应报 ErrReorgTooDeep，got %v", err)
	}
	t.Logf("深分叉 h=%d -> %v", deep.Height, err)

	// 所有拒绝不得改变索引状态。
	sn := ix.Snapshot()
	defer sn.Close()
	if h, hash, _ := sn.Tip(); h != 3 || hash != a3.Hash {
		t.Fatalf("拒绝后 tip 不应变化，got h=%d %s", h, hash)
	}
	if got := sn.Balance("Bob"); got != 5 {
		t.Fatalf("拒绝后余额不应变化，Bob=%d，期望 5", got)
	}
}
