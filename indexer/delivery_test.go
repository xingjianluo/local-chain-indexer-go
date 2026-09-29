package indexer_test

import (
	"errors"
	"testing"

	"github.com/xingjianluo/local-chain-indexer-go/chain"
	"github.com/xingjianluo/local-chain-indexer-go/indexer"
	"github.com/xingjianluo/local-chain-indexer-go/source"
)

// TestDuplicateAndOutOfOrder 模拟网络层的重复与乱序投递：
// 最终索引状态必须与按序投递完全一致，且不允许重复记账。
func TestDuplicateAndOutOfOrder(t *testing.T) {
	c := source.NewChain("main", 8)
	ix := indexer.New()

	// 顺序：0,1,2,2(重复),4(空洞，拒),3,4,5,3(重复),6,7,7(重复)
	seq := []int{0, 1, 2, 2, 4, 3, 4, 5, 3, 6, 7, 7}
	applied := 0
	for _, h := range seq {
		err := ix.ApplyBlock(c.Block(uint64(h)))
		var re *chain.RejectError
		switch {
		case err == nil:
			applied++
			t.Logf("height %d applied (tip=%d)", h, h)
		case errors.As(err, &re) && re.Reason == chain.ReasonDuplicate:
			t.Logf("height %d duplicate delivery ignored", h)
		case errors.As(err, &re) && re.Reason == chain.ReasonGap:
			t.Logf("height %d arrived too early (gap), will retry later", h)
		default:
			t.Fatalf("height %d: unexpected error %v", h, err)
		}
	}
	if applied != 8 {
		t.Fatalf("applied %d blocks, want 8 (no double-counting)", applied)
	}

	// 与干净顺序投递的索引器对比，状态必须一致。
	ref := indexer.New()
	for h := 0; h < c.Len(); h++ {
		if err := ref.ApplyBlock(c.Block(uint64(h))); err != nil {
			t.Fatal(err)
		}
	}
	tipA, _ := ix.Tip()
	tipB, _ := ref.Tip()
	if tipA != tipB {
		t.Fatalf("tips differ: %+v vs %+v", tipA, tipB)
	}
	for h := 0; h < c.Len(); h++ {
		ba, _ := ix.BlockAt(uint64(h))
		bb, _ := ref.BlockAt(uint64(h))
		if ba.Hash != bb.Hash {
			t.Fatalf("height %d differs", h)
		}
		for _, tx := range ba.Txs {
			ra, okA := ix.GetTx(tx.Hash)
			_, okB := ref.GetTx(tx.Hash)
			if !okA || !okB || ra.Height != uint64(h) {
				t.Fatalf("tx %s mis-indexed", tx.Hash)
			}
		}
	}
	for addr, want := range balancesOf(c) {
		if got := ix.Balance(addr); got != want {
			t.Fatalf("balance %s: got %d want %d (double-counted?)", addr, got, want)
		}
	}
	t.Logf("final state identical to in-order delivery: tip=%s", tipA.Hash)
}
