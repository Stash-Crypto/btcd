// Copyright (c) 2013-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"testing"
	"math/rand"
	
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcd/mining"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
)

func NewTestFeeEstimator(binSize, maxReplacements, maxRollback uint32) *feeEstimator {
	return &feeEstimator{
		maxRollback : maxRollback,
		height : mempoolHeight, 
		binSize : int(binSize), 
		maxReplacements : int(maxReplacements), 
		observed : make(map[chainhash.Hash]observedTransaction), 
		dropped : make([][]*observedTransaction, 0, maxRollback), 
	}
}

type estimateFeeTester struct {
	t *testing.T
	version int32
	height int32
}

func (eft *estimateFeeTester) testTx(fee uint64) *mining.TxDesc {
	eft.version ++
	return &mining.TxDesc{
		Tx: btcutil.NewTx(&wire.MsgTx{
			Version: eft.version,
		}), 
		Height: eft.height, 
		Fee: int64(fee), 
	}
}

func expectedFeePerKb(t *mining.TxDesc) float64 {
	size := uint32(t.Tx.MsgTx().SerializeSize())
	fee := uint64(t.Fee)
	
	return float64(1000 * fee)/float64(size)
}

func (eft *estimateFeeTester) testBlock(txs []*wire.MsgTx) *btcutil.Block {
	
	eft.height ++
	block := btcutil.NewBlock(&wire.MsgBlock{
		Transactions : txs,
	})
	block.SetHeight(eft.height)
	
	return block
}

func TestEstimateFee(t *testing.T) {
	ef := NewTestFeeEstimator(5, 3, 0)
	eft := estimateFeeTester{t: t}
	
	// Try with no txs and get zero for all queries. 
	expected := 0.0
	for i := uint32(1); i <= estimateFeeBins; i ++ {
		estimated := ef.EstimateFee(i)
		
		if estimated != expected {
			t.Errorf("Estimate fee error: expected %s when estimator is empty; got %s", expected, estimated)
		}
	}
	
	// Now insert a tx. 
	tx := eft.testTx(1000000)
	ef.ObserveTransaction(tx)
	
	// Expected should still be zero because this is still in the mempool.
	expected = 0.0
	for i := uint32(1); i <= estimateFeeBins; i ++ {
		estimated := ef.EstimateFee(i)
		
		if estimated != expected {
			t.Errorf("Estimate fee error: expected %s when estimator has one tx in mempool; got %s", expected, estimated)
		}
	}
	
	// Record a block.
	ef.RecordBlock(eft.testBlock([]*wire.MsgTx{tx.Tx.MsgTx()}))
	expected = expectedFeePerKb(tx)
	for i := uint32(1); i <= estimateFeeBins; i ++ {
		estimated := ef.EstimateFee(i)
		
		if estimated != expected {
			t.Errorf("Estimate fee error: expected %f when one tx is binned; got %f", expected, estimated)
		}
	}
	
	// Create some more transactions. 
	txA := eft.testTx( 500000)
	txB := eft.testTx(2000000)
	txC := eft.testTx(4000000)
	ef.ObserveTransaction(txA)
	ef.ObserveTransaction(txB)
	ef.ObserveTransaction(txC)
	
	// Record 8 empty blocks. 
	for i := 0; i < 8; i++ {
		ef.RecordBlock(eft.testBlock([]*wire.MsgTx{}))
	}
	
	// Mine the first tx. 
	ef.RecordBlock(eft.testBlock([]*wire.MsgTx{txA.Tx.MsgTx()}))
	
	// Now the estimated amount should depend on the value 
	// of the argument to estimate fee. 
	for i := uint32(1); i <= estimateFeeBins; i ++ {
		estimated := ef.EstimateFee(i)
		if i > 8 {
			expected = expectedFeePerKb(txA)
		} else {
			expected = expectedFeePerKb(tx)
		}
		if estimated != expected {
			t.Errorf("Estimate fee error: expected %f on round %d; got %f", expected, i, estimated)
		}
	}
	
	// Record 5 more empty blocks. 
	for i := 0; i < 5; i++ {
		ef.RecordBlock(eft.testBlock([]*wire.MsgTx{}))
	}
	
	// Mine the next tx. 
	ef.RecordBlock(eft.testBlock([]*wire.MsgTx{txB.Tx.MsgTx()}))
	
	// Now the estimated amount should depend on the value 
	// of the argument to estimate fee. 
	for i := uint32(1); i <= estimateFeeBins; i ++ {
		estimated := ef.EstimateFee(i)
		if i <= 8 {
			expected = expectedFeePerKb(txB)
		} else if i <= 8 + 6 {
			expected = expectedFeePerKb(tx)
		} else {
			expected = expectedFeePerKb(txA)
		}
		
		if estimated != expected {
			t.Errorf("Estimate fee error: expected %f on round %d; got %f", expected, i, estimated)
		}
	}
	
	// Record 9 more empty blocks. 
	for i := 0; i < 10; i++ {
		ef.RecordBlock(eft.testBlock([]*wire.MsgTx{}))
	}
	
	// Mine txC. 
	ef.RecordBlock(eft.testBlock([]*wire.MsgTx{txC.Tx.MsgTx()}))
	
	// This should have no effect on the outcome because too 
	// many blocks have been mined for txC to be recorded.
	for i := uint32(1); i <= estimateFeeBins; i ++ {
		estimated := ef.EstimateFee(i)
		if i <= 8 {
			expected = expectedFeePerKb(txB)
		} else if i <= 8 + 6 {
			expected = expectedFeePerKb(tx)
		} else {
			expected = expectedFeePerKb(txA)
		}
		
		if estimated != expected {
			t.Errorf("Estimate fee error: expected %f on round %d; got %f", expected, i, estimated)
		}
	}
}

func (eft *estimateFeeTester) estimates(ef *feeEstimator) [estimateFeeBins]float64 {
	
	// Generate estimates
	var estimates [estimateFeeBins]float64
	for i := 0; i < estimateFeeBins; i++ {
		estimates[i] = ef.EstimateFee(1)
	}
	
	// Check that all estimated fee results go in descending order. 
	for i := 1; i < estimateFeeBins; i++ {
		if estimates[i] > estimates[i - 1] {
			eft.t.Error("Estimates not in descending order.")
		}
	}
	
	return estimates
}

func (eft *estimateFeeTester) round(ef *feeEstimator, 
	txHistory [][]*mining.TxDesc, blockHistory []*btcutil.Block, 
	estimateHistory [][estimateFeeBins]float64, 
	txPerRound, txPerBlock, maxRollback uint32) ([][]*mining.TxDesc, 
	[]*btcutil.Block, [][estimateFeeBins]float64) {
		
	// generate new txs.
	var newTxs []*mining.TxDesc
	for i := uint32(0); i < txPerRound; i++ {
		newTx := eft.testTx(uint64(rand.Intn(1000000)))
		ef.ObserveTransaction(newTx) 
		newTxs = append(newTxs, newTx)
	}
	
	// Construct new tx history.
	txHistory = append(txHistory, newTxs)
	if len(txHistory) > estimateFeeBins {
		txHistory = txHistory[1 : estimateFeeBins + 1]
	}
	
	// generate new block, with no duplicates.
	newBlockTxs := make(map[chainhash.Hash]*wire.MsgTx)
	i := uint32(0)
	for i < txPerBlock {
		n := rand.Intn(len(txHistory))
		m := rand.Intn(int(txPerRound))
			
		tx := txHistory[n][m]
		hash := *tx.Tx.Hash()
			
		if _, ok := newBlockTxs[hash]; ok {
			continue
		}
		
		newBlockTxs[hash] = tx.Tx.MsgTx()
		i++
	}
	
	var newBlockList []*wire.MsgTx
	for _, tx := range newBlockTxs {
		newBlockList = append(newBlockList, tx) 
	}
	
	newBlock := eft.testBlock(newBlockList)
	ef.RecordBlock(newBlock)
	
	// return results.
	estimates := eft.estimates(ef)

	// Return results
	blockHistory = append(blockHistory, newBlock)
	if len(blockHistory) > int(maxRollback) {
		blockHistory = blockHistory[1 : maxRollback + 1]
	}
	
	return txHistory, blockHistory, append(estimateHistory, estimates)
}

func TestEstimateFeeRollback(t *testing.T) {
	txPerRound      := uint32(20)
	txPerBlock      := uint32(10)
	binSize         := uint32(5)
	maxReplacements := uint32(3)
	stepsBack       :=        2
	rounds          :=        30
	
	ef := NewTestFeeEstimator(binSize, maxReplacements, uint32(stepsBack))
	eft := estimateFeeTester{t: t}
	txHistory := make([][]*mining.TxDesc, 0)
	blockHistory := make([]*btcutil.Block, 0)
	estimateHistory := [][estimateFeeBins]float64{eft.estimates(ef)}
	
	// Make some initial rounds so that we have room to step back.
	for round := 0; round < stepsBack - 1; round++ {
		txHistory, blockHistory, estimateHistory = 
			eft.round(ef, txHistory, blockHistory, estimateHistory, 
				txPerRound, txPerBlock, uint32(stepsBack))
	}
	
	for round := 0; round < rounds; round++ {
		txHistory, blockHistory, estimateHistory = 
			eft.round(ef, txHistory, blockHistory, estimateHistory, 
				txPerRound, txPerBlock, uint32(stepsBack))
			
		for step := 0; step < stepsBack; step++ {
			err := ef.Rollback() 
			if err != nil {
				t.Fatal("Could not rollback: ", err)
			}
			
			expected := estimateHistory[len(estimateHistory) - step - 2]	
			estimates := eft.estimates(ef)
			
			// Ensure that these are both the same. 
			for i := 0; i < estimateFeeBins; i++ {
				if expected[i] != estimates[i] {
					t.Error("Rollback value mismatch.")
				}
			}
		}
		
		// Remove last estries from estimateHistory
		estimateHistory = estimateHistory[0: len(estimateHistory) - stepsBack]
		
		// replay the previous blocks.
		for b := 0; b < stepsBack; b++ {
			ef.RecordBlock(blockHistory[b])
			estimateHistory = append(estimateHistory, eft.estimates(ef))
		}
	}
}