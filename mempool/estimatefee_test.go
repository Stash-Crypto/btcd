// Copyright (c) 2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mempool

import (
	"math/rand"
	"testing"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/mining"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
)

// newTestFeeEstimator creates a feeEstimator with some different parameters
// for testing purposes.
func newTestFeeEstimator(binSize, maxReplacements, maxRollback uint32) *FeeEstimator {
	return &FeeEstimator{
		maxRollback:         maxRollback,
		lastKnownHeight:     0,
		binSize:             int32(binSize),
		minRegisteredBlocks: 0,
		maxReplacements:     int32(maxReplacements),
		observed:            make(map[chainhash.Hash]*observedTransaction),
		dropped:             make([]*registeredBlock, 0, maxRollback),
	}
}

type estimateFeeTester struct {
	ef      *FeeEstimator
	t       *testing.T
	version int32
	height  int32
}

func (eft *estimateFeeTester) testTx(fee btcutil.Amount) *TxDesc {
	eft.version++
	return &TxDesc{
		TxDesc: mining.TxDesc{
			Tx: btcutil.NewTx(&wire.MsgTx{
				Version: eft.version,
			}),
			Height: eft.height,
			Fee:    int64(fee),
		},
		StartingPriority: 0,
	}
}

func expectedFeePerByte(t *TxDesc) SatoshiPerByte {
	size := float64(t.TxDesc.Tx.MsgTx().SerializeSize())
	fee := float64(t.TxDesc.Fee)

	return SatoshiPerByte(fee / size * 1E-8)
}

func (eft *estimateFeeTester) testBlock(txs []*wire.MsgTx) *btcutil.Block {

	eft.height++
	block := btcutil.NewBlock(&wire.MsgBlock{
		Transactions: txs,
	})
	block.SetHeight(eft.height)

	return block
}

func (eft *estimateFeeTester) rollback() {

	eft.height--
	eft.ef.rollback()
}

func TestEstimateFee(t *testing.T) {
	ef := newTestFeeEstimator(5, 3, 1)
	eft := estimateFeeTester{ef: ef, t: t}

	// Try with no txs and get zero for all queries.
	expected := SatoshiPerByte(0.0)
	for i := uint32(1); i <= estimateFeeDepth; i++ {
		estimated, _ := ef.EstimateFee(i)

		if estimated != expected {
			t.Errorf("Estimate fee error: expected %f when estimator is empty; got %f", expected, estimated)
		}
	}

	// Now insert a tx.
	tx := eft.testTx(1000000)
	ef.ObserveTransaction(tx)

	// Expected should still be zero because this is still in the mempool.
	expected = SatoshiPerByte(0.0)
	for i := uint32(1); i <= estimateFeeDepth; i++ {
		estimated, _ := ef.EstimateFee(i)

		if estimated != expected {
			t.Errorf("Estimate fee error: expected %f when estimator has one tx in mempool; got %f", expected, estimated)
		}
	}

	// Change minRegisteredBlocks to make sure that works. Error return
	// value expected.
	ef.minRegisteredBlocks = 1
	expected = SatoshiPerByte(-1.0)
	for i := uint32(1); i <= estimateFeeDepth; i++ {
		estimated, _ := ef.EstimateFee(i)

		if estimated != expected {
			t.Errorf("Estimate fee error: expected %f before any blocks have been registered; got %f", expected, estimated)
		}
	}

	// Record a block with the new tx.
	ef.RegisterBlock(eft.testBlock([]*wire.MsgTx{tx.Tx.MsgTx()}))
	expected = expectedFeePerByte(tx)
	for i := uint32(1); i <= estimateFeeDepth; i++ {
		estimated, _ := ef.EstimateFee(i)

		if estimated != expected {
			t.Errorf("Estimate fee error: expected %f when one tx is binned; got %f", expected, estimated)
		}
	}

	// Roll back the last block; this was an orphan block.
	ef.minRegisteredBlocks = 0
	eft.rollback()
	expected = SatoshiPerByte(0.0)
	for i := uint32(1); i <= estimateFeeDepth; i++ {
		estimated, _ := ef.EstimateFee(i)

		if estimated != expected {
			t.Errorf("Estimate fee error: expected %f after rolling back block; got %f", expected, estimated)
		}
	}

	// Record an empty block and then a block with the new tx.
	// This test was made because of a bug that only appeared when there
	// were no transactions in the first bin.
	ef.RegisterBlock(eft.testBlock([]*wire.MsgTx{}))
	ef.RegisterBlock(eft.testBlock([]*wire.MsgTx{tx.Tx.MsgTx()}))
	expected = expectedFeePerByte(tx)
	for i := uint32(1); i <= estimateFeeDepth; i++ {
		estimated, _ := ef.EstimateFee(i)

		if estimated != expected {
			t.Errorf("Estimate fee error: expected %f when one tx is binned; got %f", expected, estimated)
		}
	}

	// Create some more transactions.
	txA := eft.testTx(500000)
	txB := eft.testTx(2000000)
	txC := eft.testTx(4000000)
	ef.ObserveTransaction(txA)
	ef.ObserveTransaction(txB)
	ef.ObserveTransaction(txC)

	// Record 7 empty blocks.
	for i := 0; i < 7; i++ {
		ef.RegisterBlock(eft.testBlock([]*wire.MsgTx{}))
	}

	// Mine the first tx.
	ef.RegisterBlock(eft.testBlock([]*wire.MsgTx{txA.Tx.MsgTx()}))

	// Now the estimated amount should depend on the value
	// of the argument to estimate fee.
	for i := uint32(1); i <= estimateFeeDepth; i++ {
		estimated, _ := ef.EstimateFee(i)
		if i > 2 {
			expected = expectedFeePerByte(txA)
		} else {
			expected = expectedFeePerByte(tx)
		}
		if estimated != expected {
			t.Errorf("Estimate fee error: expected %f on round %d; got %f", expected, i, estimated)
		}
	}

	// Record 5 more empty blocks.
	for i := 0; i < 5; i++ {
		ef.RegisterBlock(eft.testBlock([]*wire.MsgTx{}))
	}

	// Mine the next tx.
	ef.RegisterBlock(eft.testBlock([]*wire.MsgTx{txB.Tx.MsgTx()}))

	// Now the estimated amount should depend on the value
	// of the argument to estimate fee.
	for i := uint32(1); i <= estimateFeeDepth; i++ {
		estimated, _ := ef.EstimateFee(i)
		if i <= 2 {
			expected = expectedFeePerByte(txB)
		} else if i <= 8 {
			expected = expectedFeePerByte(tx)
		} else {
			expected = expectedFeePerByte(txA)
		}

		if estimated != expected {
			t.Errorf("Estimate fee error: expected %f on round %d; got %f", expected, i, estimated)
		}
	}

	// Record 9 more empty blocks.
	for i := 0; i < 10; i++ {
		ef.RegisterBlock(eft.testBlock([]*wire.MsgTx{}))
	}

	// Mine txC.
	ef.RegisterBlock(eft.testBlock([]*wire.MsgTx{txC.Tx.MsgTx()}))

	// This should have no effect on the outcome because too
	// many blocks have been mined for txC to be recorded.
	for i := uint32(1); i <= estimateFeeDepth; i++ {
		estimated, _ := ef.EstimateFee(i)
		if i <= 2 {
			expected = expectedFeePerByte(txC)
		} else if i <= 8 {
			expected = expectedFeePerByte(txB)
		} else if i <= 8+6 {
			expected = expectedFeePerByte(tx)
		} else {
			expected = expectedFeePerByte(txA)
		}

		if estimated != expected {
			t.Errorf("Estimate fee error: expected %f on round %d; got %f", expected, i, estimated)
		}
	}
}

func (eft *estimateFeeTester) estimates() [estimateFeeDepth]SatoshiPerByte {

	// Generate estimates
	var estimates [estimateFeeDepth]SatoshiPerByte
	for i := 0; i < estimateFeeDepth; i++ {
		estimates[i], _ = eft.ef.EstimateFee(uint32(i + 1))
	}

	// Check that all estimated fee results go in descending order.
	for i := 1; i < estimateFeeDepth; i++ {
		if estimates[i] > estimates[i-1] {
			eft.t.Error("Estimates not in descending order; got ",
				estimates[i], " for estimate ", i, " and ", estimates[i-1], " for ", (i - 1))
			panic("invalid state.")
		}
	}

	return estimates
}

func (eft *estimateFeeTester) round(txHistory [][]*TxDesc,
	estimateHistory [][estimateFeeDepth]SatoshiPerByte,
	txPerRound, txPerBlock uint32) ([][]*TxDesc, [][estimateFeeDepth]SatoshiPerByte) {

	// generate new txs.
	var newTxs []*TxDesc
	for i := uint32(0); i < txPerRound; i++ {
		newTx := eft.testTx(btcutil.Amount(rand.Intn(1000000)))
		eft.ef.ObserveTransaction(newTx)
		newTxs = append(newTxs, newTx)
	}

	// Generate mempool.
	mempool := make(map[*observedTransaction]*TxDesc)
	for _, h := range txHistory {
		for _, t := range h {
			if o, exists := eft.ef.observed[*t.Tx.Hash()]; exists && o.mined == mining.UnminedHeight {
				mempool[o] = t
			}
		}
	}

	// generate new block, with no duplicates.
	i := uint32(0)
	newBlockList := make([]*wire.MsgTx, 0, txPerBlock)
	for _, t := range mempool {
		newBlockList = append(newBlockList, t.TxDesc.Tx.MsgTx())
		i++

		if i == txPerBlock {
			break
		}
	}

	newBlock := eft.testBlock(newBlockList)
	eft.ef.RegisterBlock(newBlock)

	// return results.
	estimates := eft.estimates()

	// Return results
	return append(txHistory, newTxs), append(estimateHistory, estimates)
}

func TestEstimateFeeRollback(t *testing.T) {
	txPerRound := uint32(7)
	txPerBlock := uint32(5)
	binSize := uint32(6)
	maxReplacements := uint32(4)
	stepsBack := 2
	rounds := 8

	eft := estimateFeeTester{ef: newTestFeeEstimator(binSize, maxReplacements, uint32(stepsBack)), t: t}
	var txHistory [][]*TxDesc
	estimateHistory := [][estimateFeeDepth]SatoshiPerByte{eft.estimates()}

	for round := 0; round < rounds; round++ {
		// Go forward a few rounds.
		for step := 0; step <= stepsBack; step++ {
			txHistory, estimateHistory =
				eft.round(txHistory, estimateHistory, txPerRound, txPerBlock)
		}

		// Now go back.
		for step := 0; step < stepsBack; step++ {
			eft.rollback()

			// After rolling back, we should have the same estimated
			// fees as before.
			expected := estimateHistory[len(estimateHistory)-step-2]
			estimates := eft.estimates()

			// Ensure that these are both the same.
			for i := 0; i < estimateFeeDepth; i++ {
				if expected[i] != estimates[i] {
					t.Errorf("Rollback value mismatch. Expected %f, got %f. ",
						expected[i], estimates[i])
					return
				}
			}
		}

		// Erase history.
		txHistory = txHistory[0 : len(txHistory)-stepsBack]
		estimateHistory = estimateHistory[0 : len(estimateHistory)-stepsBack]
	}
}

func (eft *estimateFeeTester) checkSaveAndRestore(
	previousEstimates [estimateFeeDepth]SatoshiPerByte) {

	// Save and restore database.
	var err error
	eft.ef, err = RestoreFeeEstimator(eft.ef.Save())
	if err != nil {
		eft.t.Fatalf("Could not restore database: %s", err)
	}

	// Check that the results match.
	newEstimates := eft.estimates()

	for i, prev := range previousEstimates {
		if prev != newEstimates[i] {
			eft.t.Error("Mismatch in estimate ", i, " after restore; got ", newEstimates[i], " but expected ", prev)
		}
	}
}

// TestSave tests saving and restoring to a []byte.
func TestDatabase(t *testing.T) {

	txPerRound := uint32(7)
	txPerBlock := uint32(5)
	binSize := uint32(6)
	maxReplacements := uint32(4)
	rounds := 8

	eft := estimateFeeTester{ef: newTestFeeEstimator(binSize, maxReplacements, uint32(rounds)), t: t}
	var txHistory [][]*TxDesc
	estimateHistory := [][estimateFeeDepth]SatoshiPerByte{eft.estimates()}

	for round := 0; round < rounds; round++ {
		eft.checkSaveAndRestore(estimateHistory[len(estimateHistory)-1])

		// Go forward one step.
		txHistory, estimateHistory =
			eft.round(txHistory, estimateHistory, txPerRound, txPerBlock)
	}

	// Reverse the process and try again.
	for round := 1; round <= rounds; round++ {
		eft.rollback()
		eft.checkSaveAndRestore(estimateHistory[len(estimateHistory)-round])
	}
}
