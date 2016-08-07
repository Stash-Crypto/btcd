// Copyright (c) 2013-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"sync"
	"sort"
	"math/rand"
	"errors"
	"fmt"
	
	"github.com/btcsuite/btcd/mining"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
)

const (
	// estimateFeeDepth is the maximum number of blocks before a transaction
	// is confirmed that we want to track. 
	estimateFeeMaxWait = 25
	
	// estimateFeeBinSize is the number of txs stored in each bin. 
	estimateFeeBinSize = 100
	
	// estimateFeeMaxReplacements is the max number of replacements that 
	// can be made by the txs found in a given block. 
	estimateFeeMaxReplacements = 10
)

// observedTransaction represents an observed transaction and some
// additional data required for the fee estimation algorithm. 
type observedTransaction struct {
	hash chainhash.Hash // a transaction hash. 
	size uint32 // size in bytes. 
	fee  uint64 // miner's fee. 
	feePerKb float64
	observed int32 // The block height when it was observed. 
	mined int32 // The block in which this tx was mined. 
}

// estimateFeeSet is a slice that can be sorted by the fee per kb rate. 
type estimateFeeSet []*observedTransaction

func (b estimateFeeSet) Len() int { return len(b) }

func (b estimateFeeSet) Less(i, j int) bool {
	return b[i].feePerKb > b[j].feePerKb
}

func (b estimateFeeSet) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

// tt is safe for concurrent access. 
type feeEstimator struct {
	
	maxRollback     uint32 
	binSize         int
	maxReplacements int
	
	// The last known height.
	height int32
	
	sync.RWMutex
	observed map[chainhash.Hash]observedTransaction
	bin [estimateFeeMaxWait][]*observedTransaction
	
	// The sorted set of txs we use to estimate the fee.
	sorted estimateFeeSet
	
	// Transactions that have been removed from the bins. This allows us to
	// revert in case of an orphaned block.
	dropped [][]*observedTransaction
}

func NewFeeEstimator(maxRollback uint32) *feeEstimator {
	return &feeEstimator{
		maxRollback     : maxRollback,
		height          : mempoolHeight, 
		binSize         : estimateFeeBinSize, 
		maxReplacements : estimateFeeMaxReplacements, 
		observed        : make(map[chainhash.Hash]observedTransaction), 
		dropped         : make([][]*observedTransaction, 0, maxRollback), 
	}
}

// ObserveTransaction is called when a new transaction is observed in the mempool. 
func (ef *feeEstimator) ObserveTransaction(t *mining.TxDesc) {
	ef.Lock()
	defer ef.Unlock()
	
	hash := *t.Tx.Hash()
	if _, ok := ef.observed[hash]; !ok {
		size := uint32(t.Tx.MsgTx().SerializeSize())
		fee := uint64(t.Fee)
		
		ef.observed[hash] = observedTransaction{
			hash : hash, 
			size : size, 
			fee : fee, 
			feePerKb : float64(1000 * fee)/float64(size), 
			observed : t.Height, 
			mined : mempoolHeight, 
		}
	}
}

// RegisterBlock informs the fee estimator of a new block to take into account.
func (ef *feeEstimator) RecordBlock(block *btcutil.Block) {
	ef.Lock()
	defer ef.Unlock()
	
	// The previous sorted list is invalid, so delete it. 
	ef.sorted = nil
	
	height := block.Height()
	if height != ef.height + 1 && ef.height != mempoolHeight {
		panic(fmt.Sprint("intermediate block not recorded; current height is ", ef.height,
			"; new height is ", height))
	}
	
	ef.height = height
	
	// Randomly order txs in block. 
	transactions := make(map[*btcutil.Tx]struct{})
	for _, t := range block.Transactions() {
		transactions[t] = struct{}{}
	}
	
	// Count the number of replacements we make per bin so that we don't 
	// replace too many. 
	var replacementCounts [estimateFeeMaxWait]int
	
	// Keep track of which txs were dropped in case of an orphan block. 
	dropped := make([]*observedTransaction, 0, 100)
	
	// Go through the txs in the block. 
	for t, _ := range transactions {
		hash := *t.Hash()
		
		// Have we observed this tx in the mempool? 
		o, ok := ef.observed[hash]
		if !ok {
			continue
		}
		
		// Put the observed tx in the oppropriate bin. 
		o.mined = height
		
		blocksToConfirm := height - o.observed - 1
		
		// Make sure we do not replace too many transactions per min. 
		if replacementCounts[blocksToConfirm] == ef.maxReplacements {
			continue
		}
		
		replacementCounts[blocksToConfirm]++
		
		bin := ef.bin[blocksToConfirm]
		
		// Remove a random element and replace it with this new tx.
		if len(bin) == int(ef.binSize) {
			l := int(ef.binSize - replacementCounts[blocksToConfirm])
			drop := rand.Intn(l)
			dropped = append(dropped, bin[drop])
			
			bin[drop] = bin[l - 1]
			bin[l - 1] = &o
		} else {
			ef.bin[blocksToConfirm] = append(bin, &o)
		}
	}
	
	// Go through the mempool for txs that have been in too long. 
	for hash, o := range ef.observed {
		if height - o.observed >= estimateFeeMaxWait {
			delete(ef.observed, hash)
		}
	}
	
	// Add dropped list to history. 
	if ef.maxRollback == 0 {
		return
	}
	
	if uint32(len(ef.dropped)) == ef.maxRollback {
		ef.dropped = append(ef.dropped[1:], dropped)
	} else {
		ef.dropped = append(ef.dropped, dropped)
	}
}

// Rollback reverses the effect of the last block on the fee estimator. This
// can be used in the case of an orphaned block. The maximum number of rollbacks
// allowed is given by maxRollbacks. 
func (ef *feeEstimator) Rollback() error {
	ef.Lock()
	defer ef.Unlock()
	
	// The previous sorted list is invalid, so delete it. 
	ef.sorted = nil
	
	// pop the last list of dropped txs from the stack. 
	last := len(ef.dropped) - 1
	if last == -1 {
		// Return if we cannot rollback. 
		return errors.New("Max rollbacks reached.")
	}
	
	dropped := ef.dropped[last]
	ef.dropped = ef.dropped[0:last]
	
	// where we are in each bin as we replace txs. 
	var replacementCounters [estimateFeeMaxWait]int
	
	// Go through the txs in the dropped box. 
	for _, o := range dropped {
		// Which bin was this tx in? 
		blocksToConfirm := o.mined - o.observed - 1
		
		bin := ef.bin[blocksToConfirm] 
		
		var counter = replacementCounters[blocksToConfirm]
		
		// Continue to go through that bin where we left off. 
		for {
			if counter >= len(bin) {
				panic("Illegal state: cannot rollback dropped transaction!")
			}
			
			prev := bin[counter] 
			
			if prev.mined == ef.height {
				prev.mined = mempoolHeight
				
				bin[counter] = o
				
				counter ++
				break;
			}
			
			counter ++
		}
	}
	
	// Continue going through bins to find other txs to remove
	// which did not replace any other when they were entered.
	for i, j := range replacementCounters {
		for {
			l := len(ef.bin[i])
			if j >= l {
				break
			}
			
			prev := ef.bin[i][j] 
			
			if prev.mined == ef.height {
				prev.mined = mempoolHeight
				
				ef.bin[i] = append(ef.bin[i][0:j], ef.bin[i][j + 1 : l]...)
				
				continue
			}
			
			j ++
		}
	}
	
	ef.height --
	
	return nil
} 

// sortBinnedTransactions takes all transactions in all bins and returns a 
// sorted list of them all. 
func (ef *feeEstimator) sortBinnedTransactions() estimateFeeSet {
	capacity := 0
	for _, b := range ef.bin {
		capacity += len(b)
	}
	
	sorted := estimateFeeSet(make([]*observedTransaction, capacity))
	
	i := 0
	for _, b := range ef.bin {
		for _, o := range b {
			sorted[i] = o
			i++
		}
	}
	
	sort.Sort(sorted)
	return sorted
}

// Estimate the fee per kb to have a tx confirmed a given number of blocks 
// from now. 
func (ef *feeEstimator) EstimateFee(confirmations uint32) float64 {
	ef.Lock()
	defer ef.Unlock()
	
	if confirmations == 0 || confirmations > estimateFeeMaxWait {
		return 0
	}
	
	var min, max uint32 = 0, 0
	for i := uint32(0); i < confirmations - 1; i++ {
		min += uint32(len(ef.bin[i]))
	}
	
	max = min + uint32(len(ef.bin[confirmations - 1]))
	
	// We don't have any transactions! 
	if min == 0 && max == 0 {
		return 0
	}
	
	// Generate sorted list if it doesn't exist. 
	if ef.sorted == nil {
		ef.sorted = ef.sortBinnedTransactions() 
	}
	
	return ef.sorted[(min + max - 1) / 2].feePerKb
}