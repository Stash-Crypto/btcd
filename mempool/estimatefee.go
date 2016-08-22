// Copyright (c) 2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mempool

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/mining"
	"github.com/btcsuite/btcutil"
)

// TODO incorporate Alex Morcos' modifications to Gavin's initial model
// https://lists.linuxfoundation.org/pipermail/bitcoin-dev/2014-October/006824.html

// TODO use sat/byte rather than btc/kilobyte.

// TODO store and restore the FeeEstimator state in the database.

const (
	// estimateFeeDepth is the maximum number of blocks before a transaction
	// is confirmed that we want to track.
	estimateFeeDepth = 25

	// estimateFeeBinSize is the number of txs stored in each bin.
	estimateFeeBinSize = 100

	// estimateFeeMaxReplacements is the max number of replacements that
	// can be made by the txs found in a given block.
	estimateFeeMaxReplacements = 10
)

// observedTransaction represents an observed transaction and some
// additional data required for the fee estimation algorithm.
type observedTransaction struct {
	// A transaction hash.
	hash chainhash.Hash

	// The size of the transaction in bytes.
	size uint32

	// The miner's fee.
	fee btcutil.Amount

	// The fee per kb of the transaction in satoshis.
	feePerKb float64

	// The block height when it was observed.
	observed int32

	// The height of the block in which it was mined.
	mined int32
}

// registeredBlock has the hash of a block and the list of transactions
// it mined which had been previously observed by the FeeEstimator. It
// is used if Rollback is called to reverse the effect of registering
// a block.
type registeredBlock struct {
	hash         *chainhash.Hash
	transactions []*observedTransaction
}

// FeeEstimator manages the data necessary to create
// fee estimations. It is safe for concurrent access.
type FeeEstimator struct {
	maxRollback uint32
	binSize     int

	// The maximum number of replacements that can be made in a single
	// bin per block. Default is estimateFeeMaxReplacements
	maxReplacements int

	// The minimum number of blocks that can be registered with the fee
	// estimator before it will provide answers.
	minRegisteredBlocks uint32

	// The last known height.
	lastKnownHeight int32

	sync.RWMutex
	observed            map[chainhash.Hash]observedTransaction
	bin                 [estimateFeeDepth][]*observedTransaction
	numBlocksRegistered uint32 // The number of blocks that have been registered.

	// The cached estimates.
	cached []float64

	// Transactions that have been removed from the bins. This allows us to
	// revert in case of an orphaned block.
	dropped []registeredBlock
}

// NewFeeEstimator creates a FeeEstimator for which at most maxRollback blocks
// can be unregistered and which returns an error unless minRegisteredBlocks
// have been registered with it.
func NewFeeEstimator(maxRollback, minRegisteredBlocks uint32) *FeeEstimator {
	return &FeeEstimator{
		maxRollback:         maxRollback,
		minRegisteredBlocks: minRegisteredBlocks,
		lastKnownHeight:     mining.UnminedHeight,
		binSize:             estimateFeeBinSize,
		maxReplacements:     estimateFeeMaxReplacements,
		observed:            make(map[chainhash.Hash]observedTransaction),
		dropped:             make([]registeredBlock, 0, maxRollback),
	}
}

// ObserveTransaction is called when a new transaction is observed in the mempool.
func (ef *FeeEstimator) ObserveTransaction(t *TxDesc) {
	ef.Lock()
	defer ef.Unlock()

	hash := *t.Tx.Hash()
	if _, ok := ef.observed[hash]; !ok {
		size := uint32(t.Tx.MsgTx().SerializeSize())

		ef.observed[hash] = observedTransaction{
			hash:     hash,
			size:     size,
			fee:      btcutil.Amount(t.Fee),
			feePerKb: float64(1024*t.Fee) / float64(size),
			observed: t.Height,
			mined:    mining.UnminedHeight,
		}
	}
}

// RegisterBlock informs the fee estimator of a new block to take into account.
func (ef *FeeEstimator) RegisterBlock(block *btcutil.Block) error {
	ef.Lock()
	defer ef.Unlock()

	// The previous sorted list is invalid, so delete it.
	ef.cached = nil

	height := block.Height()
	if height != ef.lastKnownHeight+1 && ef.lastKnownHeight != mining.UnminedHeight {
		return fmt.Errorf("intermediate block not recorded; current height is %d; new height is %d",
			ef.lastKnownHeight, height)
	}

	// Update the last known height.
	ef.lastKnownHeight = height
	ef.numBlocksRegistered++

	// Randomly order txs in block.
	transactions := make(map[*btcutil.Tx]struct{})
	for _, t := range block.Transactions() {
		transactions[t] = struct{}{}
	}

	// Count the number of replacements we make per bin so that we don't
	// replace too many.
	var replacementCounts [estimateFeeDepth]int

	// Keep track of which txs were dropped in case of an orphan block.
	dropped := registeredBlock{
		hash:         block.Hash(),
		transactions: make([]*observedTransaction, 0, 100),
	}

	// Go through the txs in the block.
	for t := range transactions {
		hash := *t.Hash()

		// Have we observed this tx in the mempool?
		o, ok := ef.observed[hash]
		if !ok {
			continue
		}

		// Put the observed tx in the oppropriate bin.
		o.mined = height

		blocksToConfirm := height - o.observed - 1

		// This shouldn't happen but check just in case to avoid
		// a panic later.
		if blocksToConfirm >= estimateFeeDepth {
			continue
		}

		// Make sure we do not replace too many transactions per min.
		if replacementCounts[blocksToConfirm] == ef.maxReplacements {
			continue
		}

		replacementCounts[blocksToConfirm]++

		bin := ef.bin[blocksToConfirm]

		// Remove a random element and replace it with this new tx.
		if len(bin) == ef.binSize {
			l := ef.binSize - replacementCounts[blocksToConfirm]
			drop := rand.Intn(l)
			dropped.transactions = append(dropped.transactions, bin[drop])

			bin[drop] = bin[l-1]
			bin[l-1] = &o
		} else {
			ef.bin[blocksToConfirm] = append(bin, &o)
		}
	}

	// Go through the mempool for txs that have been in too long.
	for hash, o := range ef.observed {
		if height-o.observed >= estimateFeeDepth {
			delete(ef.observed, hash)
		}
	}

	// Add dropped list to history.
	if ef.maxRollback == 0 {
		return nil
	}

	if uint32(len(ef.dropped)) == ef.maxRollback {
		ef.dropped = append(ef.dropped[1:], dropped)
	} else {
		ef.dropped = append(ef.dropped, dropped)
	}

	return nil
}

// Rollback unregisters a recently registered block from the FeeEstimator.
// This can be used to reverse the effect of an orphaned block on the fee
// estimator. The maximum number of rollbacks allowed is given by
// maxRollbacks.
//
// Note: not everything can be rolled back because some transactions are
// deleted if they have been observed too long ago. That means the result
// of Rollback won't always be exactly the same as if the last block had not
// happened, but it should be close enough.
func (ef *FeeEstimator) Rollback(block *btcutil.Block) error {
	ef.Lock()
	defer ef.Unlock()

	hash := block.Hash()

	// Find this block in the stack of recent registered blocks.
	var n int
	for n = 1; n < len(ef.dropped); n++ {
		if ef.dropped[len(ef.dropped)-n].hash.IsEqual(hash) {
			break
		}
	}

	if n == len(ef.dropped) {
		return errors.New("No such block was recently registered.")
	}

	for i := 0; i < n; i++ {
		err := ef.rollback()
		if err != nil {
			return err
		}
	}

	return nil
}

// rollback rolls back the effect of the last block in the stack
// of registered blocks.
func (ef *FeeEstimator) rollback() error {

	// The previous sorted list is invalid, so delete it.
	ef.cached = nil

	// pop the last list of dropped txs from the stack.
	last := len(ef.dropped) - 1
	if last == -1 {
		// Return if we cannot rollback.
		return errors.New("Max rollbacks reached.")
	}

	ef.numBlocksRegistered--

	dropped := ef.dropped[last]
	ef.dropped = ef.dropped[0:last]

	// where we are in each bin as we replace txs?
	var replacementCounters [estimateFeeDepth]int

	var err error

	// Go through the txs in the dropped block.
	for _, o := range dropped.transactions {
		// Which bin was this tx in?
		blocksToConfirm := o.mined - o.observed - 1

		bin := ef.bin[blocksToConfirm]

		var counter = replacementCounters[blocksToConfirm]

		// Continue to go through that bin where we left off.
		for {
			if counter >= len(bin) {
				// Create an error but keep going in case we can roll back
				// more transactions successfully.
				err = errors.New("Illegal state: cannot rollback dropped transaction!")
			}

			prev := bin[counter]

			if prev.mined == ef.lastKnownHeight {
				prev.mined = mining.UnminedHeight

				bin[counter] = o

				counter++
				break
			}

			counter++
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

			if prev.mined == ef.lastKnownHeight {
				prev.mined = mining.UnminedHeight

				newBin := append(ef.bin[i][0:j], ef.bin[i][j+1:l]...)
				// TODO This line should prevent an unintentional memory
				// leak but it causes a panic when it is uncommented.
				// ef.bin[i][j] = nil
				ef.bin[i] = newBin

				continue
			}

			j++
		}
	}

	ef.lastKnownHeight--

	return err
}

// estimateFeeSet is a set of txs that can that is sorted
// by the fee per kb rate.
type estimateFeeSet struct {
	feeRate []float64
	bin     [estimateFeeDepth]uint32
}

func (b *estimateFeeSet) Len() int { return len(b.feeRate) }

func (b *estimateFeeSet) Less(i, j int) bool {
	return b.feeRate[i] > b.feeRate[j]
}

func (b *estimateFeeSet) Swap(i, j int) {
	b.feeRate[i], b.feeRate[j] = b.feeRate[j], b.feeRate[i]
}

// estimateFee returns the estimated fee for a transaction
// to confirm in confirmations blocks from now, given
// the data set we have collected.
func (b *estimateFeeSet) estimateFee(confirmations int) float64 {
	if confirmations <= 0 {
		return math.Inf(1)
	}

	if confirmations > estimateFeeDepth {
		return 0
	}

	var min, max uint32 = 0, 0
	for i := 0; i < confirmations-1; i++ {
		min += b.bin[i]
	}

	max = min + b.bin[confirmations-1]

	// We don't have any transactions!
	if min == 0 && max == 0 {
		return 0
	}

	return b.feeRate[(min+max-1)/2] * 1E-8
}

// newEstimateFeeSet creates a temporary data structure that
// can be used to find all fee estimates.
func (ef *FeeEstimator) newEstimateFeeSet() *estimateFeeSet {
	set := &estimateFeeSet{}

	capacity := 0
	for i, b := range ef.bin {
		l := len(b)
		set.bin[i] = uint32(l)
		capacity += l
	}

	set.feeRate = make([]float64, capacity)

	i := 0
	for _, b := range ef.bin {
		for _, o := range b {
			set.feeRate[i] = o.feePerKb
			i++
		}
	}

	sort.Sort(set)

	return set
}

// estimates returns the set of all fee estimates from 1 to estimateFeeDepth
// confirmations from now.
func (ef *FeeEstimator) estimates() []float64 {
	set := ef.newEstimateFeeSet()

	estimates := make([]float64, estimateFeeDepth)
	for i := 0; i < estimateFeeDepth; i++ {
		estimates[i] = set.estimateFee(i + 1)
	}

	return estimates
}

// EstimateFee estimates the fee per kb to have a tx confirmed a given
// number of blocks from now.
func (ef *FeeEstimator) EstimateFee(numBlocks uint32) (float64, error) {
	ef.Lock()
	defer ef.Unlock()

	// If the number of registered blocks is below the minimum, return
	// an error.
	if ef.numBlocksRegistered < ef.minRegisteredBlocks {
		return -1, errors.New("Not enough blocks have been observed.")
	}

	if numBlocks == 0 {
		return -1, errors.New("Cannot confirm transaction in zero blocks.")
	}

	if numBlocks > estimateFeeDepth {
		return -1, fmt.Errorf(
			"Can only estimate fees for up to %d blocks from now.",
			estimateFeeBinSize)
	}

	// If there are no cached results, generate them.
	if ef.cached == nil {
		ef.cached = ef.estimates()
	}

	return ef.cached[int(numBlocks)-1], nil
}
