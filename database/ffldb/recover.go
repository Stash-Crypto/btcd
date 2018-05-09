// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package ffldb

import (
	"bytes"
	"os"
	"path/filepath"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/goleveldb/leveldb"
	"github.com/btcsuite/goleveldb/leveldb/filter"
	"github.com/btcsuite/goleveldb/leveldb/opt"
)

var zeroHash chainhash.Hash

type scanner struct {
	s       *blockStore
	fileNum uint32
	fileOff uint32
	fileLen uint32
}

func (s scanner) getNextLocation() blockLocation {
	return blockLocation{blockFileNum: s.fileNum, fileOffset: s.fileOff, blockLen: 0}
}

func (s scanner) getNextBlock() (scanner, *btcutil.Block, blockLocation, error) {
	if s.s == nil {
		return scanner{}, nil, blockLocation{}, nil
	}

	next := s
	old := next.getNextLocation()

	// if the length of the file is zero, we have to figure out what
	// the length is so that we know when to move on to the next file.
	if next.fileLen == 0 {
		filePath := blockFilePath(s.s.basePath, s.fileNum)
		st, err := os.Stat(filePath)

		// if the file does not exist, that means we have just
		// reached the end of the list.
		if err != nil {
			return scanner{}, nil, blockLocation{}, nil
		}

		next.fileLen = uint32(st.Size())
	}

	block, err := s.s.readBlock(&zeroHash, old)
	if err != nil {
		return scanner{}, nil, blockLocation{}, err
	}

	var msgBlock wire.MsgBlock
	msgBlock.Deserialize(bytes.NewBuffer(block))

	// 12 is added to the offest to account for the extra metadata stored in the
	// block database.
	old.blockLen = uint32(len(block)) + 12
	next.fileOff += old.blockLen

	if next.fileOff == next.fileLen {
		next.fileLen = 0
		next.fileOff = 0
		next.fileNum++
	}

	return next, btcutil.NewBlock(&msgBlock), old, nil
}

// isDbBucketNotFoundErr returns whether or not the passed error is a
// database.Error with an error code of database.ErrBucketNotFound.
func isDbErrCorruption(err error) bool {
	dbErr, ok := err.(database.Error)
	return ok && dbErr.ErrorCode == database.ErrCorruption
}

// recoverDB takes a leveldb database that doesn't know about any of the blocks
// stored in the flat files and goes through all the flat files
func recoverDB(chain *blockchain.BlockChain, db *db, p *chaincfg.Params) (blocksRead uint32, err error) {

	sc := scanner{s: db.store}
	var scn scanner
	var blk *btcutil.Block

	// skip genesis block.
	sc, _, _, err = sc.getNextBlock()
	if err != nil {
		return 0, err
	}

	for {
		blocksRead++
		scn, blk, _, err = sc.getNextBlock()
		if err != nil {
			// If the database past a certain point is corrupted, return nil
			// and allow the program to truncate the block files as usual at this
			// point.
			if isDbErrCorruption(err) {
				err = nil
				break
			}
			return
		}
		if blk == nil {
			break
		}

		_, _, err = chain.ProcessBlock(blk, blockchain.BFFastAdd|blockchain.BFNoPoWCheck)
		if err != nil {
			return
		}

		sc = scn
	}

	return
}

func RecoverDB(dbPath, oldDbPath string, p *chaincfg.Params) (uint32, error) {
	// Error if the database exists.
	metadataDbPath := filepath.Join(oldDbPath, metadataDbName)

	// Open the metadata database (will create it if needed).
	opts := opt.Options{
		ErrorIfExist: false,
		Strict:       opt.DefaultStrict,
		Compression:  opt.NoCompression,
		Filter:       filter.NewBloomFilter(10),
	}
	ldb, err := leveldb.OpenFile(metadataDbPath, &opts)
	if err != nil {
		return 0, convertErr(err.Error(), err)
	}

	store := newBlockStore(oldDbPath, p.Net)
	cache := newDbCache(ldb, store, defaultCacheSize, defaultFlushSecs)
	pdb := &db{store: store, cache: cache}
	defer func() {
		pdb.Close()
	}()

	rdb, err := openDB(dbPath, p.Net, true)
	if err != nil {
		return 0, err
	}
	defer func() {
		rdb.Close()
	}()

	// Create blockchain
	chain, err := blockchain.New(&blockchain.Config{
		DB:           rdb,
		ChainParams:  p,
		Checkpoints:  p.Checkpoints,
		TimeSource:   blockchain.NewMedianTime(),
		IndexManager: nil, // Fill this in later.
		SigCache:     txscript.NewSigCache(100000),
		HashCache:    txscript.NewHashCache(100000),
	})
	if err != nil {
		return 0, err
	}

	return recoverDB(chain, pdb, p)
}
