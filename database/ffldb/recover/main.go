package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/database/ffldb"
	"github.com/btcsuite/btcd/wire"
)

var recoveryDir = "recovery"
var blocksDir = "blocks_ffldb"

func recoverDatabase(path string, net wire.BitcoinNet) (uint32, error) {
	var subdir string
	var chainParams *chaincfg.Params
	if net == wire.MainNet {
		subdir = "mainnet"
		chainParams = &chaincfg.MainNetParams
	} else if net == wire.TestNet3 {
		subdir = "testnet"
		chainParams = &chaincfg.TestNet3Params
	}

	// Does the given path exist?
	if _, err := os.Stat(path); err != nil {
		return 0, errors.New("Could not read path.")
	}

	dbPath := filepath.Join(path, subdir)
	recoveryPath := filepath.Join(path, recoveryDir)
	recoveryDbPath := filepath.Join(recoveryPath, subdir)

	// Create recovery directory if it does not exist.
	if _, err := os.Stat(recoveryPath); os.IsNotExist(err) {
		if _, err := os.Stat(dbPath); err != nil {
			return 0, err
		}

		if err = os.MkdirAll(recoveryPath, 0700); err != nil {
			return 0, err
		}
	} else if err != nil {
		return 0, err
	}

	// Move database to recovery directory.
	if _, err := os.Stat(recoveryDbPath); os.IsNotExist(err) {
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			return 0, errors.New("Could not find database to recover.")
		} else if err != nil {
			return 0, err
		}

		if err = exec.Command("mv", dbPath, recoveryPath).Run(); err != nil {
			return 0, fmt.Errorf("Could not move folder: %s", err.Error())
		}
	}

	// Delete old database if necessary.
	if _, err := os.Stat(dbPath); err == nil {
		if err = os.RemoveAll(dbPath); err != nil {
			return 0, err
		}
	}

	blks, err := ffldb.RecoverDB(filepath.Join(dbPath, blocksDir),
		filepath.Join(recoveryDbPath, blocksDir), chainParams)
	if err != nil {
		// Delete the directory in which the new database would have been created.
		os.Remove(dbPath)
		return 0, err
	} else {
		os.Remove(recoveryPath)
	}

	return blks, nil
}

func recoverDatabaseFromArgs(args []string) (uint32, error) {
	if len(args) < 1 {
		return 0, errors.New("Must provide database path as only argument.")
	}

	var net wire.BitcoinNet
	if len(args) > 1 {
		switch args[1] {
		case "mainnet":
			net = wire.MainNet
		case "testnet":
			net = wire.TestNet3
		default:
			return 0, errors.New("unrecognized net type")
		}
	} else {
		net = wire.MainNet
	}

	return recoverDatabase(args[0], net)
}

func recoverDatabaseProcedure(args []string) string {
	blks, err := recoverDatabaseFromArgs(args)
	if err != nil {
		return err.Error()
	}

	return fmt.Sprintf("There were %d blocks read.", blks)
}

func main() {
	println(recoverDatabaseProcedure(os.Args[1:]))
}
