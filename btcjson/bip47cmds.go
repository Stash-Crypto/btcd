// Copyright (c) 2014 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// This file contains commands for bip47, reusable payment codes.
// https://github.com/bitcoin/bips/blob/master/bip-0047.mediawiki

package btcjson

// Bip47NotifyCmd is a command to send a notification transaction
// from Alice to Bob.
type Bip47NotifyCmd struct {
	Account uint32
	Alice   string
	Bob     string
}

// NewBip47NotifyCmd creates a new bip 47 notification command.
func NewBip47NotifyCmd(account uint32, alice, bob string) *Bip47NotifyCmd {
	return &Bip47NotifyCmd{
		Account: account,
		Alice:   alice,
		Bob:     bob,
	}
}

func init() {
	// The commands in this file are only usable with a wallet server.
	flags := UFWalletOnly

	MustRegisterCmd("bip47notify", (*Bip47NotifyCmd)(nil), flags)
}
