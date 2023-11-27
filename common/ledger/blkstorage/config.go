/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"path/filepath"

	"github.com/hyperledger/fabric/core/ledger"
)

const (
	// ChainsDir is the name of the directory containing the channel ledgers.
	ChainsDir = "chains"
	// IndexDir is the name of the directory containing all block indexes across ledgers.
	IndexDir                = "index"
	defaultMaxBlockfileSize = 64 * 1024 * 1024 // bytes
)

// Conf encapsulates all the configurations for `BlockStore`
type Conf struct {
	blockStorageDir  string
	maxBlockfileSize int
	keyValueDBConfig *ledger.KeyValueDBConfig
}

// NewConf constructs new `Conf`.
// blockStorageDir is the top level folder under which `BlockStore` manages its data
func NewConf(blockStorageDir string, maxBlockfileSize int, keyValueDBConfigs ...*ledger.KeyValueDBConfig) *Conf {
	kvDBConfig := &ledger.KeyValueDBConfig{
		KeyValueDatabase: ledger.GoLevelDB,
	}
	if len(keyValueDBConfigs) > 0 {
		kvDBConfig = keyValueDBConfigs[0]
	}
	if maxBlockfileSize <= 0 {
		maxBlockfileSize = defaultMaxBlockfileSize
	}
	return &Conf{blockStorageDir, maxBlockfileSize, kvDBConfig}
}

func (conf *Conf) getIndexDir() string {
	return filepath.Join(conf.blockStorageDir, IndexDir)
}

func (conf *Conf) getChainsDir() string {
	return filepath.Join(conf.blockStorageDir, ChainsDir)
}

func (conf *Conf) getLedgerBlockDir(ledgerid string) string {
	return filepath.Join(conf.getChainsDir(), ledgerid)
}
