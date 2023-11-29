/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvdbhelper

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const testDBPath = "/tmp/fabric/ledgertests/util/kvdbhelper"

type testDBEnv struct {
	t    *testing.T
	path string
	db   *DB
}

type testDBProviderEnv struct {
	t        *testing.T
	path     string
	provider *Provider
}

func newTestDBEnv(t *testing.T, path string) *testDBEnv {
	testDBEnv := &testDBEnv{t: t, path: path}
	testDBEnv.cleanup()
	testDBEnv.db = CreateDB(&Conf{DBPath: path})
	testDBEnv.truncate()
	return testDBEnv
}

func newTestProviderEnv(t *testing.T, path string) *testDBProviderEnv {
	testProviderEnv := &testDBProviderEnv{t: t, path: path}
	testProviderEnv.cleanup()
	var err error
	testProviderEnv.provider, err = NewProvider(&Conf{DBPath: path})
	testProviderEnv.truncate()
	if err != nil {
		panic(err)
	}
	return testProviderEnv
}

func (dbEnv *testDBEnv) cleanup() {
	if dbEnv.db != nil {
		dbEnv.db.Truncate()
		dbEnv.db.Close()
	}
	require.NoError(dbEnv.t, os.RemoveAll(dbEnv.path))
}

func (dbEnv *testDBEnv) truncate() {
	if dbEnv.db != nil {
		dbEnv.db.Truncate()
	}
}

func (providerEnv *testDBProviderEnv) cleanup() {
	if providerEnv.provider != nil {
		providerEnv.provider.Truncate()
		providerEnv.provider.Close()
	}
	require.NoError(providerEnv.t, os.RemoveAll(providerEnv.path))
}

func (providerEnv *testDBProviderEnv) truncate() {
	if providerEnv.provider != nil {
		providerEnv.provider.Truncate()
	}
}
