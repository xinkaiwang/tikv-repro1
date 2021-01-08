package integrationtest

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/stretchr/testify/assert"
)

func OpenKvStorage() (kv.Storage, error) {
	d := tikv.Driver{}
	// TODO: Investigate the path & query params here. Current version of TiDB libs seem to ignore
	// all of it.
	storage, err := d.Open(fmt.Sprintf("tikv://%s/pd?cluster=1", "localhost:2379"))
	if err != nil {
		return nil, err
	}
	return storage, nil
}

func TestBasicGet(t *testing.T) {
	storage, err := OpenKvStorage()

	// rawKeyStr := "7307"
	// rawValueStr := "0103"

	rawKeyStr := "74800000080000006c5f69662eff313031fb"
	rawValueStr := "06f80306000000000031"

	rawKey, err := hex.DecodeString(rawKeyStr)
	assert.Nil(t, err)
	rawValue, err := hex.DecodeString(rawValueStr)
	assert.Nil(t, err)

	{
		// delete
		tx, err := storage.Begin()
		assert.Nil(t, err)
		defer tx.Rollback()
		err = tx.Delete(kv.Key(rawKey))
		assert.Nil(t, err)
		err = tx.Commit(context.Background())
		assert.Nil(t, err)
	}
	{
		// write
		tx, err := storage.Begin()
		assert.Nil(t, err)
		defer tx.Rollback()
		err = tx.Set(kv.Key(rawKey), rawValue)
		assert.Nil(t, err)
		err = tx.Commit(context.Background())
		assert.Nil(t, err)
	}

	{
		// read (verify)
		tx, err := storage.Begin()
		assert.Nil(t, err)
		defer tx.Rollback()
		_, err = tx.Get(context.Background(), kv.Key(rawKey))
		assert.Nil(t, err)
		// assert.Equal(t, rawValueStr, hex.EncodeToString(result))
	}
}
