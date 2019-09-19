package leveldb

import (
	"fmt"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var MainDB string = "db"

type DB struct {
	env *lmdb.Env
	dbi lmdb.DBI
}

func NewDB(file string) (*DB, error) {
	env, err := lmdb.NewEnv()
	if err != nil {
		panic(err)
	}

	err = env.SetMaxDBs(1)
	if err != nil {
		panic(err)
	}

	err = env.SetMapSize(1024 * 1024 * 1024)
	if err != nil {
		panic(err)
	}

	fmt.Println("========> ", file)

	err = env.Open(file, 0, 0644)
	if err != nil {
		panic(err)
	}

	db := &DB{
		env: env,
	}

	return db, nil
}

func (db *DB) GetEnv() *lmdb.Env {
	return db.env
}

func (db *DB) Close() error {
	if db.env != nil {
		return db.env.Close()
	}

	return nil
}

func (db *DB) Get(key []byte, ro *opt.ReadOptions) ([]byte, error) {
	var value []byte
	err := db.env.View(func(txn *lmdb.Txn) (err error) {
		dbi, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}

		value, err = txn.Get(dbi, key)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (db *DB) Has(key []byte, ro *opt.ReadOptions) (bool, error) {
	var value []byte
	err := db.env.View(func(txn *lmdb.Txn) (err error) {
		dbi, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}

		value, err = txn.Get(dbi, key)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return false, err
	}

	return true, nil
}

func (db *DB) Put(key, value []byte, wo *opt.WriteOptions) error {
	err := db.env.Update(func(txn *lmdb.Txn) (err error) {
		db, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}

		err = txn.Put(db, key, value, 0)
		if err != nil {
			return fmt.Errorf("put: %v", err)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	return err
}

func (db *DB) Delete(key []byte, wo *opt.WriteOptions) error {
	err := db.env.Update(func(txn *lmdb.Txn) (err error) {
		db, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}

		err = txn.Del(db, key, nil)
		if err != nil {
			return fmt.Errorf("del: %v", err)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	return err
}

func (db *DB) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	return nil
}

func (db *DB) GetProperty(name string) (value string, err error) {
	return "", nil
}

func (db *DB) CompactRange(r util.Range) error {
	return nil
}

func (db *DB) Write(batch *Batch, wo *opt.WriteOptions) error {

	err := db.env.Update(func(txn *lmdb.Txn) (err error) {
		db, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}

		for k, v := range batch.cache {
			err = txn.Put(db, []byte(k), v, 0)
			if err != nil {
				return fmt.Errorf("put: %v", err)
			}
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	return nil
}

type BatchReplay interface {
	Put(key, value []byte)
	Delete(key []byte)
}

type Batch struct {
	env   *lmdb.Env
	cache map[string][]byte
}

func NewBatch(env *lmdb.Env) *Batch {

	b := &Batch{
		env:   env,
		cache: make(map[string][]byte),
	}

	return b
}

func (b *Batch) Put(key, value []byte) {
	b.cache[string(key)] = value
}

func (b *Batch) Delete(key []byte) {
	delete(b.cache, string(key))
}
func (b *Batch) Reset() {

}

func (b *Batch) Replay(r BatchReplay) error {
	return nil
}
