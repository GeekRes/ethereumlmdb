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

	fmt.Println(file)

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

func (db *DB) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	txn, err := db.env.BeginTxn(nil, lmdb.Readonly)
	if err != nil {
		return nil, err
	}

	defer txn.Abort()
	dpi, err := txn.OpenDBI(MainDB, 0)

	if err != nil {
		return nil, err
	}

	value, err = txn.Get(dpi, key)
	db.env.CloseDBI(dpi)
	return value, err
}

func (db *DB) Has(key []byte, ro *opt.ReadOptions) (ret bool, err error) {
	txn, err := db.env.BeginTxn(nil, lmdb.Readonly)
	if err != nil {
		return false, err
	}

	defer txn.Abort()
	dpi, err := txn.OpenDBI(MainDB, 0)

	if err != nil {
		return false, err
	}

	_, err = txn.Get(dpi, key)
	db.env.CloseDBI(dpi)

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

		err = txn.Put(db, key, value, lmdb.Current)
		if err != nil {
			return fmt.Errorf("put: %v", err)
		}
		return nil
	})

	return err
}

func (db *DB) Delete(key []byte, wo *opt.WriteOptions) error {
	txn, err := db.env.BeginTxn(nil, 0)
	if err != nil {
		return err
	}
	defer txn.Abort()

	dpi, err := txn.OpenDBI(MainDB, 0)
	if err != nil {
		return err
	}
	defer db.env.CloseDBI(dpi)

	err = txn.Del(dpi, key, nil)
	if err != nil {
		return err
	}
	err = txn.Commit()
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
	return batch.txn.Commit()
}

type BatchReplay interface {
	Put(key, value []byte)
	Delete(key []byte)
}

type Batch struct {
	txn      *lmdb.Txn
	dbi      lmdb.DBI
	errornum int
}

func NewBatch(env *lmdb.Env) *Batch {
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		panic(err)
	}

	dbi, err := txn.OpenDBI(MainDB, lmdb.Create)
	if err != nil {
		panic(err)
	}

	b := &Batch{
		txn: txn,
		dbi: dbi,
	}

	return b
}

func (b *Batch) Put(key, value []byte) {
	err := b.txn.Put(b.dbi, key, value, 0)

	if err != nil {
		b.errornum++
	}
}

func (b *Batch) Delete(key []byte) {
	err := b.txn.Del(b.dbi, key, nil)
	if err != nil {
		b.errornum++
	}
}
func (b *Batch) Reset() {
	b.txn.Reset()
}

func (b *Batch) Replay(r BatchReplay) error {
	return nil
}
