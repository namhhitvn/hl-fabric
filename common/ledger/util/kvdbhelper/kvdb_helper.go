package kvdbhelper

import (
	"fmt"
	"sync"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/internal/fileutil"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	goleveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

var logger = flogging.MustGetLogger("kvdbhelper")

type dbState int32

type CassandraIndexModel struct {
	key       []byte
	value     []byte
	timestamp time.Time
}

const (
	closed dbState = iota
	opened
)

// DB - a wrapper on an actual store
type DB struct {
	conf             *Conf
	leveldb          *leveldbhelper.DB
	cassandraCluster *gocql.ClusterConfig
	cassandra        *gocql.Session
	dbState          dbState
	mutex            sync.RWMutex

	readOpts        *opt.ReadOptions
	writeOptsNoSync *opt.WriteOptions
	writeOptsSync   *opt.WriteOptions
}

// CreateDB constructs a `DB`
func CreateDB(conf *Conf) *DB {
	readOpts := &opt.ReadOptions{}
	writeOptsNoSync := &opt.WriteOptions{}
	writeOptsSync := &opt.WriteOptions{}
	writeOptsSync.Sync = true

	// leveldb
	leveldb := leveldbhelper.CreateDB(&leveldbhelper.Conf{
		DBPath:         conf.DBPath,
		ExpectedFormat: conf.ExpectedFormat,
	})

	// TODO: namhhitvn - mapping setting
	// cassandra
	cassandraCluster := gocql.NewCluster("localhost:9042")
	cassandraCluster.Keyspace = "hlf"

	return &DB{
		conf:             conf,
		leveldb:          leveldb,
		cassandraCluster: cassandraCluster,
		dbState:          closed,
		readOpts:         readOpts,
		writeOptsNoSync:  writeOptsNoSync,
		writeOptsSync:    writeOptsSync,
	}
}

func (dbInst *DB) Truncate() {
	dbInst.mutex.Lock()
	defer dbInst.mutex.Unlock()

	if dbInst.cassandraCluster != nil {
		var err error
		dbInst.cassandra, err = dbInst.cassandraCluster.CreateSession()
		if err != nil {
			panic(fmt.Sprintf("Error creating cassandra session: %s", err))
		}
	}

	query := `TRUNCATE TABLE hlf_index`
	err := dbInst.cassandra.Query(query).Exec()
	if err != nil {
		panic(fmt.Sprintf("Error cleanup cassandra session: %s", err))
	}
}

// Open opens the underlying db
func (dbInst *DB) Open() {
	dbInst.mutex.Lock()
	defer dbInst.mutex.Unlock()
	if dbInst.dbState == opened {
		return
	}

	// leveldb
	dbInst.leveldb.Open()

	// cassandra
	if dbInst.cassandraCluster != nil {
		var err error
		dbInst.cassandra, err = dbInst.cassandraCluster.CreateSession()
		if err != nil {
			panic(fmt.Sprintf("Error creating cassandra session: %s", err))
		}
	}

	dbInst.dbState = opened
}

// IsEmpty returns whether or not a database is empty
func (dbInst *DB) IsEmpty() (bool, error) {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()

	// leveldb
	hasItems, err := dbInst.leveldb.IsEmpty()

	// cassandra
	if dbInst.cassandra != nil {
		query := `SELECT * FROM hlf_index`
		itr := dbInst.cassandra.Query(query).Iter()
		defer itr.Close()
		scanner := itr.Scanner()
		hasItems = !scanner.Next()
		err = errors.Wrapf(scanner.Err(), "error while trying to see if the cassandra is empty")
	}

	return hasItems, err
}

// Close closes the underlying db
func (dbInst *DB) Close() {
	dbInst.mutex.Lock()
	defer dbInst.mutex.Unlock()
	if dbInst.dbState == closed {
		return
	}

	// leveldb
	dbInst.leveldb.Close()

	// cassandra
	if dbInst.cassandra != nil {
		dbInst.cassandra.Close()
	}

	dbInst.dbState = closed
}

// Get returns the value for the given key
func (dbInst *DB) Get(key []byte) ([]byte, error) {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()

	// leveldb
	value, err := dbInst.leveldb.Get(key)

	// cassandra
	if dbInst.cassandra != nil {
		var retrieved CassandraIndexModel
		query := `SELECT key, value, timestamp FROM hlf_index WHERE key = ?`
		err = dbInst.cassandra.Query(query, key).Scan(&retrieved.key, &retrieved.value, &retrieved.timestamp)
		if err != nil {
			if err != gocql.ErrNotFound {
				logger.Errorf("Error retrieving cassandra key [%#v]: %s", key, err)
				value = nil
				err = errors.Wrapf(err, "error retrieving cassandra key [%#v]", key)
			} else {
				err = nil
			}
		} else {
			value = retrieved.value
			err = nil
		}
	}

	return value, err
}

// Put saves the key/value
func (dbInst *DB) Put(key []byte, value []byte, sync bool) error {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()

	// leveldb
	err := dbInst.leveldb.Put(key, value, sync)

	// cassandra
	if dbInst.cassandra != nil {
		query := `INSERT INTO hlf_index (key, value, timestamp) VALUES (?, ?, ?)`
		err = dbInst.cassandra.Query(query, key, value, time.Now()).Exec()
		if err != nil {
			logger.Errorf("Error writing cassandra key [%#v]", key)
			err = errors.Wrapf(err, "error writing cassandra key [%#v]", key)
		}
	}

	return err
}

// Delete deletes the given key
func (dbInst *DB) Delete(key []byte, sync bool) error {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()

	// leveldb
	err := dbInst.leveldb.Delete(key, sync)

	// cassandra
	if dbInst.cassandra != nil {
		query := `DELETE FROM hlf_index WHERE key = ?`
		err = dbInst.cassandra.Query(query, key).Exec()
		if err != nil {
			logger.Errorf("Error deleting cassandra key [%#v]", key)
			err = errors.Wrapf(err, "error deleting cassandra key [%#v]", key)
		}
	}

	return err
}

// GetIterator returns an iterator over key-value store. The iterator should be released after the use.
// The resultset contains all the keys that are present in the db between the startKey (inclusive) and the endKey (exclusive).
// A nil startKey represents the first available key and a nil endKey represent a logical key after the last available key
func (dbInst *DB) GetIterator(startKey []byte, endKey []byte) iterator.Iterator {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()

	// leveldb
	itr := dbInst.leveldb.DB.NewIterator(&goleveldbutil.Range{Start: startKey, Limit: endKey}, dbInst.readOpts)

	// cassandra
	if dbInst.cassandra != nil {
		// TODO: huhu
	}

	return itr
}

// WriteBatch writes a batch
func (dbInst *DB) WriteBatch(batch *leveldb.Batch, sync bool) error {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()

	// leveldb
	err := dbInst.leveldb.WriteBatch(batch, sync)

	// cassandra
	if dbInst.cassandra != nil {
		if dbInst.dbState == closed || batch == nil || batch.Len() == 0 {
			logger.Errorf("Error writing batch cassandra")
			err = errors.Wrapf(err, "error writing batch cassandra")
		}

		batch.Replay(&CassandraBatch{
			session: dbInst.cassandra,
		})
	}

	return err
}

// FileLock encapsulate the DB that holds the file lock.
// As the FileLock to be used by a single process/goroutine,
// there is no need for the semaphore to synchronize the
// FileLock usage.
type FileLock struct {
	db       *leveldb.DB
	filePath string
}

// NewFileLock returns a new file based lock manager.
func NewFileLock(filePath string) *FileLock {
	return &FileLock{
		filePath: filePath,
	}
}

// Lock acquire a file lock. We achieve this by opening
// a db for the given filePath. Internally, leveldb acquires a
// file lock while opening a db. If the db is opened again by the same or
// another process, error would be returned. When the db is closed
// or the owner process dies, the lock would be released and hence
// the other process can open the db. We exploit this leveldb
// functionality to acquire and release file lock as the leveldb
// supports this for Windows, Solaris, and Unix.
func (f *FileLock) Lock() error {
	dbOpts := &opt.Options{}
	var err error
	var dirEmpty bool
	if dirEmpty, err = fileutil.CreateDirIfMissing(f.filePath); err != nil {
		panic(fmt.Sprintf("Error creating dir if missing: %s", err))
	}
	dbOpts.ErrorIfMissing = !dirEmpty
	db, err := leveldb.OpenFile(f.filePath, dbOpts)
	if err != nil && err == syscall.EAGAIN {
		return errors.Errorf("lock is already acquired on file %s", f.filePath)
	}
	if err != nil {
		panic(fmt.Sprintf("Error acquiring lock on file %s: %s", f.filePath, err))
	}

	// only mutate the lock db reference AFTER validating that the lock was held.
	f.db = db

	return nil
}

// Determine if the lock is currently held open.
func (f *FileLock) IsLocked() bool {
	return f.db != nil
}

// Unlock releases a previously acquired lock. We achieve this by closing
// the previously opened db. FileUnlock can be called multiple times.
func (f *FileLock) Unlock() {
	if f.db == nil {
		return
	}
	if err := f.db.Close(); err != nil {
		logger.Warningf("unable to release the lock on file %s: %s", f.filePath, err)
		return
	}
	f.db = nil
}

type CassandraBatch struct {
	session *gocql.Session
}

func (b *CassandraBatch) Put(key, value []byte) {
	query := `INSERT INTO hlf_index (key, value, timestamp) VALUES (?, ?, ?)`
	err := b.session.Query(query, key, value, time.Now()).Exec()
	if err != nil {
		logger.Errorf("Error batch writing cassandra key [%#v]", key)
	}
}

func (b *CassandraBatch) Delete(key []byte) {
	query := `DELETE FROM hlf_index WHERE key = ?`
	err := b.session.Query(query, key).Exec()
	if err != nil {
		logger.Errorf("Error batch deleting cassandra key [%#v]", key)
	}
}
