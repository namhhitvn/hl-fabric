package kvdbhelper

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sort"
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
	goleveldbIterator "github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	goleveldbUtil "github.com/syndtr/goleveldb/leveldb/util"
)

var logger = flogging.MustGetLogger("kvdbhelper")

type dbState int32

type CassandraIndexModel struct {
	key       []byte
	hex_key   string
	value     []byte
	name      []byte
	timestamp time.Time
}

const (
	closed dbState = iota
	opened
)

var (
	ErrWithoutOpen             = errors.New("kvdb: without open")
	ErrCassandraClosed         = errors.New("kvdb: closed")
	ErrCassandraIterIndexRange = errors.New("kvdb/iteratorArray: index out of range")
	ErrCassandraIterScan       = errors.New("kvdb/iteratorArray: iterator sca")
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

	// hlf-leveldb
	// leveldb := leveldbhelper.CreateDB(&leveldbhelper.Conf{DBPath: conf.DBPath, ExpectedFormat: conf.ExpectedFormat})

	// TODO: namhhitvn - mapping setting
	// cassandra
	cassandraCluster := gocql.NewCluster("localhost:9042")
	cassandraCluster.Keyspace = "hlf"

	return &DB{
		conf: conf,
		// hlf-leveldb
		// leveldb:          leveldb,
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

func (dbInst *DB) CassandraClosed() bool {
	if dbInst.cassandra != nil {
		return dbInst.cassandra.Closed()
	}
	return true
}

// Open opens the underlying db
func (dbInst *DB) Open() {
	dbInst.mutex.Lock()
	defer dbInst.mutex.Unlock()
	if dbInst.dbState == opened {
		return
	}

	// hlf-leveldb
	// dbInst.leveldb.Open()

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

	var hasItems bool
	var err error

	// hlf-leveldb
	// hasItems, err = dbInst.leveldb.IsEmpty()

	// cassandra
	if dbInst.cassandra != nil {
		query := `SELECT * FROM hlf_index`
		iter := dbInst.cassandra.Query(query).Iter()
		defer iter.Close()
		scanner := iter.Scanner()
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

	// hlf-leveldb
	// dbInst.leveldb.Close()

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

	var value []byte
	var err error

	if dbInst.dbState == closed {
		err = ErrWithoutOpen
	}

	// hlf-leveldb
	// value, err = dbInst.leveldb.Get(key)

	// cassandra
	if dbInst.cassandra != nil {
		var retrieved CassandraIndexModel
		query := `SELECT key, value, timestamp FROM hlf_index WHERE hex_key = ?`
		err = dbInst.cassandra.Query(query, encodeKeyToHexString(key)).Scan(&retrieved.key, &retrieved.value, &retrieved.timestamp)

		if err == gocql.ErrNotFound {
			value = nil
			err = nil
		} else if err != nil {
			logger.Errorf("Error retrieving cassandra key [%#v]: %s", key, err)
			value = nil
			err = errors.Wrapf(err, "error retrieving cassandra key [%#v]", key)
		} else {
			value = retrieved.value
		}
	}

	return value, err
}

// Put saves the key/value
func (dbInst *DB) Put(key []byte, value []byte, sync bool) error {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()

	var err error

	if dbInst.dbState == closed {
		err = ErrWithoutOpen
	}

	// hlf-leveldb
	// err = dbInst.leveldb.Put(key, value, sync)

	// cassandra
	if dbInst.cassandra != nil {
		query := `INSERT INTO hlf_index (key, hex_key, value, timestamp, name) VALUES (?, ?, ?, ?, ?)`
		fmt.Println(fmt.Sprintf(`[MYDEBUG] Put -> key="%s" hexKey="%s"`, string(key), encodeKeyToHexString(key)))
		err = dbInst.cassandra.Query(query, key, encodeKeyToHexString(key), value, time.Now(), string(GetDBName(key))).Exec()
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

	var err error

	// hlf-leveldb
	// err = dbInst.leveldb.Delete(key, sync)

	// cassandra
	if dbInst.cassandra != nil {
		query := `DELETE FROM hlf_index WHERE hex_key = ?`
		err := dbInst.cassandra.Query(query, encodeKeyToHexString(key)).Exec()
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
func (dbInst *DB) GetIterator(startKey []byte, endKey []byte, args ...[]byte) iterator.Iterator {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()

	var iter iterator.Iterator

	// hlf-leveldb
	// iter = dbInst.leveldb.DB.NewIterator(&goleveldbUtil.Range{Start: startKey, Limit: endKey}, dbInst.readOpts)

	// cassandra
	if dbInst.cassandra != nil {
		if dbInst.CassandraClosed() {
			return goleveldbIterator.NewEmptyIterator(ErrCassandraClosed)
		}

		var queryArgs []any
		var dbName []byte
		var sKey []byte = startKey
		var eKey []byte = endKey

		query := `SELECT key, value FROM hlf_index`
		hasWhere := false
		hasWhereDBName := false

		if len(args) > 0 {
			dbName = args[0]

			if len(args) >= 2 {
				sKey = args[1]
			}

			if len(args) >= 3 {
				eKey = args[2]
			}
		}

		if sKey != nil || eKey != nil {
			var startRetrieved CassandraIndexModel
			var endRetrieved CassandraIndexModel
			findQuery := `SELECT name, timestamp FROM hlf_index WHERE hex_key = ?`
			startErr := dbInst.cassandra.Query(findQuery, encodeKeyToHexString(startKey)).Scan(&startRetrieved.name, &startRetrieved.timestamp)
			endErr := dbInst.cassandra.Query(findQuery, encodeKeyToHexString(endKey)).Scan(&endRetrieved.name, &endRetrieved.timestamp)

			fmt.Println(fmt.Sprintf(`[MYDEBUG] GetIterator -> sKey="%s" eKey="%s" --- sHexKey="%s" eHexKey="%s"`, string(startKey), string(endKey), encodeKeyToHexString(startKey), encodeKeyToHexString(endKey)))

			if startErr == nil {
				query = query + " WHERE timestamp >= ?"
				hasWhere = true
				queryArgs = append(queryArgs, startRetrieved.timestamp)
			} else if sKey != nil && startErr == gocql.ErrNotFound && eKey == nil {
				query = query + " WHERE key = ?"
				hasWhere = true
				queryArgs = append(queryArgs, []byte("_empty_"))
			}

			if endErr == nil {
				if !hasWhere {
					query = query + " WHERE timestamp < ?"
				} else {
					query = query + " AND timestamp < ?"
				}
				hasWhere = true
				queryArgs = append(queryArgs, endRetrieved.timestamp)
			} else if eKey != nil && endErr == gocql.ErrNotFound && ((sKey != nil && startErr != nil) || (sKey == nil || startErr == gocql.ErrNotFound)) {
				if !hasWhere {
					query = query + " WHERE key = ?"
				} else {
					query = query + " AND key = ?"
				}
				hasWhere = true
				queryArgs = append(queryArgs, []byte("_empty_"))
			}

			if startRetrieved.name != nil {
				query = query + " AND name = ?"
				queryArgs = append(queryArgs, startRetrieved.name)
				hasWhereDBName = true
			} else if endRetrieved.name != nil {
				query = query + " AND name = ?"
				queryArgs = append(queryArgs, endRetrieved.name)
				hasWhereDBName = true
			}
		}

		if dbName != nil && !hasWhereDBName {
			if !hasWhere {
				query = query + " WHERE name = ?"
			} else {
				query = query + " AND name = ?"
			}
			queryArgs = append(queryArgs, dbName)
		}

		query = query + " ALLOW FILTERING"
		iter = NewCassandraIterator(dbInst.cassandra.Query(query, queryArgs...).Iter())
	}

	return iter
}

// WriteBatch writes a batch
func (dbInst *DB) WriteBatch(batch *leveldb.Batch, sync bool) error {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()

	var err error

	// hlf-leveldb
	// err = dbInst.leveldb.WriteBatch(batch, sync)

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
	query := `INSERT INTO hlf_index (key, hex_key, value, timestamp, name) VALUES (?, ?, ?, ?, ?)`
	fmt.Println(fmt.Sprintf(`[MYDEBUG] Put(Batch) -> key="%s" hexKey="%s"`, string(key), encodeKeyToHexString(key)))
	err := b.session.Query(query, key, encodeKeyToHexString(key), value, time.Now(), string(GetDBName(key))).Exec()
	if err != nil {
		logger.Errorf("Error batch writing cassandra key [%#v]", key)
	}
}

func (b *CassandraBatch) Delete(key []byte) {
	query := `DELETE FROM hlf_index WHERE hex_key = ?`
	err := b.session.Query(query, encodeKeyToHexString(key)).Exec()
	if err != nil {
		logger.Errorf("Error batch deleting cassandra key [%#v]", key)
	}
}

type CassandraIteratorArray struct {
	iter *gocql.Iter
	rows []CassandraIndexModel
}

func (ia *CassandraIteratorArray) Len() int {
	return len(ia.rows)
}

func (ia *CassandraIteratorArray) Search(key []byte) int {
	def := -1
	for i, value := range ia.rows {
		if bytes.Equal(value.key, key) || bytes.Compare(value.key, key) > 0 {
			return i
		}
	}
	if def == -1 && bytes.Compare(ia.rows[len(ia.rows)-1].key, key) < 0 {
		def = len(ia.rows)
	}
	return def
}

func (ia *CassandraIteratorArray) Index(i int) (key, value []byte) {
	if i < 0 || i >= len(ia.rows) {
		panic(ErrCassandraIterIndexRange)
	}
	row := ia.rows[i]
	return row.key, row.value
}

func NewCassandraIteratorArray(iter *gocql.Iter) *CassandraIteratorArray {
	var rows []CassandraIndexModel
	scanner := iter.Scanner()
	defer iter.Close()

	for scanner.Next() {
		var retrieved CassandraIndexModel
		err := scanner.Scan(&retrieved.key, &retrieved.value)
		if err == gocql.ErrNotFound {
			break
		} else if err != nil {
			panic(ErrCassandraIterScan)
		}
		rows = append(rows, retrieved)
	}

	sort.Slice(rows, func(a int, b int) bool {
		return string(rows[a].key) < string(rows[b].key)
	})

	return &CassandraIteratorArray{iter: iter, rows: rows}
}

type CassandraIterator struct {
	iter goleveldbIterator.Iterator
}

func (i *CassandraIterator) Valid() bool {
	return i.iter.Valid()
}

// First moves the iterator to the first key/value pair. If the iterator
// only contains one key/value pair then First and Last would moves
// to the same key/value pair.
// It returns whether such pair exist.
func (i *CassandraIterator) First() bool {
	return i.iter.First()
}

// Last moves the iterator to the last key/value pair. If the iterator
// only contains one key/value pair then First and Last would moves
// to the same key/value pair.
// It returns whether such pair exist.
func (i *CassandraIterator) Last() bool {
	return i.iter.Last()
}

// Seek moves the iterator to the first key/value pair whose key is greater
// than or equal to the given key.
// It returns whether such pair exist.
// It is safe to modify the contents of the argument after Seek returns.
func (i *CassandraIterator) Seek(key []byte) bool {
	return i.iter.Seek(key)
}

// Next moves the iterator to the next key/value pair.
// It returns false if the iterator is exhausted.
func (i *CassandraIterator) Next() bool {
	return i.iter.Next()
}

// Prev moves the iterator to the previous key/value pair.
// It returns false if the iterator is exhausted.
func (i *CassandraIterator) Prev() bool {
	return i.iter.Prev()
}

// Key returns the key of the current key/value pair, or nil if done.
// The caller should not modify the contents of the returned slice, and
// its contents may change on the next call to any 'seeks method'.
func (i *CassandraIterator) Key() []byte {
	return i.iter.Key()
}

// Value returns the value of the current key/value pair, or nil if done.
// The caller should not modify the contents of the returned slice, and
// its contents may change on the next call to any 'seeks method'.
func (i *CassandraIterator) Value() []byte {
	return i.iter.Value()
}

func (i *CassandraIterator) Release() {
	i.iter.Release()
}

func (i *CassandraIterator) SetReleaser(releaser goleveldbUtil.Releaser) {
	i.iter.SetReleaser(releaser)
}

func (i *CassandraIterator) Error() error {
	return i.iter.Error()
}

func NewCassandraIterator(iter *gocql.Iter) *CassandraIterator {
	return &CassandraIterator{
		iter: goleveldbIterator.NewArrayIterator(NewCassandraIteratorArray(iter)),
	}
}

func GetDBName(key []byte) []byte {
	dbNameLength := bytes.Index(key, dbNameKeySep)
	if dbNameLength == -1 {
		return []byte("")
	}
	return key[:dbNameLength]
}

func trimKey(key []byte) []byte {
	return bytes.Split(key, TxSuffixSep)[0]
}

func encodeKeyToHexString(key []byte) string {
	return hex.EncodeToString(trimKey(key))
}
