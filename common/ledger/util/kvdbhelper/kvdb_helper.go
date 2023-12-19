package kvdbhelper

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	coreLedger "github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/internal/fileutil"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	goleveldbIterator "github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	goleveldbUtil "github.com/syndtr/goleveldb/leveldb/util"
)

var (
	logger                 = flogging.MustGetLogger("kvdbhelper")
	GlobalKeyValueDBConfig = &coreLedger.KeyValueDBConfig{
		KeyValueDatabase: coreLedger.CassandraDB,
		CassandraDB: &coreLedger.CassandraDBConfig{
			Hosts:    []string{"localhost:9042"},
			Keyspace: "hlf",
		},
	}
)

type dbState int32

type CassandraIndexModel struct {
	uuid      string
	name      []byte
	key       []byte
	value     []byte
	prefix    []byte
	timestamp time.Time
}

func (c CassandraIndexModel) Key() []byte {
	if c.name != nil && len(c.name) > 0 {
		return append(append(c.name, dbNameKeySep...), c.key...)
	}
	return c.key
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
	name             string
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
	var cassandraCluster *gocql.ClusterConfig

	leveldb := leveldbhelper.CreateDB(&leveldbhelper.Conf{DBPath: conf.DBPath, ExpectedFormat: conf.ExpectedFormat})

	// TODO: namhhitvn - mapping setting
	if GlobalKeyValueDBConfig.KeyValueDatabase == coreLedger.CassandraDB {
		cassandraCluster = gocql.NewCluster(GlobalKeyValueDBConfig.CassandraDB.Hosts...)
		if GlobalKeyValueDBConfig.CassandraDB.Keyspace != "" {
			cassandraCluster.Keyspace = GlobalKeyValueDBConfig.CassandraDB.Keyspace
		}
	}

	return &DB{
		conf:             conf,
		name:             filepath.Base(conf.DBPath),
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

	err := dbInst.cassandra.Query(fmt.Sprintf(`DESCRIBE TABLE kv_%s;`, dbInst.name)).Exec()
	if err == nil {
		err := dbInst.cassandra.Query(fmt.Sprintf(`TRUNCATE TABLE kv_%s`, dbInst.name)).Exec()
		if err != nil {
			panic(fmt.Sprintf("Error cleanup cassandra session: %s", err))
		}
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

	dbInst.leveldb.Open()

	// cassandra
	if dbInst.cassandraCluster != nil {
		var err error
		dbInst.cassandra, err = dbInst.cassandraCluster.CreateSession()
		if err == nil {
			err = dbInst.cassandra.Query(fmt.Sprintf(`DESCRIBE TABLE kv_%s;`, dbInst.name)).Exec()
			if err != nil {
				err = dbInst.cassandra.Query(`CREATE KEYSPACE IF NOT EXISTS "hlf" WITH REPLICATION = {'class': 'SimpleStrategy','replication_factor': 1}`).Exec()
				err = dbInst.cassandra.Query(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "hlf"."kv_%s" ("uuid" TEXT, "name" TEXT, "key" BLOB, "prefix" BLOB, "value" BLOB, "timestamp" TIMESTAMP, PRIMARY KEY ("uuid", "key"));`, dbInst.name)).Exec()
			}
		}
		if err != nil {
			panic(fmt.Sprintf("kvdb: unable to create session: %s", err))
		}
	}

	dbInst.dbState = opened
}

// Close closes the underlying db
func (dbInst *DB) Close() {
	dbInst.mutex.Lock()
	defer dbInst.mutex.Unlock()
	if dbInst.dbState == closed {
		return
	}

	dbInst.leveldb.Close()

	// cassandra
	if dbInst.cassandra != nil {
		dbInst.cassandra.Close()
	}

	dbInst.dbState = closed
}

// IsEmpty returns whether or not a database is empty
func (dbInst *DB) IsEmpty() (bool, error) {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()

	var hasItems bool
	var err error

	if dbInst.cassandra != nil {
		query := fmt.Sprintf(`SELECT * FROM kv_%s`, dbInst.name)
		iter := dbInst.cassandra.Query(query).Iter()
		defer iter.Close()
		scanner := iter.Scanner()
		hasItems = !scanner.Next()
		err = errors.Wrapf(scanner.Err(), "error while trying to see if the cassandra is empty")
	} else {
		hasItems, err = dbInst.leveldb.IsEmpty()
	}

	return hasItems, err
}

// Get returns the value for the given key
func (dbInst *DB) Get(key []byte) ([]byte, error) {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()

	var value []byte
	var err error

	if dbInst.cassandra != nil {
		if dbInst.dbState == closed {
			err = ErrWithoutOpen
		}

		var retrieved CassandraIndexModel
		query := fmt.Sprintf(`SELECT value FROM kv_%s WHERE uuid = ?`, dbInst.name)
		err = dbInst.cassandra.Query(query, genUUIDFromKey(key)).Scan(&retrieved.value)

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
	} else {
		value, err = dbInst.leveldb.Get(key)
	}

	return value, err
}

// Put saves the key/value
func (dbInst *DB) Put(key []byte, value []byte, sync bool) error {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()

	var err error
	err = dbInst.leveldb.Put(key, value, sync)

	if dbInst.cassandra != nil {
		if dbInst.dbState == closed {
			err = ErrWithoutOpen
		}

		model := newCassandraIndexModel(key, value)
		query := fmt.Sprintf(`INSERT INTO kv_%s (uuid, name, key, prefix, value, timestamp) VALUES (?, ?, ?, ?, ?, ?)`, dbInst.name)
		// fmt.Println(fmt.Sprintf(`[MYDEBUG] Put -> name="%s" key="%s" uuid="%s"`, string(model.name), string(model.key), model.uuid))
		err = dbInst.cassandra.Query(query, model.uuid, model.name, model.key, model.prefix, model.value, model.timestamp).Exec()
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
	err = dbInst.leveldb.Delete(key, sync)

	if dbInst.cassandra != nil {
		query := fmt.Sprintf(`DELETE FROM kv_%s WHERE uuid = ?`, dbInst.name)
		err := dbInst.cassandra.Query(query, genUUIDFromKey(key)).Exec()
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

	if dbInst.cassandra != nil {
		if dbInst.CassandraClosed() {
			return goleveldbIterator.NewEmptyIterator(ErrCassandraClosed)
		}

		var queryArgs []any
		var dbName []byte
		var sKey []byte = startKey
		var eKey []byte = endKey
		var startRetrieved CassandraIndexModel
		var endRetrieved CassandraIndexModel

		query := fmt.Sprintf(`SELECT name, key, value FROM kv_%s`, dbInst.name)
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

		if bytes.Equal(sKey, []byte{TxIDIdxKeyPrefix}) && bytes.Equal(eKey, []byte{TxIDIdxKeyPrefix + 1}) { // is searching all transaction
			query = query + ` WHERE prefix = ?`
			hasWhere = true
			queryArgs = append(queryArgs, []byte{TxIDIdxKeyPrefix})
		} else if isRangeTxId(startKey, endKey) { // is searching special transaction
			query = query + ` WHERE uuid = ?`
			hasWhere = true
			queryArgs = append(queryArgs, genUUIDFromKey(startKey))
		} else if sKey != nil || eKey != nil { // is searching range inserted
			findQuery := fmt.Sprintf(`SELECT name, timestamp FROM kv_%s WHERE uuid = ?`, dbInst.name)
			startErr := dbInst.cassandra.Query(findQuery, genUUIDFromKey(startKey)).Scan(&startRetrieved.name, &startRetrieved.timestamp)
			endErr := dbInst.cassandra.Query(findQuery, genUUIDFromKey(endKey)).Scan(&endRetrieved.name, &endRetrieved.timestamp)

			// sModel := newCassandraIndexModel(startKey, []byte{})
			// eModel := newCassandraIndexModel(endKey, []byte{})
			// fmt.Println(fmt.Sprintf(`[MYDEBUG] GetIterator -> name="%s"\nsKey="%s" sUuid="%s"\neKey="%s" eUuid="%s"`, string(sModel.name), string(sModel.key), sModel.uuid, string(eModel.key), eModel.uuid))

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
	} else {
		iter = dbInst.leveldb.DB.NewIterator(&goleveldbUtil.Range{Start: startKey, Limit: endKey}, dbInst.readOpts)
	}

	return iter
}

// WriteBatch writes a batch
func (dbInst *DB) WriteBatch(batch *leveldb.Batch, sync bool) error {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()

	var err error

	if dbInst.cassandra != nil {
		if dbInst.dbState == closed || batch == nil || batch.Len() == 0 {
			logger.Errorf("Error writing batch cassandra")
			err = errors.New("Error writing batch cassandra")
			return err
		}

		if err = batch.Replay(&CassandraBatch{
			name:    dbInst.name,
			leveldb: dbInst.leveldb,
			session: dbInst.cassandra,
		}); err != nil {
			err = errors.Wrapf(err, "Error writing batch cassandra")
		}
	} else {
		err = dbInst.leveldb.WriteBatch(batch, sync)
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
	name    string
	leveldb *leveldbhelper.DB
	session *gocql.Session
}

func (b *CassandraBatch) Put(key, value []byte) {
	model := newCassandraIndexModel(key, value)
	query := fmt.Sprintf(`INSERT INTO kv_%s (uuid, name, key, prefix, value, timestamp) VALUES (?, ?, ?, ?, ?, ?)`, b.name)
	// fmt.Println(fmt.Sprintf(`[MYDEBUG] Put(Batch) -> name="%s" key="%s" uuid="%s"`, string(model.name), string(model.key), model.uuid))
	err := b.leveldb.Put(key, value, false)
	err = b.session.Query(query, model.uuid, model.name, model.key, model.prefix, model.value, model.timestamp).Exec()
	if err != nil {
		logger.Errorf("Error batch writing cassandra key [%#v]", key)
	}
}

func (b *CassandraBatch) Delete(key []byte) {
	query := fmt.Sprintf(`DELETE FROM kv_%s WHERE uuid = ?`, b.name)
	err := b.leveldb.Delete(key, false)
	err = b.session.Query(query, genUUIDFromKey(key)).Exec()
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
		if bytes.Equal(value.Key(), key) || bytes.Compare(value.Key(), key) > 0 {
			return i
		}
	}
	if def == -1 && bytes.Compare(ia.rows[len(ia.rows)-1].Key(), key) < 0 {
		def = len(ia.rows)
	}
	return def
}

func (ia *CassandraIteratorArray) Index(i int) (key, value []byte) {
	if i < 0 || i >= len(ia.rows) {
		panic(ErrCassandraIterIndexRange)
	}
	row := ia.rows[i]
	return row.Key(), row.value
}

func NewCassandraIteratorArray(iter *gocql.Iter) *CassandraIteratorArray {
	var rows []CassandraIndexModel
	scanner := iter.Scanner()
	defer iter.Close()

	// TODO: should scan key to sort instead of scan all
	for scanner.Next() {
		var retrieved CassandraIndexModel
		err := scanner.Scan(&retrieved.name, &retrieved.key, &retrieved.value)
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

var namespace = uuid.UUID{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}

func genUUIDFromKey(key []byte) string {
	trimKey := bytes.Split(key, TxIDIdxKeySep)[0]
	uuid := uuid.NewSHA1(namespace, trimKey)
	return uuid.String()
}

func isRangeTxId(sKey []byte, eKey []byte) bool {
	prefix := []byte{TxIDIdxKeyPrefix}
	suffix := []byte{0xff}
	sModel := newCassandraIndexModel(sKey, []byte{})
	eModel := newCassandraIndexModel(eKey, []byte{})

	if bytes.Equal(sModel.prefix, prefix) &&
		bytes.Equal(eModel.prefix, prefix) &&
		bytes.Index(sModel.key, suffix) == -1 &&
		(bytes.Equal(sModel.key, eModel.key) || (bytes.Index(eModel.key, suffix) > 0 && bytes.Equal(sModel.key, eModel.key[:bytes.Index(eModel.key, suffix)]))) {
		return true
	}

	return false
}

func newCassandraIndexModel(key []byte, value []byte) *CassandraIndexModel {
	var dbKey []byte = key
	var prefix []byte

	dbNameLength := bytes.Index(key, dbNameKeySep)
	if dbNameLength != -1 {
		dbKey = key[dbNameLength+1:]
	}

	if dbKey != nil && len(dbKey) > 0 {
		prefix = dbKey[:1]
	}

	return &CassandraIndexModel{
		uuid:      genUUIDFromKey(key),
		name:      GetDBName(key),
		key:       dbKey,
		value:     value,
		prefix:    prefix,
		timestamp: time.Now(),
	}
}
