package boltHandler

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/StreamSpace/ss-store"
	"github.com/boltdb/bolt"
	"github.com/google/uuid"
	logger "github.com/ipfs/go-log/v2"
)

var log = logger.Logger("store/bolt")

type BoltConfig struct {
	Root   string
	DbName string
	Bucket string
}

func (b *BoltConfig) Handler() string {
	return "boltdb"
}

func (b *BoltConfig) StoreFile() string {
	return b.Root + string(os.PathSeparator) + b.DbName + ".db"
}

type ssBoltHandler struct {
	dbP  *bolt.DB
	conf *BoltConfig
}

func NewBoltStore(conf *BoltConfig) (store.Store, error) {
	db, err := bolt.Open(conf.StoreFile(), 0600, nil)
	if err != nil {
		return nil, err
	}
	return &ssBoltHandler{
		dbP:  db,
		conf: conf,
	}, nil
}

func createBoltKey(i store.Item) []byte {
	return []byte(i.GetNamespace() + "_" + i.GetId())
}

// DeleteIndex Func Imp
func (b *ssBoltHandler) deleteIndex(key []byte) error {
	var indexName = []byte("Index")
	return b.dbP.Update(func(tx *bolt.Tx) error {
		indxBkt, err := tx.CreateBucketIfNotExists(indexName)
		if err != nil {
			return err
		}
		err = indxBkt.Delete(key)
		if err != nil {
			return err
		}
		return nil
	})
}

// AddIndex Func Imp
func (b *ssBoltHandler) addIndex(i store.Item, key []byte) error {
	var indexName = []byte("Index")
	return b.dbP.Update(func(tx *bolt.Tx) error {
		value := createBoltKey(i)
		indxBkt, err := tx.CreateBucketIfNotExists(indexName)
		if err != nil {
			return err
		}
		err = indxBkt.Put(key, value)
		if err != nil {
			return err
		}
		return nil
	})
}

// Create
func (b *ssBoltHandler) Create(i store.Item) error {
	serializableItem, ok := i.(store.Serializable)
	if ok != true {
		return errors.New("item is not Serializable")
	}
	idSetter, ok := i.(store.IDSetter)
	if ok == true {
		idSetter.SetID(uuid.New().String())
	}
	if timeTracker, ok := i.(store.TimeTracker); ok {
		var unixTime = time.Now().Unix()
		timeTracker.SetCreated(unixTime)
		timeTracker.SetUpdated(unixTime)
		// Index Created
		key := []byte(fmt.Sprintf("%d_created", timeTracker.GetCreated()))
		err := b.addIndex(i, key)
		if err != nil {
			return err
		}
		// Index Updated
		key = []byte(fmt.Sprintf("%d_updated", timeTracker.GetUpdated()))
		err = b.addIndex(i, key)
		if err != nil {
			return err
		}
	}
	// Main Bucket
	err := b.dbP.Update(func(tx *bolt.Tx) error {
		key := createBoltKey(i)
		value, err := serializableItem.Marshal()
		if err != nil {
			return err
		}
		mainBkt, err := tx.CreateBucketIfNotExists([]byte(b.conf.Bucket))
		if err != nil {
			return err
		}
		err = mainBkt.Put(key, value)
		return err
	})
	return err
}

// Read
func (b *ssBoltHandler) Read(i store.Item) error {

	serializableItem, ok := i.(store.Serializable)
	if ok != true {
		return errors.New("item is not Serializable")
	}
	bucketName := []byte(b.conf.Bucket)
	key := createBoltKey(i)

	err := b.dbP.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return errors.New("bucket not found")
		}
		buf := bucket.Get(key)
		if buf == nil {
			return store.ErrRecordNotFound
		}
		return serializableItem.Unmarshal(buf)
	})
	return err
}

// Update
func (b *ssBoltHandler) Update(i store.Item) error {
	serializableItem, ok := i.(store.Serializable)
	if ok != true {
		return errors.New("item is not Serializable")
	}
	if timeTracker, ok := i.(store.TimeTracker); ok {
		// Delete Prev Updated
		delKey := []byte(fmt.Sprintf("%d_updated", timeTracker.GetUpdated()))
		err := b.deleteIndex(delKey)
		if err != nil {
			return err
		}
		// Add New Updated
		var unixTime = time.Now().Unix()
		timeTracker.SetUpdated(unixTime)
		key := []byte(fmt.Sprintf("%d_updated", unixTime))
		err = b.addIndex(i, key)
		if err != nil {
			return err
		}
	}
	// Main Bucket
	err := b.dbP.Update(func(tx *bolt.Tx) error {
		key := createBoltKey(i)
		value, err := serializableItem.Marshal()
		if err != nil {
			return err
		}
		bucket, err := tx.CreateBucketIfNotExists([]byte(b.conf.Bucket))
		if err != nil {
			return err
		}
		err = bucket.Put(key, value)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

// Delete
func (b *ssBoltHandler) Delete(i store.Item) error {
	bucketName := []byte(b.conf.Bucket)
	key := createBoltKey(i)

	err := b.dbP.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		err = bucket.Delete(key)
		if err != nil {
			return err
		}
		return nil
	})
	if timeTracker, ok := i.(store.TimeTracker); ok {
		// Delete Prev Created
		delKey := []byte(fmt.Sprintf("%d_created", timeTracker.GetCreated()))
		err := b.deleteIndex(delKey)
		if err != nil {
			return err
		}
		// Delete Prev Updated
		delKey = []byte(fmt.Sprintf("%d_updated", timeTracker.GetUpdated()))
		err = b.deleteIndex(delKey)
		if err != nil {
			return err
		}
	}
	return err
}

// List
func (b *ssBoltHandler) List(factory store.Factory, o store.ListOpt) (store.Items, error) {
	var (
		mainBucket        = []byte(b.conf.Bucket)
		indexBucket       = []byte("Index")
		skip              = (o.Page) * o.Limit
		listCounter       = 0
		loopCounter int64 = 1
		list			  = []store.Item{}
	)
	nsToList := factory.Factory().GetNamespace()
	order := o.Sort

	err := b.dbP.View(func(tx *bolt.Tx) error {
		mainBkt := tx.Bucket(mainBucket)
		if mainBkt == nil {
			return errors.New("bucket not found")
		}
		mainCursor := mainBkt.Cursor()
		var indexCursor *bolt.Cursor
		if order != store.SortNatural {
			indexBkt := tx.Bucket(indexBucket)
			if indexBkt == nil {
				return errors.New("index Bucket not found")
			}
			indexCursor = indexBkt.Cursor()
		}
		switch order {
		case store.SortNatural:
			for k, v := mainCursor.First(); k != nil; k, v = mainCursor.Next() {
				if listCounter < int(o.Limit) {
					serializableItem := factory.Factory()
					if strings.HasPrefix(string(k), nsToList) {
						if loopCounter <= skip {
							loopCounter++
							continue
						}
						err := serializableItem.Unmarshal(v)
						if err != nil {
							return err
						}
						list = append(list, serializableItem)
						listCounter++
					}
				}
			}
		case store.SortCreatedAsc:
			if indexCursor != nil {
				for k, v := indexCursor.First(); k != nil; k, v = indexCursor.Next() {
					if strings.HasSuffix(string(k), "created") && strings.HasPrefix(string(v), nsToList) {
						mainValue := mainBkt.Get(v)
						if listCounter < int(o.Limit) {
							serializableItem := factory.Factory()
							log.Debug("IndexKey:", string(k), "IndexValue:", string(v), "MainValue:", string(mainValue))
							if loopCounter <= skip {
								loopCounter++
								continue
							}
							err := serializableItem.Unmarshal(mainValue)
							if err != nil {
								return err
							}
							list = append(list, serializableItem)
							listCounter++
						}
					}
				}
			}
		case store.SortCreatedDesc:
			if indexCursor != nil {
				for k, v := indexCursor.Last(); k != nil; k, v = indexCursor.Prev() {
					if strings.HasSuffix(string(k), "created") && strings.HasPrefix(string(v), nsToList) {
						mainValue := mainBkt.Get(v)
						if listCounter < int(o.Limit) {
							serializableItem := factory.Factory()
							log.Debug("IndexKey:", string(k), "IndexValue:", string(v), "MainValue:", string(mainValue))
							if loopCounter <= skip {
								loopCounter++
								continue
							}
							err := serializableItem.Unmarshal(mainValue)
							if err != nil {
								return err
							}
							list = append(list, serializableItem)
							listCounter++
						}
					}
				}
			}
		case store.SortUpdatedAsc:
			if indexCursor != nil {
				for k, v := indexCursor.First(); k != nil; k, v = indexCursor.Next() {
					if strings.HasSuffix(string(k), "updated") && strings.HasPrefix(string(v), nsToList) {
						mainValue := mainBkt.Get(v)
						if listCounter < int(o.Limit) {
							serializableItem := factory.Factory()
							log.Debug("IndexKey:", string(k), "IndexValue:", string(v), "MainValue:", string(mainValue))
							if loopCounter <= skip {
								loopCounter++
								continue
							}
							err := serializableItem.Unmarshal(mainValue)
							if err != nil {
								return err
							}
							list = append(list, serializableItem)
							listCounter++
						}
					}
				}
			}
		case store.SortUpdatedDesc:
			if indexCursor != nil {
				for k, v := indexCursor.Last(); k != nil; k, v = indexCursor.Prev() {
					if strings.HasSuffix(string(k), "updated") && strings.HasPrefix(string(v), nsToList) {
						mainValue := mainBkt.Get(v)
						if listCounter < int(o.Limit) {
							serializableItem := factory.Factory()
							log.Debug("IndexKey:", string(k), "IndexValue:", string(v), "MainValue:", string(mainValue))
							if loopCounter <= skip {
								loopCounter++
								continue
							}
							err := serializableItem.Unmarshal(mainValue)
							if err != nil {
								return err
							}
							list = append(list, serializableItem)
							listCounter++
						}
					}
				}
			}
		}
		return nil
	})
	return list, err
}

func (b *ssBoltHandler) Close() error {
	if b.dbP == nil {
		return nil
	}
	return b.dbP.Close()
}
