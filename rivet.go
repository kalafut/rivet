package rivet

import (
	"encoding/binary"
	"fmt"
	"log"
	"path/filepath"

	"github.com/boltdb/bolt"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const DefaultBucket = "__default__"
const expBucket = "__expires__"

var dbs = make(map[string]*bolt.DB)

type Rivet struct {
	*bolt.DB
	path   string
	bucket string
}

func New(filename string, bucket ...string) (*Rivet, error) {
	b := DefaultBucket
	if len(bucket) > 0 {
		b = bucket[0]
	}
	return newDb(filename, b)
}

func newDb(filename, bucket string) (*Rivet, error) {
	var db *bolt.DB
	var err error

	path, err := filepath.Abs(filename)
	if err != nil {
		return nil, err
	}

	db, ok := dbs[path]
	if !ok {
		db, err = bolt.Open(filename, 0600, nil)
		if err != nil {
			return nil, err
		}
		dbs[path] = db
	}

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}

		return nil
	})

	return &Rivet{db, path, bucket}, nil
}

func (db *Rivet) Close() {
	db.DB.Close()
	delete(dbs, db.path)
}

func (db *Rivet) SetJ(key string, data interface{}) {
	b, _ := json.Marshal(data)
	db.SetBytes(key, b)
}

func (db *Rivet) GetJ(key string, out interface{}) {
	b := db.GetBytes(key)
	json.Unmarshal(b, out)
}

func (db *Rivet) SetBytes(key string, data []byte) {
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.bucket))
		err := b.Put([]byte(key), data)
		return err
	})

	if err != nil {
		log.Fatal(err)
	}
}

func (db *Rivet) Set(key, data string) {
	db.SetBytes(key, []byte(data))
}

func (db *Rivet) SetX(key, data string, expires int) {
	db.SetBytes(key, []byte(data))
}

func (db *Rivet) SetInt(key string, data int64) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(data))
	db.SetBytes(key, b)
}

func (db *Rivet) GetInt(key string) int64 {
	val, _ := db.GetIntOK(key)
	return val
}

func (db *Rivet) GetIntOK(key string) (int64, bool) {
	b := db.GetBytes(key)
	if b == nil {
		return 0, false
	}

	result := int64(binary.BigEndian.Uint64(b))

	return result, true
}

//func (db *Rivet) Expire(key string, expires int) {
//	b := make([]byte, 8)
//
//	expiration = time.Unix() + expires
//
//	binary.BigEndian.PutUint64(b, data)
//	db.SetBytes(key, b)
//}

func (db *Rivet) Get(key string) string {
	b := db.GetBytes(key)

	return string(b)
}

func (db *Rivet) GetOK(key string) (string, bool) {
	b := db.GetBytes(key)

	return string(b), b != nil
}

func (db *Rivet) Keys() []string {
	var keys []string

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.bucket))

		b.ForEach(func(k, _ []byte) error {
			keys = append(keys, string(k))
			return nil
		})
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	return keys
}

func (db *Rivet) GetBytes(key string) []byte {
	var out []byte

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.bucket))
		data := b.Get([]byte(key))
		if data != nil {
			out = make([]byte, len(data))
			copy(out, data)
		}
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	return out
}

func (db *Rivet) Del(key string) {
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.bucket))
		err := b.Delete([]byte(key))
		return err
	})

	if err != nil {
		log.Fatal(err)
	}
}
