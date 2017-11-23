package rivet

import (
	"encoding/binary"
	"log"
	"path/filepath"
	"time"

	"github.com/boltdb/bolt"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const DefaultBucket = "__rivet_default__"
const ExpireBucket = "__rivet_expirations__"

var dbs = make(map[string]*bolt.DB)

type rivet struct {
	*bolt.DB
}

type Rivet struct {
	rivet
	path   string
	bucket string
}

func New(filename string) (*Rivet, error) {
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
		for _, bkt := range []string{ExpireBucket, DefaultBucket} {
			_, err := tx.CreateBucketIfNotExists([]byte(bkt))
			if err != nil {
				return err
			}
		}

		return nil
	})

	r := rivet{db}
	//go func() {
	//	for {
	//		//fmt.Printf("Running expire\n")
	//		nextExpiration := r.expire()
	//		//fmt.Printf("Next run: %v\n", nextExpiration)
	//		if nextExpiration.IsZero() {
	//			time.Sleep(1 * time.Second)
	//		} else {
	//			time.Sleep(nextExpiration.Sub(time.Now()))
	//		}
	//	}
	//}()
	return &Rivet{r, path, DefaultBucket}, nil
}

func (db *Rivet) SetBucket(bucket string) {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucket))
		return err
	})

	if err != nil {
		log.Fatal(err)
	}

	db.bucket = bucket
}

func (db Rivet) SetData(key string, data interface{}) {
	b, _ := json.Marshal(data)
	db.SetBytes(key, b)
}

func (db Rivet) GetData(key string, out interface{}) {
	b := db.GetBytes(key)
	json.Unmarshal(b, out)
}

func (db Rivet) SetBytes(key string, val []byte) {
	db.setBytes(db.bucket, key, val)
}

func (db Rivet) setBytes(bucket, key string, val []byte) {
	db.clearExpire(db.bucket, key)
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		err := b.Put([]byte(key), val)
		return err
	})

	if err != nil {
		log.Fatal(err)
	}
}

func (db Rivet) Set(key, val string) {
	db.SetBytes(key, []byte(val))
}

func (db Rivet) SetX(key, val string, expires time.Duration) {
	db.SetBytes(key, []byte(val))
	db.Expire(key, expires)
}

func (db Rivet) SetInt(key string, val int64) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(val))
	db.SetBytes(key, b)
}

func (db Rivet) GetInt(key string) int64 {
	val, _ := db.GetIntOK(key)
	return val
}

func (db Rivet) GetIntOK(key string) (int64, bool) {
	b := db.GetBytes(key)
	if b == nil {
		return 0, false
	}

	result := int64(binary.BigEndian.Uint64(b))

	return result, true
}

func (db Rivet) Get(key string) string {
	b := db.GetBytes(key)

	return string(b)
}

func (db Rivet) GetOK(key string) (string, bool) {
	b := db.GetBytes(key)

	return string(b), b != nil
}

func (db Rivet) Keys() []string {
	var keys, keysRaw []string

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.bucket))

		b.ForEach(func(k, _ []byte) error {
			keysRaw = append(keysRaw, string(k))
			return nil
		})
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	for _, key := range keysRaw {
		if db.TTL(key) != KeyNotFound {
			keys = append(keys, key)
		}
	}

	return keys
}

func (db Rivet) Exists(key string) bool {
	return db.exists(db.bucket, key)
}

func (db Rivet) exists(bucket, key string) bool {
	exists := false
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b != nil {
			data := b.Get([]byte(key))
			exists = (data != nil)
		}

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	return exists
}

func (db Rivet) GetBytes(key string) []byte {
	if db.TTL(key) == KeyNotFound {
		return nil
	}

	return db.getBytes(db.bucket, key)
}

func (db Rivet) getBytes(bucket, key string) []byte {
	var out []byte

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
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

func (db Rivet) Del(key string) {
	db.del(db.bucket, key)
}

func (db Rivet) del(bucket, key string) {
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		err := b.Delete([]byte(key))
		return err
	})

	if err != nil {
		log.Fatal(err)
	}
}

//func (db rivet) expire() time.Time {
//	var nextExpiration time.Time
//
//	db.Update(func(tx *bolt.Tx) error {
//		c := tx.Bucket([]byte(expBucket)).Cursor()
//
//		k, _ := c.First()
//		for k != nil {
//			//for k, _ := c.First(); k != nil && keyExpired(k); k, _ = c.Next() {
//			bucket, key, expiration := unpackExpireKey(k)
//			if expiration.Before(time.Now()) {
//				b := tx.Bucket([]byte(bucket))
//				err := b.Delete([]byte(key))
//				if err != nil {
//					return err
//				}
//				b = tx.Bucket([]byte(expBucket))
//				err = b.Delete([]byte(k))
//				if err != nil {
//					return err
//				}
//				k, _ = c.Next()
//			} else {
//				nextExpiration = expiration
//				break
//			}
//
//		}
//
//		return nil
//	})
//	//expiration := time.Now().Add(time.Duration(expires) * time.Second)
//	//packedKey := packExpireKey(db.bucket, key, expiration)
//	//db.setBytes(expBucket, string(packedKey), []byte{})
//	return nextExpiration
//}
