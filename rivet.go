package rivet

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/boltdb/bolt"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const DefaultBucket = "__default__"

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
	fmt.Printf("%v\n", string(b))
	db.Set(key, b)
}

func (db *Rivet) GetJ(key string, out interface{}) {
	b := db.Get(key)
	json.Unmarshal(b, out)
}

func (db *Rivet) Set(key string, data []byte) {
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.bucket))
		err := b.Put([]byte(key), data)
		return err
	})

	if err != nil {
		log.Fatal(err)
	}
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

func (db *Rivet) Get(key string) []byte {
	var out []byte

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.bucket))
		data := b.Get([]byte(key))
		out = make([]byte, len(data))
		copy(out, data)
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	if len(out) == 0 {
		out = nil
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

/*
func NewEncryptedStore(db *bolt.DB, acct []byte) (EncryptedStore, error) {
	var exists bool

	if len(acct) == 0 {
		return EncryptedStore{}, errors.New("Empty account")
	}

	db.View(func(tx *bolt.Tx) error {
		accts := tx.Bucket([]byte(ACCTS))
		b := accts.Bucket(acct)
		exists = (b != nil)
		return nil
	})

	return EncryptedStore{
		db:          db,
		acct:        acct,
		initialized: exists,
	}, nil
}

func (s *EncryptedStore) Init(passphrase []byte) ([]byte, error) {
	if s.initialized {
		return nil, errors.New("Account already initialized")
	}

	// Create bucket for account data
	err := s.db.Update(func(tx *bolt.Tx) error {
		accts := tx.Bucket([]byte(ACCTS))
		_, err := accts.CreateBucket(s.acct)

		return err
	})

	if err != nil {
		return nil, errors.New("Unable to create account")
	}

	// Generate userkey
	salt := make([]byte, 10)
	rand.Read(salt)

	params := pdkdf2Params{
		Salt:   salt,
		Iter:   4096,
		Keylen: 32,
	}

	userkey := pbkdf2.Key(passphrase, params.Salt, params.Iter, params.Keylen, sha1.New)

	// Generate masterkey
	masterkey := make([]byte, 32)
	rand.Read(masterkey)

	return userkey, nil
}

func (t tmpStore) Put(acct, key, data []byte) error {
	dst := make([]byte, len(data))
	copy(dst, data)
	acctMap, ok := t.store[string(acct)]
	if !ok {
		acctMap = make(map[string][]byte)
		t.store[string(acct)] = acctMap
	}
	acctMap[string(key)] = dst

	return nil
}

func (t tmpStore) Get(acct, key []byte) ([]byte, error) {
	data, ok := t.store[string(acct)][string(key)]
	if !ok {
		return nil, errors.New("Missing key")
	}
	dst := make([]byte, len(data))
	copy(dst, data)

	return dst, nil
}

func NewDB(filename string) *bolt.DB {
}

func AccountExists(db *bolt.DB, acct []byte) bool {
	exists := false

	db.View(func(tx *bolt.Tx) error {
		accts := tx.Bucket([]byte(ACCTS))
		b := accts.Bucket(acct)
		exists = (b != nil)
		return nil
	})

	return exists
}

func StoreItem(db *bolt.DB, acct, id, key, data []byte) error {
	if !AccountExists(db, acct) {
		return errors.New("Account doesn't exist")
	}

	err := db.Update(func(tx *bolt.Tx) error {
		accts := tx.Bucket([]byte(ACCTS))
		b := accts.Bucket(acct)
		err := b.Put(id, data)
		return err
	})

	return err
}

func LoadItem(db *bolt.DB, acct, id, key []byte) ([]byte, error) {
	var out []byte

	if !AccountExists(db, acct) {
		return nil, errors.New("Account doesn't exist")
	}

	err := db.View(func(tx *bolt.Tx) error {
		accts := tx.Bucket([]byte(ACCTS))
		b := accts.Bucket(acct)
		data := b.Get(id)
		if data == nil {
			return errors.New("Missing ID")
		}
		out = make([]byte, len(data))
		copy(out, data)
		return nil
	})

	return out, err
}
*/
