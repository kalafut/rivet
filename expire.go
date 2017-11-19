package rivet

import (
	"bytes"
	"encoding/binary"
	"log"
	"time"
)

const NoExpiration = -1
const KeyNotFound = -2

/*
Requirements for expiration:
  0. Set expiration.
  1. Given a bucket/key, when will it expire (for TTL)
  2. What bucket/key should be expired
  3. Remove/change expiration
  4. We'd like no O(n) operations

  #1 is done by having a full list of bucket|key:expiration
  #2 is done have having a list of expirating keys prefixed by MSB expiration time

Process:
  foreach packedKey in ExpSorted:
    if expiration > Now(), break

*/
func (db Rivet) Expire(key string, expires time.Duration) {
	if db.exists(db.bucket, key) {
		expiration := time.Now().Add(expires)
		db.setExpire(db.bucket, key, expiration)
	}
}

func (db Rivet) TTL(key string) time.Duration {
	exp := db.expireKey(db.bucket, key)
	if exp.IsZero() {
		if db.exists(db.bucket, key) {
			return -1
		} else {
			return -2
		}
	}

	return exp.Sub(time.Now())
}

// expireKey checks the expiration time for a key. If the key has an expiration
// in the future, it is returned. If the key has expired, it will be deleted. In
// this case, or if the key is not found or has no expiration, the zero value is
// returned.
func (db Rivet) expireKey(bucket, key string) time.Time {
	packedKey := packBucketKey(db.bucket, key)
	expiration := db.getExpire(bucket, key)
	//expBytes := db.getBytes(ExpireBucket, packedKey)

	if expiration.IsZero() {
		return expiration
	}
	//	return time.Time{}
	//}

	//exp := binary.LittleEndian.Uint32(expBytes)
	//expiration := time.Unix(int64(exp), 0)

	if expiration.Before(time.Now()) {
		db.del(bucket, key)
		db.del(ExpireBucket, string(packedKey))
		return time.Time{}
	}
	return expiration
}

func (db Rivet) setExpire(bucket, key string, expires time.Time) {
	//expBytes := make([]byte, 4)
	//binary.LittleEndian.PutUint32(expBytes, uint32(expires.Unix()))
	expBytes, _ := expires.MarshalBinary()

	packedKey := packBucketKey(db.bucket, key)
	db.setBytes(ExpireBucket, packedKey, expBytes)
}

func (db Rivet) getExpire(bucket, key string) time.Time {
	var expiration time.Time
	packedKey := packBucketKey(db.bucket, key)
	expBytes := db.getBytes(ExpireBucket, packedKey)
	//expBytes := make([]byte, 4)
	//binary.LittleEndian.PutUint32(expBytes, uint32(expires.Unix()))
	err := expiration.UnmarshalBinary(expBytes)
	if err != nil {
		return time.Time{}
	}

	return expiration
}

func (db Rivet) clearExpire(bucket, key string) {
	packedKey := packBucketKey(db.bucket, key)
	db.del(ExpireBucket, packedKey)
}

func packBucketKey(bucket, key string) string {
	b := new(bytes.Buffer)

	bSize := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(bSize, uint64(len(bucket)))
	b.Write(bSize[:n])
	b.Write([]byte(bucket))
	b.Write([]byte(key))

	return string(b.Bytes())
}

func unpackBucketKey(packed string) (string, string) {
	b := bytes.NewBuffer([]byte(packed))

	bucketLen, err := binary.ReadUvarint(b)
	if err != nil {
		log.Fatal(err)
	}

	bucket := make([]byte, bucketLen)
	b.Read(bucket)
	key := b.String()

	return string(bucket), key
}
