package rivet

import (
	"strconv"
	"testing"
	"time"

	"github.com/tylerb/is"
)

func TestPackeBucketKey(t *testing.T) {
	is := is.New(t)

	packed := packBucketKey("bucket1", "key1")
	bucket, key := unpackBucketKey(packed)
	is.Equal(bucket, "bucket1")
	is.Equal(key, "key1")
}

func TestTTL(t *testing.T) {
	is := is.New(t)
	db, _ := New(randName())

	db.Set("foo", "bar")
	db.Expire("foo", 2*time.Second)
	is.True(durationsEqual(db.TTL("foo"), 2*time.Second))
	db.Expire("foo", -5*time.Second)
	is.Equal(db.TTL("foo"), KeyNotFound)

	db.Set("no expiration", "bar")
	is.Equal(db.TTL("no expiration"), NoExpiration)

	is.Equal(db.TTL("no such key"), KeyNotFound)
	db.Expire("no such key", 2)
	is.Equal(db.TTL("no such key"), KeyNotFound)
}

func durationsEqual(a, b time.Duration) bool {
	eps := 50 * time.Millisecond
	diff := a - b //.Sub(b)
	return -eps <= diff && diff <= eps
}

func TestExpireCore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	is := is.New(t)

	name := randName()
	db1, _ := New(name)
	db2, _ := New(name, "bucket")

	db1.Set("foo", "bar")
	db2.Set("baz", "bar")
	db1.Expire("foo", 1*time.Second)
	db2.Expire("baz", 3*time.Second)
	is.Equal(len(db1.Keys()), 1)
	is.Equal(len(db2.Keys()), 1)
	time.Sleep(3500 * time.Millisecond)
	is.Equal(len(db1.Keys()), 0)
	is.Equal(len(db2.Keys()), 0)

	db1.Set("foo", "bar")
	db1.Expire("foo", 1*time.Second)
	is.True(durationsEqual(db1.TTL("foo"), 1*time.Second))
	db1.Set("foo", "bar")
	is.Equal(db1.TTL("foo"), NoExpiration)

	// Big
	db1.Del("foo")
	for i := 0; i < 1000; i++ {
		s := strconv.Itoa(i)
		db1.SetX(s, s, -1*time.Second)
	}
	db1.SetX("300", "yay", 500*time.Second)
	is.Equal(db1.Keys(), []string{"300"})

}
