package rivet

import (
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/tylerb/is"
)

var D1 = []byte("data1")
var D2 = []byte("data2")
var D3 = []byte("data3")

const testDir = "test_dbs"

func TestMain(m *testing.M) {
	os.Mkdir(testDir, 0700)
	result := m.Run()
	os.RemoveAll(testDir)
	os.Exit(result)
}

func TestDB(t *testing.T) {
	is := is.New(t)
	db, err := New(randName())
	is.NotErr(err)

	db.SetBytes("key1", D1)
	is.Equal(db.GetBytes("key1"), D1)
	is.Nil(db.GetBytes("key2"))

	db.SetBytes("key1", []byte{})
	is.Equal(db.GetBytes("key1"), []byte{})
	is.NotNil(db.GetBytes("key1"))

	db.Delete("key1")
	is.Nil(db.GetBytes("key1"))
	db.Delete("key2")
}

func TestBucket(t *testing.T) {
	const key = "foo"

	is := is.New(t)

	db1, _ := New(randName())
	db2, _ := New(randName())
	db2.Bucket("b1")
	db3, _ := New(randName())
	db3.Bucket("b2")

	db1.SetBytes(key, D1)
	db2.SetBytes(key, D2)
	db3.SetBytes(key, D3)
	is.Equal(db1.GetBytes(key), D1)
	is.Equal(db2.GetBytes(key), D2)
	is.Equal(db3.GetBytes(key), D3)

	for _, db := range []*Rivet{db1, db2, db3} {
		is.NotNil(db.GetBytes(key))
		db.Delete(key)
		is.Nil(db.GetBytes(key))
	}
}

func TestMultiInstance(t *testing.T) {
	is := is.New(t)

	name := randName()
	db1, err := New(name)
	is.NotErr(err)
	db2, err := New(name)
	is.NotErr(err)

	db1.SetBytes("key1", D1)
	db2.SetBytes("key2", D2)
	is.Equal(db1.GetBytes("key2"), D2)
	is.Equal(db2.GetBytes("key1"), D1)
}

func TestStrings(t *testing.T) {
	is := is.New(t)

	db, _ := New(randName())

	db.Set("S1", "my string")
	is.Equal(db.Get("S1"), "my string")
	is.Equal(db.Get("S2"), "")
}

func TestInts(t *testing.T) {
	is := is.New(t)
	db, _ := New(randName())

	db.SetInt("I1", 42)
	is.Equal(db.GetInt("I1"), 42)
	db.SetInt("I2", -42)
	is.Equal(db.GetInt("I2"), -42)
	is.Equal(db.GetInt("I3"), 0)
}

func TestStruct(t *testing.T) {
	is := is.New(t)
	db, _ := New(randName())

	type ColorGroup struct {
		ID     int
		Name   string
		Colors []string
	}

	group := ColorGroup{
		ID:     1,
		Name:   "Reds",
		Colors: []string{"Crimson", "Red", "Ruby", "Maroon"},
	}

	db.SetData("colors", group)

	var g ColorGroup
	db.GetData("colors", &g)
	is.Equal(g, group)

	type M map[int]string
	m := M{4: "foo", 13: "bar"}
	var mo M
	db.SetData("map", m)
	db.GetData("map", &mo)
	is.Equal(m, mo)

	var i1 int = 5
	var i2 int
	db.SetData("int", &i1)
	db.GetData("int", &i2)
	is.Equal(i1, i2)

}

func TestKeys(t *testing.T) {
	is := is.New(t)
	db, _ := New(randName())

	db.SetBytes("c", D1)
	db.SetBytes("b", D2)
	db.SetBytes("a", D2)

	is.Equal(db.Keys(), []string{"a", "b", "c"})
}

func TestExpire(t *testing.T) {
	t.Skip()
	is := is.New(t)

	db, _ := New(randName())

	db.Set("foo", "bar")
	is.Equal(db.TTL("foo"), -1)
	db.Expire("foo", 2)
	is.Equal(db.TTL("foo"), 2)
	time.Sleep(2 * time.Second)
	is.Equal(db.TTL("foo"), 0)

	is.False(db.Exists("foo"))
}

func TestExists(t *testing.T) {
	is := is.New(t)
	db, _ := New(randName())

	is.False(db.Exists("foo"))
	db.Set("foo", "bar")
	is.True(db.Exists("foo"))
	db.Delete("foo")
	is.False(db.Exists("foo"))
}

func randName() string {
	rand.Seed(time.Now().UnixNano())
	v := rand.Uint64()
	return filepath.Join(testDir, strconv.FormatUint(v, 16))
}
