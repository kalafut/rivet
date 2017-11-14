package rivet

import (
	"os"
	"testing"
	"time"

	"github.com/tylerb/is"
)

var D1 = []byte("data1")
var D2 = []byte("data2")
var D3 = []byte("data3")

func TestDB(t *testing.T) {
	is := is.New(t)
	defer os.Remove("test_db")

	db, err := New("test_db")
	is.NotErr(err)

	db.SetBytes("key1", D1)
	is.Equal(db.GetBytes("key1"), D1)
	is.Nil(db.GetBytes("key2"))

	db.SetBytes("key1", []byte{})
	is.Equal(db.GetBytes("key1"), []byte{})
	is.NotNil(db.GetBytes("key1"))

	db.Del("key1")
	is.Nil(db.GetBytes("key1"))
	db.Del("key2")
}

func TestBucket(t *testing.T) {
	const key = "foo"

	is := is.New(t)
	defer os.Remove("test_db")

	db1, _ := New("test_db")
	db2, _ := New("test_db", "b1")
	db3, _ := New("test_db", "b2")

	db1.SetBytes(key, D1)
	db2.SetBytes(key, D2)
	db3.SetBytes(key, D3)
	is.Equal(db1.GetBytes(key), D1)
	is.Equal(db2.GetBytes(key), D2)
	is.Equal(db3.GetBytes(key), D3)

	for _, db := range []*Rivet{db1, db2, db3} {
		is.NotNil(db.GetBytes(key))
		db.Del(key)
		is.Nil(db.GetBytes(key))
	}
}

func TestMultiInstance(t *testing.T) {
	is := is.New(t)
	defer os.Remove("test_db")

	db1, err := New("test_db")
	is.NotErr(err)
	db2, err := New("test_db")
	is.NotErr(err)

	db1.SetBytes("key1", D1)
	db2.SetBytes("key2", D2)
	is.Equal(db1.GetBytes("key2"), D2)
	is.Equal(db2.GetBytes("key1"), D1)
}

func TestStrings(t *testing.T) {
	is := is.New(t)
	var s string
	var ok bool

	db, _ := New("test_db")
	defer db.Close()
	defer os.Remove("test_db")

	db.Set("S1", "my string")
	is.Equal(db.Get("S1"), "my string")
	is.Equal(db.Get("S2"), "")

	s, ok = db.GetOK("S1")
	is.Equal(s, "my string")
	is.True(ok)
	s, ok = db.GetOK("S2")
	is.Equal(s, "")
	is.False(ok)
}

func TestStruct(t *testing.T) {
	is := is.New(t)

	db, _ := New("test_db")
	defer db.Close()
	defer os.Remove("test_db")

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

	db.SetJ("colors", group)

	var g ColorGroup
	db.GetJ("colors", &g)
	is.Equal(g, group)

	type M map[int]string
	m := M{4: "foo", 13: "bar"}
	var mo M
	db.SetJ("map", m)
	db.GetJ("map", &mo)
	is.Equal(m, mo)

	var i1 int = 5
	var i2 int
	db.SetJ("int", &i1)
	db.GetJ("int", &i2)
	is.Equal(i1, i2)

}

func TestKeys(t *testing.T) {
	is := is.New(t)

	db, _ := New("test_db")
	defer db.Close()
	defer os.Remove("test_db")

	db.SetBytes("c", D1)
	db.SetBytes("b", D2)
	db.SetBytes("a", D2)

	is.Equal(db.Keys(), []string{"a", "b", "c"})
}

func TestExpires(t *testing.T) {
	is := is.New(t)

	db, _ := New("test_db")
	defer db.Close()
	defer os.Remove("test_db")

	db.SetX("foo", "bar", 2)
	is.Equal(db.Get("foo"), "bar")
	time.Sleep(2 * time.Second)
	//_, ok := db.GetOK("foo")
	//is.False(ok)
}

//func TestClose(t *testing.T) {
//	is := is.New(t)
//	defer os.Remove("test_db")
//
//	db1, _ := New("test_db")
//	db1.Set("foo", D1)
//	is.Equal(db1.Get("foo"), D1)
//
//	db2, _ := New("test_db", "bucket")
//	db2.Set("foo", D2)
//	is.Equal(db2.Get("foo"), D2)
//
//	db1.Close()
//	is.Nil(db1.Get("foo"))
//	is.Nil(db2.Get("foo"))
//}
