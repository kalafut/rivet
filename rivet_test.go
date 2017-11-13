package rivet

import (
	"os"
	"testing"

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

	db.Set("key1", D1)
	is.Equal(db.Get("key1"), D1)
	is.Nil(db.Get("key2"))

	db.Del("key1")
	is.Nil(db.Get("key1"))
	db.Del("key2")
}

func TestBucket(t *testing.T) {
	const key = "foo"

	is := is.New(t)
	defer os.Remove("test_db")

	db1, _ := New("test_db")
	db2, _ := New("test_db", "b1")
	db3, _ := New("test_db", "b2")

	db1.Set(key, D1)
	db2.Set(key, D2)
	db3.Set(key, D3)
	is.Equal(db1.Get(key), D1)
	is.Equal(db2.Get(key), D2)
	is.Equal(db3.Get(key), D3)

	for _, db := range []*Rivet{db1, db2, db3} {
		is.NotNil(db.Get(key))
		db.Del(key)
		is.Nil(db.Get(key))
	}
}

func TestMultiInstance(t *testing.T) {
	is := is.New(t)
	defer os.Remove("test_db")

	db1, err := New("test_db")
	is.NotErr(err)
	db2, err := New("test_db")
	is.NotErr(err)

	db1.Set("key1", D1)
	db2.Set("key2", D2)
	is.Equal(db1.Get("key2"), D2)
	is.Equal(db2.Get("key1"), D1)
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

	db.Set("c", D1)
	db.Set("b", D2)
	db.Set("a", D2)

	is.Equal(db.Keys(), []string{"a", "b", "c"})
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
