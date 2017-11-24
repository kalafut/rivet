## Project Status: In progress... Not ready for public consumption.

# Rivet
Rivet is a [boltdb](https://github.com/boltdb/bolt) wrapper to simplify common uses. Bolt is straightforward to use as
is, but I found myself frequently creating little helpers to marshal common data types. I also found creating the recommendation transactions for everything and the prevalence of `[]byte` conversions a bit tedious. 

The most advanced thing Rivet does is data expiration, and most of what it does is making transactions and marshalling data. There are other libraries that offer advanced indexing, querying, etc. I don't intend to grow Rivet into these areas.

The main design principle Rivet is that it is just a thin wrapper over Bolt with no surprises. Keys, buckets and data are all written verbatim as if you'd called Bolt directly. In fact, you can make Bolt calls right on the Rivet object—it embeds *bolt.DB. The only thing that's all all different is the presence of Rivet's private bucket rivet.InternalBucket. It is used for expiration tracking and some metadata.

### DB Creation
A new Rivet object is created with New.  Multiple Rivet objects can refered to the same underlying database simultanteously. This may simplify code that uses different buckets.

```
db, err := rivet.New("test.db")
db2, err := rivet.New("test.db") // OK!
```

### Buckets
Rivet objects refer to a single bucket at any time, and operations apply to that bucket. A default bucket (Rivet.DefaultBucket) will be used if no other is specified. Bucket names are specified as strings, not byte slices as in bolt.

```
db.SetBucket("upper")          // change bucket
db.SetBucket("upper", "lower") // nested buckets are supported
db.SetBucket()                 // back to default
```

### Storing and Retrieving Data
Rivet makes it set and get basic types and structures. Dedicated functions handle all marshalling of data and execution within a transaction. Key names are always strings. Retrieval will always succeed. If a key does not exist, the GetXXX() function will return the zero value for that type. Exist() may be used to check for key existence.

```
// setting
db.Set("my string", "val")
db.SetInt("my int", 42)
db.SetBytes("my bytes", []byte{1,2,3})
db.SetFloat("my float", Math.PI)
db.SetData("my data", myComplexType) // structs will be marshalled using JSON under the hood

// getting
sVar := db.Get("my string")
sInt := db.GetInt("my int")
db.GetInt("doesn't exist")   // 0 
db.Get("doesn't exist")      // ""
db.GetInt("my string")       // panic, or garbage, unmarshalling the wrong type
db.GetData("my data", &dataVar)
```

### Deleting and Expiration
Buckets and keys can be deleted explicitly. Keys can be set to expire (be deleted) after a specified duration. This expiration is done lazily at read time, and also as a background process to ensure keys never read again will be expired as well.

*TODO: Cost of expiration*

```
// manual deletion 
db.Delete("key")
db.DeleteBucket("bucket")           // delete bucket and any nested buckets
db.DeleteBucket("parent", "child")  // delete just a nested bucket

// expiration
db.Expire("foo", 2*time.Hour)       // Any duration. Negative will expire immediately
db.TTL("foo")                       // Check remaining time. May also indicate NoExpiration.
db.Set("foo", "bar")                // setting clears any expiration
```

### Misc
```
db.Keys()         // list all keys. Potentially expensive as it will check expiration of everything
db.Exists("foo")  // test for key existence
```

### Error Handling
Rivet functions will panic on bolt and other errors, by design. In most cases, errors from bolt are practically fatal. Similarly, no type information is stored, so the data marshalling done by Rivet assumes you're minding your types. If you try to write one type and read with another on the same key, expect panics or garbage data.

If you'd prefer to handle these errors, use Bolt directly. 




### Alternatives
- https://github.com/asdine/storm
