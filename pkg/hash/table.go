package hash

import (
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"os"

	pager "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/pager"
	utils "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/utils"
)

// HashTable definitions.
type HashTable struct {
	depth   int64
	buckets []int64 // Array of bucket page numbers
	pager   *pager.Pager
	rwlock  sync.RWMutex // Lock on the hash table index
}

// Returns a new HashTable.
func NewHashTable(pager *pager.Pager) (*HashTable, error) {
	depth := int64(2)
	buckets := make([]int64, powInt(2, depth))
	for i := range buckets {
		bucket, err := NewHashBucket(pager, depth)
		if err != nil {
			return nil, err
		}
		buckets[i] = bucket.page.GetPageNum()
		bucket.page.Put()
	}
	return &HashTable{depth: depth, buckets: buckets, pager: pager}, nil
}

// [CONCURRENCY] Grab a write lock on the hash table index
func (table *HashTable) WLock() {
	table.rwlock.Lock()
}

// [CONCURRENCY] Release a write lock on the hash table index
func (table *HashTable) WUnlock() {
	table.rwlock.Unlock()
}

// [CONCURRENCY] Grab a read lock on the hash table index
func (table *HashTable) RLock() {
	table.rwlock.RLock()
}

// [CONCURRENCY] Release a read lock on the hash table index
func (table *HashTable) RUnlock() {
	table.rwlock.RUnlock()
}

// Get depth.
func (table *HashTable) GetDepth() int64 {
	return table.depth
}

// Get bucket page numbers.
func (table *HashTable) GetBuckets() []int64 {
	return table.buckets
}

// Get pager.
func (table *HashTable) GetPager() *pager.Pager {
	return table.pager
}

// Finds the entry with the given key.
func (table *HashTable) Find(key int64) (utils.Entry, error) {
	// Hash the key.
	hash := Hasher(key, table.depth)
	fmt.Printf("hash is %v \n", hash)
	if hash < 0 || int(hash) >= len(table.buckets) {
		return nil, errors.New("not found")
	}
	// Get and lock the corresponding bucket.
	bucket, err := table.GetBucket(hash)
	if err != nil {
		return nil, err
	}
	fmt.Println("Bucket is: \n")
	bucket.Print(os.Stdout)
	defer bucket.page.Put()
	// Find the entry.
	entry, found := bucket.Find(key)
	if !found {
		return nil, errors.New("not found")
	}
	return entry, nil
}

// ExtendTable increases the global depth of the table by 1.
func (table *HashTable) ExtendTable() {
	table.depth = table.GetDepth() + 1
	table.buckets = append(table.buckets, table.buckets...)
}

// Split the given bucket into two, extending the table if necessary.
func (table *HashTable) Split(bucket *HashBucket, hash int64) error {
	// add to local depth, extending global depth if necessary
	// fmt.Println("Before split, bucket contents: \n")
	// bucket.Print(os.Stdout)
	bucket.updateDepth(bucket.depth + 1)
	
	// create new bucket and add to table
	new_bucket, err := NewHashBucket(table.pager, bucket.depth)
	if err != nil {
		return err
	}
	defer new_bucket.page.Put()

	if bucket.depth > table.depth {
		table.ExtendTable()
	}
	
	// redistribute keys between buckets
	MaxIndex := bucket.numKeys - 1
	second_hash := hash
	for i := MaxIndex; i >= 0; i-- {
		key := bucket.getKeyAt(i)
        value := bucket.getValueAt(i)
		
		newHash := Hasher(key, table.depth)

		if newHash != hash {
			second_hash = newHash
			new_bucket.Insert(key, value)
            bucket.Delete(key)
		}
	}
	if int(second_hash) > (len(table.buckets) - 1) {
		return errors.New("PROBLEM FOUND")
	}
	table.buckets[second_hash] = new_bucket.page.GetPageNum()
	return nil
}

// Inserts the given key-value pair, splits if necessary.
func (table *HashTable) Insert(key int64, value int64) error {
	hash := Hasher(key, table.depth)
	bucket, err := table.GetBucket(hash)
	if err != nil {
		return err
	}
	defer bucket.page.Put()
	should_split, err := bucket.Insert(key, value)
	if err != nil {
		return err
	}
	if should_split {
		table.Split(bucket, hash)
	}
	return nil
}

// Update the given key-value pair.
func (table *HashTable) Update(key int64, value int64) error {
	hash := Hasher(key, table.depth)
	bucket, err := table.GetBucket(hash)
	if err != nil {
		return err
	}
	defer bucket.page.Put()
	return bucket.Update(key, value)
}

// Delete the given key-value pair, does not coalesce.
func (table *HashTable) Delete(key int64) error {
	hash := Hasher(key, table.depth)
	bucket, err := table.GetBucket(hash)
	if err != nil {
		return err
	}
	defer bucket.page.Put()
	return bucket.Delete(key)
}

// Select all entries in this table.
func (table *HashTable) Select() ([]utils.Entry, error) {	
	slice := make([]utils.Entry, 0)
	for i := 0; i <= len(table.buckets) - 1; i ++ {
		bucket, err := table.GetBucket(int64(i))
		if err != nil {
			return nil, err
		}
		entries, err := bucket.Select()
		if err != nil {
			return nil, err
		}
		slice = append(slice, entries...)
	}
	return slice, nil
}

// Print out each bucket.
func (table *HashTable) Print(w io.Writer) {
	table.RLock()
	defer table.RUnlock()
	io.WriteString(w, "====\n")
	io.WriteString(w, fmt.Sprintf("global depth: %d\n", table.depth))
	for i := range table.buckets {
		io.WriteString(w, fmt.Sprintf("====\nbucket %d\n", i))
		bucket, err := table.GetBucket(int64(i))
		if err != nil {
			continue
		}
		bucket.Print(w)
		bucket.RUnlock()
		bucket.page.Put()
	}
	io.WriteString(w, "====\n")
}

// Print out a specific bucket.
func (table *HashTable) PrintPN(pn int, w io.Writer) {
	table.RLock()
	defer table.RUnlock()
	if int64(pn) >= table.pager.GetNumPages() {
		fmt.Println("out of bounds")
		return
	}
	bucket, err := table.GetBucketByPN(int64(pn))
	if err != nil {
		return
	}
	bucket.Print(w)
	bucket.RUnlock()
	bucket.page.Put()
}

// x^y
func powInt(x, y int64) int64 {
	return int64(math.Pow(float64(x), float64(y)))
}
