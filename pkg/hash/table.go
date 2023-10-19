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
	if hash < 0 || int(hash) >= len(table.buckets) {
		return nil, errors.New("not found")
	}
	// Get and lock the corresponding bucket.
	bucket, err := table.GetBucket(hash)
	if err != nil {
		return nil, err
	}
	defer bucket.page.Put()
	// Find the entry.
	entry, found := bucket.Find(key)
	if !found {
		fmt.Printf("Not found bucket contents:\n")
    	bucket.Print(os.Stdout) // Printing the old bucket's contents
		fmt.Printf(" \n Looking for key: %v \n", key)
		fmt.Printf(" Hash: %v \n", hash)
		bucket.Print(os.Stdout)
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
	fmt.Printf("\n Old bucket before split:\n")
    bucket.Print(os.Stdout) // Printing the old bucket's contents
	
	// create new bucket and add to table
	new_bucket, err := NewHashBucket(table.pager, bucket.depth)
	if err != nil {
		return err
	}
	defer new_bucket.page.Put()

	bucket.updateDepth(bucket.depth + 1)
	new_bucket.depth = bucket.depth

	if bucket.depth > table.depth {
		table.ExtendTable()
		fmt.Printf("\n EXTEND TABLE \n")
	}
	
	update_size := int64(math.Pow(2, float64(bucket.depth)))
	// redistribute keys between buckets
	MaxIndex := bucket.numKeys - 1
	// second_hash := hash
	for i := MaxIndex; i >= 0; i-- {
		key := bucket.getKeyAt(i)
        value := bucket.getValueAt(i)
		newHash := Hasher(key, bucket.depth) // bucket depth
		fmt.Printf("KV pair: %v, %v \n", key, value)
		fmt.Printf("hash: %v ", hash)
		fmt.Printf("New hash: %v \n", newHash)
		if (newHash % update_size) - update_size/2 >= 0 {
			// second_hash = newHash
			new_bucket.Insert(key, value)
            bucket.Delete(key)
		}
	}
	
	table_size := int64(math.Pow(2, float64(table.depth))) - 1
	starting_num := hash % update_size
	if starting_num - update_size/2 >= 0 {
		starting_num -= update_size/2
	}

	fmt.Printf("update size: %v \n", update_size)
	fmt.Printf("table size: %v \n", table_size)
	fmt.Printf("starting num: %v \n", starting_num)
	for i:= starting_num; i <= table_size; i += update_size {
		fmt.Printf("adding old bucket to table at %v \n", i)
		table.buckets[i] = bucket.page.GetPageNum()
		fmt.Printf("adding new bucket to table at %v \n", i + update_size/2)
		table.buckets[i + update_size/2] = new_bucket.page.GetPageNum()
	}
	
	// table.buckets[second_hash] = new_bucket.page.GetPageNum()
	fmt.Printf("Old bucket contents:\n")
    bucket.Print(os.Stdout) // Printing the old bucket's contents
    fmt.Printf("New bucket contents:\n")
    new_bucket.Print(os.Stdout) // Printing the new bucket's contents
	fmt.Printf("size of table: %v \n", len(table.buckets))
	fmt.Printf("table depth: %v \n", table.depth)
	// fmt.Printf("old bucket position: %v \n", hash)
	// fmt.Printf("(2^(bucket.depth))/2: %v \n", math.Pow(2, float64(bucket.depth)) / 2)
	// fmt.Printf("new bucket position: %v \n", new_hash)
	
	

	if new_bucket.numKeys == 0 {
		fmt.Printf("\n \n \n Split old bucket \n")
		table.Split(bucket, starting_num)
	}
	if bucket.numKeys == 0 {
		fmt.Printf("\n \n \nSplit new bucket \n")
		table.Split(new_bucket, (starting_num + update_size/2))
	}
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
