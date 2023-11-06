package query

import (
	bitset "github.com/bits-and-blooms/bitset"
	// hash "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/hash"
)

type BloomFilter struct {
	size int64
	bits *bitset.BitSet
}

// CreateFilter initializes a BloomFilter with the given size.
func CreateFilter(size int64) *BloomFilter {
	panic("function not yet implemented")
}

// Insert adds an element into the bloom filter.
func (filter *BloomFilter) Insert(key int64) {
	panic("function not yet implemented")
}

// Contains checks if the given key can be found in the bloom filter/
func (filter *BloomFilter) Contains(key int64) bool {
	panic("function not yet implemented")
}
