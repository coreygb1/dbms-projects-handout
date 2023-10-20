package query

import (
	bitset "github.com/bits-and-blooms/bitset"
	hash "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/hash"
)

type BloomFilter struct {
	size int64
	bits *bitset.BitSet
}

// CreateFilter initializes a BloomFilter with the given size.
func CreateFilter(size int64) *BloomFilter {
	bloom := BloomFilter{
        size: size,  // Set the 'size' field to your desired value
        bits: bitset.New(uint(size)), // Initialize the 'bits' field with a new BitSet
    }
	return &bloom
}

// Insert adds an element into the bloom filter.
func (filter *BloomFilter) Insert(key int64) {
	filter.bits.Set(hash.XxHasher(key, filter.size))
	filter.bits.Set(hash.MurmurHasher(key, filter.size))
}

// Contains checks if the given key can be found in the bloom filter/
func (filter *BloomFilter) Contains(key int64) bool {
	test1 := filter.bits.Test(hash.XxHasher(key, filter.size))
	test2 := filter.bits.Test(hash.MurmurHasher(key, filter.size))
	return (test1 && test2)
}
