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
func CreateFilter(size int64) (bf *BloomFilter) {
	/* SOLUTION {{{ */
	return &BloomFilter{
		size: size,
		bits: bitset.New(uint(size)),
	}
	/* SOLUTION }}} */
}

// Insert adds an element into the bloom filter.
func (filter *BloomFilter) Insert(key int64) {
	/* SOLUTION {{{ */
	filter.bits.Set(hash.XxHasher(key, filter.size))
	filter.bits.Set(hash.MurmurHasher(key, filter.size))
	/* SOLUTION }}} */
}

// Contains checks if the given key can be found in the bloom filter/
func (filter *BloomFilter) Contains(key int64) (contains bool) {
	/* SOLUTION {{{ */
	return (filter.bits.Test(hash.XxHasher(key, filter.size)) &&
		filter.bits.Test(hash.MurmurHasher(key, filter.size)))
	/* SOLUTION }}} */
}

