package query

import (
	"context"
	"os"

	db "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/db"
	hash "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/hash"
	utils "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/utils"

	errgroup "golang.org/x/sync/errgroup"
)

var DEFAULT_FILTER_SIZE int64 = 1024

// Entry pair struct - output of a join.
type EntryPair struct {
	l utils.Entry
	r utils.Entry
}

// Int pair struct - to keep track of seen bucket pairs.
type pair struct {
	l int64
	r int64
}

// buildHashIndex constructs a temporary hash table for all the entries in the given sourceTable.
func buildHashIndex(
	sourceTable db.Index,
	useKey bool,
) (tempIndex *hash.HashIndex, dbName string, err error) {
	// Get a temporary db file.
	dbName, err = db.GetTempDB()
	if err != nil {
		return nil, "", err
	}
	// Init the temporary hash table.
	tempIndex, err = hash.OpenTable(dbName)
	if err != nil {
		return nil, "", err
	}
	// Build the hash index.
	/* SOLUTION {{{ */
	// Get the cursor and load the hash table.
	cursor, err := sourceTable.TableStart()
	if err != nil {
		return nil, "", err
	}
	// Loop through all entries.
	for {
		if !cursor.IsEnd() {
			val, err := cursor.GetEntry()
			if err != nil {
				return nil, "", err
			}
			// Swap keys and values if needed, this needs to be swapped back later.
			if useKey {
				tempIndex.Insert(val.GetKey(), val.GetValue())
			} else {
				tempIndex.Insert(val.GetValue(), val.GetKey())
			}
		}
		if cursor.StepForward() {
			break
		}
	}
	return tempIndex, dbName, nil
	/* SOLUTION }}} */
}

// sendResult attempts to send a single join result to the resultsChan channel as long as the errgroup hasn't been cancelled.
func sendResult(
	ctx context.Context,
	resultsChan chan EntryPair,
	result EntryPair,
) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case resultsChan <- result:
		return nil
	}
}

// See which entries in rBucket have a match in lBucket.
func probeBuckets(
	ctx context.Context,
	resultsChan chan EntryPair,
	lBucket *hash.HashBucket,
	rBucket *hash.HashBucket,
	joinOnLeftKey bool,
	joinOnRightKey bool,
) (err error) {
	defer lBucket.GetPage().Put()
	defer rBucket.GetPage().Put()
	// Probe buckets.
	/* SOLUTION {{{ */
	// Get bucket entries.
	lBucketEntries, err := lBucket.Select()
	if err != nil {
		return err
	}
	rBucketEntries, err := rBucket.Select()
	if err != nil {
		return err
	}
	// Set up the bloom filter.
	filter := CreateFilter(DEFAULT_FILTER_SIZE)
	for _, rEntry := range rBucketEntries {
		filter.Insert(rEntry.GetKey())
	}
	for _, lEntry := range lBucketEntries {
		lMatchKey := lEntry.GetKey()
		// Check the bloom filter first.
		if !filter.Contains(lMatchKey) {
			continue
		}
		// Check all entries if the key is in the filter.
		for _, rEntry := range rBucketEntries {
			rMatchKey := rEntry.GetKey()
			if lMatchKey == rMatchKey {
				// Swap keys and values as needed.
				var lResult, rResult hash.HashEntry
				if joinOnLeftKey {
					lResult.SetKey(lEntry.GetKey())
					lResult.SetValue(lEntry.GetValue())
				} else {
					lResult.SetKey(lEntry.GetValue())
					lResult.SetValue(lEntry.GetKey())
				}
				if joinOnRightKey {
					rResult.SetKey(rEntry.GetKey())
					rResult.SetValue(rEntry.GetValue())
				} else {
					rResult.SetKey(rEntry.GetValue())
					rResult.SetValue(rEntry.GetKey())
				}
				err = sendResult(ctx, resultsChan, EntryPair{l: lResult, r: rResult})
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
	/* SOLUTION }}} */
}

// Join leftTable on rightTable using Grace Hash Join.
func Join(
	ctx context.Context,
	leftTable db.Index,
	rightTable db.Index,
	joinOnLeftKey bool,
	joinOnRightKey bool,
) (resultsChan chan EntryPair, ctxt context.Context, group *errgroup.Group, cleanupCallback func(), err error) {
	leftHashIndex, leftDbName, err := buildHashIndex(leftTable, joinOnLeftKey)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	rightHashIndex, rightDbName, err := buildHashIndex(rightTable, joinOnRightKey)
	if err != nil {
		os.Remove(leftDbName)
		os.Remove(leftDbName + ".meta")
		return nil, nil, nil, nil, err
	}
	cleanupCallback = func() {
		os.Remove(leftDbName)
		os.Remove(leftDbName + ".meta")
		os.Remove(rightDbName)
		os.Remove(rightDbName + ".meta")
	}
	// Make both hash indices the same global size.
	leftHashTable := leftHashIndex.GetTable()
	rightHashTable := rightHashIndex.GetTable()
	for leftHashTable.GetDepth() != rightHashTable.GetDepth() {
		if leftHashTable.GetDepth() < rightHashTable.GetDepth() {
			// Split the left table
			leftHashTable.ExtendTable()
		} else {
			// Split the right table
			rightHashTable.ExtendTable()
		}
	}
	// Probe phase: match buckets to buckets and emit entries that match.
	group, ctx = errgroup.WithContext(ctx)
	resultsChan = make(chan EntryPair, 1024)
	// Iterate through hash buckets, keeping track of pairs we've seen before.
	leftBuckets := leftHashTable.GetBuckets()
	rightBuckets := rightHashTable.GetBuckets()
	seenList := make(map[pair]bool)
	for i, lBucketPN := range leftBuckets {
		rBucketPN := rightBuckets[i]
		bucketPair := pair{l: lBucketPN, r: rBucketPN}
		if _, seen := seenList[bucketPair]; seen {
			continue
		}
		seenList[bucketPair] = true

		lBucket, err := leftHashTable.GetBucketByPN(lBucketPN)
		if err != nil {
			return nil, nil, nil, cleanupCallback, err
		}
		rBucket, err := rightHashTable.GetBucketByPN(rBucketPN)
		if err != nil {
			lBucket.GetPage().Put()
			return nil, nil, nil, cleanupCallback, err
		}
		group.Go(func() error {
			return probeBuckets(ctx, resultsChan, lBucket, rBucket, joinOnLeftKey, joinOnRightKey)
		})
	}
	return resultsChan, ctx, group, cleanupCallback, nil
}