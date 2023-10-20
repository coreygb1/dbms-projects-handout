package query

import (
	"context"
	"os"
	"fmt"

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
	// start table and get first entry
	cursor, err := sourceTable.TableStart()
	if err != nil {
		return nil, "", err
	}

	entry, err := cursor.GetEntry()
	if err != nil {
		return nil, "", err
	}
	
	endBool := cursor.IsEnd()
	for !endBool {
		if useKey {
			err = tempIndex.Insert(entry.GetKey(), entry.GetValue())
		} else {
			err = tempIndex.Insert(entry.GetValue(), entry.GetKey())
		}
		if err != nil {
			return nil, "", err
		}
		endBool = cursor.StepForward()
	}
	fmt.Println("Hash Table: \n")
	tempIndex.GetTable().Print(os.Stdout)
	return tempIndex, dbName, nil
}

// sendResult attempts to send a single join result to the resultsChan channel as long as the errgroup hasn't been cancelled.
func sendResult(
	ctx context.Context,
	resultsChan chan EntryPair,
	result EntryPair,
) error {
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
) error {
	defer lBucket.GetPage().Put()
	defer rBucket.GetPage().Put()
	// Probe buckets.
	for i := int64(0); i < lBucket.GetNumKeys(); i++ {
		left_entry := lBucket.GetEntry(i)
		right_entry, match := rBucket.Find(left_entry.GetKey())
		if match {
			var return_left hash.HashEntry
			var return_right hash.HashEntry
			if !joinOnLeftKey {
				return_left.SetKey(left_entry.GetValue())
				return_left.SetValue(left_entry.GetKey())
			} else {
				return_left.SetKey(left_entry.GetKey())
				return_left.SetValue(left_entry.GetValue())
			}
			if !joinOnRightKey {
				return_right.SetKey(right_entry.GetValue())
				return_right.SetValue(right_entry.GetKey())
			} else {
				return_right.SetKey(right_entry.GetKey())
				return_right.SetValue(right_entry.GetValue())
			}
			sendResult(ctx, resultsChan, EntryPair{l: return_left, r: return_right})
		}
	}
	return nil
}

// Join leftTable on rightTable using Grace Hash Join.
func Join(
	ctx context.Context,
	leftTable db.Index,
	rightTable db.Index,
	joinOnLeftKey bool,
	joinOnRightKey bool,
) (chan EntryPair, context.Context, *errgroup.Group, func(), error) {
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
	cleanupCallback := func() {
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
	group, ctx := errgroup.WithContext(ctx)
	resultsChan := make(chan EntryPair, 1024)
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
