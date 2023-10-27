package hash

func IsHash(index *HashIndex) (bool, error) {
    table := index.GetTable()
    buckets := table.GetBuckets()
    for _, pn := range buckets {
        // Get bucket
        bucket, err := table.GetBucketByPN(pn)
        if err != nil {
            return false, err
        }
        d := bucket.GetDepth()
        // Get all entries
        entries, err := bucket.Select()
        if err != nil {
            bucket.GetPage().Put()
            return false, err
        }
        // Check that all entries should hash to this bucket.
        for _, e := range entries {
            key := e.GetKey()
            hash := Hasher(key, d)
            if pn != table.buckets[hash] {
                bucket.GetPage().Put()
                return false, nil
            }
        }
        bucket.GetPage().Put()
    }
    return true, nil
}