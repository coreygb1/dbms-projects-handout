package btree

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"errors"

	pager "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/pager"
)

// Split is a supporting data structure to propagate keys up our B+ tree.
type Split struct {
	isSplit bool  // A flag that's set if a split occurs.
	key     int64 // The key to promote.
	leftPN  int64 // The pagenumber for the left node.
	rightPN int64 // The pagenumber for the right node.
	err     error // Used to propagate errors upwards.
}

// Node defines a common interface for leaf and internal nodes.
type Node interface {
	// Interface for main node functions.
	search(int64) int64
	insert(int64, int64, bool) Split
	delete(int64)
	get(int64) (int64, bool)

	// Interface for helper functions.
	keyToNodeEntry(int64) (*LeafNode, int64, error)
	printNode(io.Writer, string, string)
	getPage() *pager.Page
	getNodeType() NodeType
}

/////////////////////////////////////////////////////////////////////////////
///////////////////////////// Leaf Node Methods /////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// search returns the first index where key >= given key.
// If no key satisfies this condition, returns numKeys.
func (node *LeafNode) search(key int64) int64 {
	// Binary search for the key.
	minIndex := sort.Search(
		int(node.numKeys),
		func(idx int) bool {
			return node.getKeyAt(int64(idx)) >= key
		},
	)
	return int64(minIndex)
}

// insert finds the appropriate place in a leaf node to insert a new tuple.
// if update is true, allow overwriting existing keys. else, error.
func (node *LeafNode) insert(key int64, value int64, update bool) Split {
	index := node.search(key)
	if (key == node.getKeyAt(int64(index))) && update == false {
		return Split{err: errors.New("Duplicate keys cannot be updated")}
	} else {
		for i := index; i < node.numKeys-1; i++ {
			node.updateKeyAt(i+1, node.getKeyAt(i)) // make sure no error when range too large
			node.updateValueAt(i+1, node.getValueAt(i))
		}
		node.updateKeyAt(index, key)
		node.updateValueAt(index, value)
		node.updateNumKeys(node.numKeys + 1)
	}
	if node.numKeys >= ENTRIES_PER_LEAF_NODE {
		return node.split()
	} else {
		return Split{isSplit: false}
	}

}

// delete removes a given tuple from the leaf node, if the given key exists.
func (node *LeafNode) delete(key int64) {
	needNodeIndex := node.search(key) // Search the key
	// Case if the key is not in the node on the last one
	if needNodeIndex >= node.numKeys && node.getKeyAt(needNodeIndex) != key {
		// Not in here!!
		return
	}
	// Shift the keys and values to the left
	for i := needNodeIndex; i < node.numKeys-1; i++ {
		node.updateKeyAt(i, node.getKeyAt(i+1))
		node.updateValueAt(i, node.getValueAt(i+1))
	}
	// Update the number of keys
	node.updateNumKeys(node.numKeys - 1)
}

// split is a helper function to split a leaf node, then propagate the split upwards.
func (node *LeafNode) split() Split {
	leaf, err := createLeafNode(node.page.GetPager())
	if err != nil {
		return Split{
			err: err,
		}
	}
	defer leaf.getPage().Put() // check if i'm using put correctly
	medianIndex := (node.numKeys + 1) / 2

	// set new right siblings
	leaf.setRightSibling(node.rightSiblingPN)
	node.setRightSibling(leaf.getPage().GetPageNum())
	
	// fill in the new leaf entries
	leaf.updateNumKeys(medianIndex) // one less than the median
	for i := medianIndex; i < node.numKeys; i++ {
		entry := BTreeEntry{
			key:   node.getKeyAt(i),
			value: node.getValueAt(i),
		}
		leaf.modifyEntry(i-medianIndex, entry)
	}
	

	// "delete" old leaf overflow entries by changing numKeys
	node.updateNumKeys(node.numKeys - medianIndex) 

	// return split
	return Split{
		isSplit: true,
	key: leaf.getKeyAt(0),
	leftPN: node.getPage().GetPageNum(),
	rightPN: leaf.getPage().GetPageNum(),
	err: nil,
	}
}

// get returns the value associated with a given key from the leaf node.
func (node *LeafNode) get(key int64) (value int64, found bool) {
	// Find index.
	index := node.search(key)
	if index >= node.numKeys || node.getKeyAt(index) != key {
		// Thank you Mario! But our key is in another castle!
		return 0, false
	}
	entry := node.getEntry(index)
	return entry.GetValue(), true
}

// keyToNodeEntry is a helper function to create cursors that point to a given index within a leaf node.
func (node *LeafNode) keyToNodeEntry(key int64) (*LeafNode, int64, error) {
	return node, node.search(key), nil
}

// printNode pretty prints our leaf node.
func (node *LeafNode) printNode(w io.Writer, firstPrefix string, prefix string) {
	// Format header data.
	var nodeType string = "Leaf"
	var isRoot string
	if node.isRoot() {
		isRoot = " (root)"
	}
	numKeys := strconv.Itoa(int(node.numKeys))
	// Print header data.
	io.WriteString(w, fmt.Sprintf("%v[%v] %v%v size: %v\n",
		firstPrefix, node.page.GetPageNum(), nodeType, isRoot, numKeys))
	// Print entries.
	for cellnum := int64(0); cellnum < node.numKeys; cellnum++ {
		entry := node.getEntry(cellnum)
		io.WriteString(w, fmt.Sprintf("%v |--> (%v, %v)\n",
			prefix, entry.GetKey(), entry.GetValue()))
	}
	if node.rightSiblingPN > 0 {
		io.WriteString(w, fmt.Sprintf("%v |--+\n", prefix))
		io.WriteString(w, fmt.Sprintf("%v    | right sibling @ [%v]\n",
			prefix, node.rightSiblingPN))
		io.WriteString(w, fmt.Sprintf("%v    v\n", prefix))
	}
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////// Internal Node Methods ///////////////////////////
/////////////////////////////////////////////////////////////////////////////

// search returns the first index where key > given key.
// If no such index exists, it returns numKeys.
func (node *InternalNode) search(key int64) int64 {
	// Binary search for the key.
	minIndex := sort.Search(
		int(node.numKeys),
		func(idx int) bool {
			return node.getKeyAt(int64(idx)) > key
		},
	)
	return int64(minIndex)
}

// insert finds the appropriate place in a leaf node to insert a new tuple.
func (node *InternalNode) insert(key int64, value int64, update bool) Split {
	// Insert the entry into the appropriate child node. Use getChildAt for the indexing
	childIdx := node.search(key)
	child, err := node.getChildAt(childIdx)
	if err != nil {
		return Split{err: err}
	}
	defer child.getPage().Put()
	// Insert value into the child.
	result := child.insert(key, value, update)
	// Insert a new key into our node if necessary.
	if result.isSplit {
		split := node.insertSplit(result)
		return split
	}
	return Split{err: result.err}
}

// insertSplit inserts a split result into an internal node.
// If this insertion results in another split, the split is cascaded upwards.
func (node *InternalNode) insertSplit(split Split) Split {

	index := node.search(split.key)
	for i := index; i < node.numKeys-1; i++ {
		node.updateKeyAt(i+1, node.getKeyAt(i)) 
		node.updatePNAt(i+2, node.getPNAt(i+1)) 
	}
	node.updateKeyAt(index, split.key)
	node.updatePNAt(i, split.leftPN) 
	node.updatePNAt(i+1, split.rightPN)
	node.updateNumKeys(node.numKeys + 1)
	
	if node.numKeys >= KEYS_PER_INTERNAL_NODE {
		return node.split()
	} else {
		return Split{isSplit: false}
	}
}

// delete removes a given tuple from the leaf node, if the given key exists.
func (node *InternalNode) delete(key int64) {
	// Get child.
	childIdx := node.search(key)
	child, err := node.getChildAt(childIdx)
	if err != nil {
		return
	}
	defer child.getPage().Put()
	// Delete from child.
	child.delete(key)
}

// split is a helper function that splits an internal node, then propagates the split upwards.
func (node *InternalNode) split() Split {
	intern, err := createInternalNode(node.page.GetPager())
	if err != nil {
		return Split{
			err: err,
		}
	}
	defer intern.getPage().Put() // check if i'm using put correctly
	medianIndex := (node.numKeys + 1) / 2
	
	// fill in the new leaf entries
	intern.updateNumKeys(medianIndex) // one less than the median
	for i := medianIndex; i < node.numKeys; i++ {
		intern.updateKeyAt(i-medianIndex, node.getKeyAt(i))
	}
	
	// "delete" old node overflow entries by changing numKeys
	node.updateNumKeys(node.numKeys - medianIndex) 

	// return split
	return Split{
		isSplit: true,
	key: intern.getKeyAt(0),
	leftPN: node.getPage().GetPageNum(),
	rightPN: intern.getPage().GetPageNum(),
	err: nil,
	}
}

// get returns the value associated with a given key from the leaf node.
func (node *InternalNode) get(key int64) (value int64, found bool) {
	// Find the child.
	childIdx := node.search(key)
	child, err := node.getChildAt(childIdx)
	if err != nil {
		return 0, false
	}
	node.initChild(child)
	defer child.getPage().Put()
	return child.get(key)
}

// keyToNodeEntry is a helper function to create cursors that point to a given index within a leaf node.
func (node *InternalNode) keyToNodeEntry(key int64) (*LeafNode, int64, error) {
	index := node.search(key)
	child, err := node.getChildAt(index)
	if err != nil {
		return &LeafNode{}, 0, err
	}
	defer child.getPage().Put()
	return child.keyToNodeEntry(key)
}

// printNode pretty prints our internal node.
func (node *InternalNode) printNode(w io.Writer, firstPrefix string, prefix string) {
	// Format header data.
	var nodeType string = "Internal"
	var isRoot string
	if node.isRoot() {
		isRoot = " (root)"
	}
	numKeys := strconv.Itoa(int(node.numKeys + 1))
	// Print header data.
	io.WriteString(w, fmt.Sprintf("%v[%v] %v%v size: %v\n",
		firstPrefix, node.page.GetPageNum(), nodeType, isRoot, numKeys))
	// Print entries.
	nextFirstPrefix := prefix + " |--> "
	nextPrefix := prefix + " |    "
	for idx := int64(0); idx <= node.numKeys; idx++ {
		io.WriteString(w, fmt.Sprintf("%v\n", nextPrefix))
		child, err := node.getChildAt(idx)
		if err != nil {
			return
		}
		defer child.getPage().Put()
		child.printNode(w, nextFirstPrefix, nextPrefix)
		if idx != node.numKeys {
			io.WriteString(w, fmt.Sprintf("\n%v[KEY] %v\n", nextPrefix, node.getKeyAt(idx)))
		}
	}
}
