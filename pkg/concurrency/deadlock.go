package concurrency

import (
	"errors"
	"sync"
)

// Graph.
type Graph struct {
	edges []Edge
	lock  sync.RWMutex
}

// Edge.
type Edge struct {
	from *Transaction
	to   *Transaction
}

// Grab a write lock on the graph
func (g *Graph) WLock() {
	g.lock.Lock()
}

// Release the write lock on the graph
func (g *Graph) WUnlock() {
	g.lock.Unlock()
}

// Grab a read lock on the graph
func (g *Graph) RLock() {
	g.lock.RLock()
}

// Release the write lock on the graph
func (g *Graph) RUnlock() {
	g.lock.RUnlock()
}

// Construct a new graph.
func NewGraph() *Graph {
	return &Graph{edges: make([]Edge, 0)}
}

// Add an edge from `from` to `to`. Logically, `from` waits for `to`.
func (g *Graph) AddEdge(from *Transaction, to *Transaction) {
	g.WLock()
	defer g.WUnlock()
	g.edges = append(g.edges, Edge{from: from, to: to})
}

// Remove an edge. Only removes one of these edges if multiple copies exist.
func (g *Graph) RemoveEdge(from *Transaction, to *Transaction) error {
	g.WLock()
	defer g.WUnlock()
	toRemove := Edge{from: from, to: to}
	for i, e := range g.edges {
		if e == toRemove {
			g.edges = removeEdge(g.edges, i)
			return nil
		}
	}
	return errors.New("edge not found")
}

/*
  - We want to create the graph to detect the deadlocks. This function will be used for
    checking the lock in Transaction Manager.

    1. Get all the transaction to the graph
    2. Construct union-find array
    3. Iterate through edges, applying DFS

    Return : true if a cycle exists; false otherwise.

*
*/
// func (g *Graph) DetectCycle() bool {
// 	g.RLock()
// 	defer g.RUnlock()
	
// 	var seen []*Transaction
// 	var check_txn *Transaction
// 	var cycle bool

// 	// for each edge, run dfs
// 	for _,e := range g.edges {
// 		check_txn = e.from
// 		if !contains(seen, check_txn) {
// 			seen = append(seen, check_txn)
// 			cycle = dfs(g, check_txn, seen)
// 		}
// 		if cycle {
// 			return true
// 		}
// 	}
// 	return false
// }

func (g *Graph) DetectCycle() bool {
	g.RLock()
	defer g.RUnlock()

	visit := make(map[*Transaction]bool)

	for _, edges := range g.edges {
		for v := range visit {
			delete(visit, v)
		}
		seen := []*Transaction{}
		
		for t := range visit {
			seen = append(seen, t)
		}
		if dfs(g, edges.from, seen) {
			return true
		}
	}

	return false
}

func (g *Graph) DetectCycle() bool {
	g.RLock()
	defer g.RUnlock()

	seen := []*Transaction{}
	for _, edges := range g.edges {
		if dfs(g, edges.from, seen) {
			return true
		}
	}
	return false
}


func contains(transactions []*Transaction, target *Transaction) bool {
    for _, txn := range transactions {
        if txn == target {
            return true
        }
    }
    return false
}

func dfs(g *Graph, from *Transaction, seen []*Transaction) bool {
	// Go through each edge.
	for _, e := range g.edges {
		// If there is an edge from here to elsewhere,
		if e.from == from {
			// Check if it creates a cycle.
			for _, s := range seen {
				if e.to == s {
					return true
				}
			}
			// Otherwise, run dfs on it.
			return dfs(g, e.to, append(seen, e.from))
		}
	}
	return false
}

// Remove the element at index `i` from `l`.
func removeEdge(l []Edge, i int) []Edge {
	l[i] = l[len(l)-1]
	return l[:len(l)-1]
}
