package concurrency

import (
	"errors"
	"sync"

	db "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/db"
	uuid "github.com/google/uuid"
)

// Each client can have a transaction running. Each transaction has a list of locked resources.
type Transaction struct {
	clientId  uuid.UUID
	resources map[Resource]LockType
	lock      sync.RWMutex
}

// Grab a write lock on the tx
func (t *Transaction) WLock() {
	t.lock.Lock()
}

// Release the write lock on the tx
func (t *Transaction) WUnlock() {
	t.lock.Unlock()
}

// Grab a read lock on the tx
func (t *Transaction) RLock() {
	t.lock.RLock()
}

// Release the write lock on the tx
func (t *Transaction) RUnlock() {
	t.lock.RUnlock()
}

// Get the transaction id.
func (t *Transaction) GetClientID() uuid.UUID {
	return t.clientId
}

// Get the transaction's resources.
func (t *Transaction) GetResources() map[Resource]LockType {
	return t.resources
}

// Transaction Manager manages all of the transactions on a server.
type TransactionManager struct {
	lm           *LockManager
	tmMtx        sync.RWMutex
	pGraph       *Graph
	transactions map[uuid.UUID]*Transaction
}

// Get a pointer to a new transaction manager.
func NewTransactionManager(lm *LockManager) *TransactionManager {
	return &TransactionManager{lm: lm, pGraph: NewGraph(), transactions: make(map[uuid.UUID]*Transaction)}
}

// Get the transactions.
func (tm *TransactionManager) GetLockManager() *LockManager {
	return tm.lm
}

// Get the transactions.
func (tm *TransactionManager) GetTransactions() map[uuid.UUID]*Transaction {
	return tm.transactions
}

// Get a particular transaction.
func (tm *TransactionManager) GetTransaction(clientId uuid.UUID) (*Transaction, bool) {
	tm.tmMtx.RLock()
	defer tm.tmMtx.RUnlock()
	t, found := tm.transactions[clientId]
	return t, found
}

// Begin a transaction for the given client; error if already began.
func (tm *TransactionManager) Begin(clientId uuid.UUID) error {
	tm.tmMtx.Lock()
	defer tm.tmMtx.Unlock()
	_, found := tm.transactions[clientId]
	if found {
		return errors.New("transaction already began")
	}
	tm.transactions[clientId] = &Transaction{clientId: clientId, resources: make(map[Resource]LockType)}
	return nil
}

// Locks the given resource. Will return an error if deadlock is created.
func (tm *TransactionManager) Lock(clientId uuid.UUID, table db.Index, resourceKey int64, lType LockType) error {
	// get the transaction and resource with proper locking
	tm.tmMtx.Lock()
	tran, bool := tm.GetTransaction(clientId)
	if !bool {
		return errors.New("transaction doesn't exist")
	}
	tran.WLock()
	resource := Resource{table.GetName(), resourceKey}

	// check if lock already exists. Do appropriate returns if so
	lock_type, exists := tran.GetResources()[resource]
	if exists {
		if lType == 0 && lock_type == 1 {
			tran.RUnlock()
			return errors.New("requesting write lock over existing read lock")
		} 
		tran.RUnlock()
		return nil
	}

	// find conflicts by adding and removing edges to the graph
	conflicts := tm.discoverTransactions(resource, lType)
	for i := 0; i<len(conflicts); i++ {
		tm.pGraph.AddEdge(tran, conflicts[i])
	}

	cycle := tm.pGraph.DetectCycle()
	
	for i := 0; i<len(conflicts); i++ {
		tm.pGraph.RemoveEdge(tran, conflicts[i])
	}
	
	// either lock resource or return error
	if cycle {
		return errors.New("Cycle detected")
	}
	tran.resources[resource] = lType
	tm.lm.Lock(resource, lType)
	tm.tmMtx.Unlock()
	tran.WUnlock()
	return nil
}

// Unlocks the given resource.
func (tm *TransactionManager) Unlock(clientId uuid.UUID, table db.Index, resourceKey int64, lType LockType) error {
	tran, bool := tm.GetTransaction(clientId)
	
	if !bool {
		return errors.New("transaction doesn't exist")
	}
	tran.WLock()
	defer tran.WUnlock()

	resource := Resource{table.GetName(), resourceKey}
	_, exists := tran.GetResources()[resource]
	if exists {
		tm.lm.Unlock(resource, lType)
		delete(tran.resources, resource)
		return nil
	}
	return errors.New("resource doesn't exist")
}

// Commits the given transaction and removes it from the running transactions list.
func (tm *TransactionManager) Commit(clientId uuid.UUID) error {
	tm.tmMtx.Lock()
	defer tm.tmMtx.Unlock()
	// Get the transaction we want.
	t, found := tm.transactions[clientId]
	if !found {
		return errors.New("no transactions running")
	}
	// Unlock all resources.
	t.RLock()
	defer t.RUnlock()
	for r, lType := range t.resources {
		err := tm.lm.Unlock(r, lType)
		if err != nil {
			return err
		}
	}
	// Remove the transaction from our transactions list.
	delete(tm.transactions, clientId)
	return nil
}

// Returns a slice of all transactions that conflict w/ the given resource and locktype.
func (tm *TransactionManager) discoverTransactions(r Resource, lType LockType) []*Transaction {
	ret := make([]*Transaction, 0)
	for _, t := range tm.transactions {
		t.RLock()
		for storedResource, storedType := range t.resources {
			if storedResource == r && (storedType == W_LOCK || lType == W_LOCK) {
				ret = append(ret, t)
				break
			}
		}
		t.RUnlock()
	}
	return ret
}
