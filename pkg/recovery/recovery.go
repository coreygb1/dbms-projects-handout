package recovery

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	concurrency "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/concurrency"
	db "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/db"
	"github.com/otiai10/copy"

	uuid "github.com/google/uuid"
)

// Recovery Manager.
type RecoveryManager struct {
	d       *db.Database
	tm      *concurrency.TransactionManager
	txStack map[uuid.UUID]([]Log)
	fd      *os.File
	mtx     sync.Mutex
}

// Construct a recovery manager.
func NewRecoveryManager(
	d *db.Database,
	tm *concurrency.TransactionManager,
	logName string,
) (*RecoveryManager, error) {
	fd, err := os.OpenFile(logName, os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &RecoveryManager{
		d:       d,
		tm:      tm,
		txStack: make(map[uuid.UUID][]Log),
		fd:      fd,
	}, nil
}

// Write the string `s` to the log file. Expects rm.mtx to be locked
func (rm *RecoveryManager) writeToBuffer(s string) error {
	_, err := rm.fd.WriteString(s)
	if err != nil {
		return err
	}
	err = rm.fd.Sync()
	return err
}

// Write a Table log.
func (rm *RecoveryManager) Table(tblType string, tblName string) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	tl := tableLog{
		tblType: tblType,
		tblName: tblName,
	}
	rm.writeToBuffer(tl.toString())
}

// Write an Edit log.
func (rm *RecoveryManager) Edit(clientId uuid.UUID, table db.Index, action Action, key int64, oldval int64, newval int64) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	el := editLog{
		id: clientId,
		tablename: table.GetName(),
		action: action,
		key: key,
		oldval: oldval,
		newval: newval,
	}
	rm.writeToBuffer(el.toString())
	rm.txStack[clientId] = append(rm.txStack[clientId], &el)
}

// Write a transaction start log.
func (rm *RecoveryManager) Start(clientId uuid.UUID) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	sl := startLog{
		id: clientId,
	}
	rm.writeToBuffer(sl.toString())
	rm.txStack[clientId] = []Log{}
	rm.txStack[clientId] = append(rm.txStack[clientId], &sl)
}

// Write a transaction commit log.
func (rm *RecoveryManager) Commit(clientId uuid.UUID) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	cl := commitLog {
		id: clientId,
	}
	rm.writeToBuffer(cl.toString())
	delete(rm.txStack, clientId)
}

// Flush all pages to disk and write a checkpoint log.
func (rm *RecoveryManager) Checkpoint() {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	var idsList []uuid.UUID
	for id, _ := range rm.txStack {
		idsList = append(idsList, id)
	}
	cpl := checkpointLog {
		ids: idsList,
	}
	for _, table := range rm.d.GetTables() {
		table.GetPager().LockAllUpdates()
		table.GetPager().FlushAllPages()
		table.GetPager().UnlockAllUpdates()
	}
	rm.writeToBuffer(cpl.toString())
	// add to the stack? 
	rm.Delta() // Sorta-semi-pseudo-copy-on-write (to ensure db recoverability)
}

// Redo a given log's action.
func (rm *RecoveryManager) Redo(log Log) error {
	switch log := log.(type) {
	case *tableLog:
		payload := fmt.Sprintf("create %s table %s", log.tblType, log.tblName)
		err := db.HandleCreateTable(rm.d, payload, os.Stdout)
		if err != nil {
			return errors.New("table redo error")
		}
	case *editLog:
		switch log.action {
		case INSERT_ACTION:
			payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
			err := db.HandleInsert(rm.d, payload)
			if err != nil {
				// There is already an entry, try updating
				payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
				err = db.HandleUpdate(rm.d, payload)
				if err != nil {
					return errors.New("table insert error")
				}
			}
		case UPDATE_ACTION:
			payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
			err := db.HandleUpdate(rm.d, payload)
			if err != nil {
				// Entry may have been deleted, try inserting
				payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
				err := db.HandleInsert(rm.d, payload)
				if err != nil {
					return errors.New("table update error")
				}
			}
		case DELETE_ACTION:
			payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
			err := db.HandleDelete(rm.d, payload)
			if err != nil {
				return errors.New("table delete error")
			}
		}
	default:
		return errors.New("can only redo edit logs")
	}
	return nil
}

// Undo a given log's action.
func (rm *RecoveryManager) Undo(log Log) error {
	switch log := log.(type) {
	case *editLog:
		switch log.action {
		case INSERT_ACTION:
			payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
			err := HandleDelete(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		case UPDATE_ACTION:
			payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.oldval)
			err := HandleUpdate(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		case DELETE_ACTION:
			payload := fmt.Sprintf("insert %v %v into %s", log.key, log.oldval, log.tablename)
			err := HandleInsert(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("can only undo edit logs")
	}
	return nil
}

// helper function that checks if value is in list
func isInList(value int, list []int) bool {
    for _, v := range list {
        if v == value {
            return true
        }
    }
    return false 
}


// Do a full recovery to the most recent checkpoint on startup.
func (rm *RecoveryManager) Recover() error {
	logs, checkpointPos, err := rm.readLogs()
	
	if err != nil {
		return errors.New("error 1")
	}

	///// Step 1: Get a map of all active transactions

	// If a checkpoint exists, initialize the map with active transactions contained
	// in the checkpoint log
	activeTran := make(map[uuid.UUID]bool)
	if _, isCheckpoint := logs[checkpointPos].(*checkpointLog); isCheckpoint {
		for _, id := range logs[checkpointPos].(*checkpointLog).ids {
			activeTran[id] = true
		}
	}

	// Process logs that started after the checkpoint and remove logs that commit after 
	// the checkpoint
	for i := checkpointPos; i < len(logs); i++ {
		switch log := logs[i].(type) {
		case *startLog:
			activeTran[log.id] = true
		case *commitLog:
			delete(activeTran, log.id)
		}
	}

	fmt.Println("Active Logs:")
	for id := range activeTran {
		fmt.Println(id)
	}

	// Restart all transactions in transaction manager
	for id := range activeTran {
		rm.tm.Begin(id)
	}

	// Step 2: Redo, maintaining updated active transactions

	for i := checkpointPos + 1; i < len(logs); i++ {
		switch log := logs[i].(type) {
		case *startLog:
			rm.Start(log.id)
		case *commitLog:
			delete(activeTran, log.id)
			rm.Commit(log.id)
			rm.tm.Commit(log.id)
		default:
			err := rm.Redo(log)
			if err != nil {
				return err
			}
		}
    }

	// Step 3: Undo

	for i := len(logs); i >= 0; i-- {
		log := logs[i]
		if activeTran[log.id] {
			switch log.(type) {
			case *editLog:
				err := rm.Undo(log)
				if err != nil {
					return err
				}
			case *startLog: 
				err := rm.tm.Commit(log.id) // remove from transaction list
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
	

	///// Remaining questions:
	// Do I use 'Start', 'begin' and 'Commit' correctly? 
	// Do I need to do anything else for step 4?
	// Is this a correct understanding of active transactions?

}

// Roll back a particular transaction.
func (rm *RecoveryManager) Rollback(clientId uuid.UUID) error {
	rm.tm.Begin(clientId)
	
	logs, _ := rm.txStack[clientId]
	if len(logs) == 0 {
		return errors.New("No logs available for client ID")
	}

	if _, isStart := logs[0].(*startLog); !isStart {
		return errors.New("Must start with start log")
	}
	
	for i := len(logs) - 1; i >= 0; i-- {
		log := logs[i]
		if _, isEdit := log.(*editLog); isEdit {
			err := rm.Undo(log)
			if err != nil {
				return errors.New("error 5")
			}
		}
	}

	rm.Commit(clientId)
	err := rm.tm.Commit(clientId)
	if err != nil {
		return err
	}

	return nil
}

// Primes the database for recovery
func Prime(folder string) (*db.Database, error) {
	// Ensure folder is of the form */
	base := strings.TrimSuffix(folder, "/")
	recoveryFolder := base + "-recovery/"
	dbFolder := base + "/"
	if _, err := os.Stat(dbFolder); err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(recoveryFolder, 0775)
			if err != nil {
				return nil, err
			}
			return db.Open(dbFolder)
		}
		return nil, err
	}
	if _, err := os.Stat(recoveryFolder); err != nil {
		if os.IsNotExist(err) {
			return db.Open(dbFolder)
		}
		return nil, err
	}
	os.RemoveAll(dbFolder)
	err := copy.Copy(recoveryFolder, dbFolder)
	if err != nil {
		return nil, err
	}
	return db.Open(dbFolder)
}

// Should be called at end of Checkpoint.
func (rm *RecoveryManager) Delta() error {
	folder := strings.TrimSuffix(rm.d.GetBasePath(), "/")
	recoveryFolder := folder + "-recovery/"
	folder += "/"
	os.RemoveAll(recoveryFolder)
	err := copy.Copy(folder, recoveryFolder)
	return err
}
