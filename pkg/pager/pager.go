package pager

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	config "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/config"
	list "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/list"

	directio "github.com/ncw/directio"
)

// Page size - defaults to 4kb.
const PAGESIZE = int64(directio.BlockSize)

// Number of pages.
const NUMPAGES = config.NumPages

// Pagers manage pages of data read from a file.
type Pager struct {
	file         *os.File             // File descriptor.
	maxPageNum   int64                // The number of pages used by this database.
	ptMtx        sync.Mutex           // Page table mutex.
	freeList     *list.List           // Free page list.
	unpinnedList *list.List           // Unpinned page list.
	pinnedList   *list.List           // Pinned page list.
	pageTable    map[int64]*list.Link // Page table.
}

// Construct a new Pager.
func NewPager() *Pager {
	var pager *Pager = &Pager{}
	pager.pageTable = make(map[int64]*list.Link)
	pager.freeList = list.NewList()
	pager.unpinnedList = list.NewList()
	pager.pinnedList = list.NewList()
	frames := directio.AlignedBlock(int(PAGESIZE * NUMPAGES))
	for i := 0; i < NUMPAGES; i++ {
		frame := frames[i*int(PAGESIZE) : (i+1)*int(PAGESIZE)]
		page := Page{
			pager:    pager,
			pagenum:  NOPAGE,
			pinCount: 0,
			dirty:    false,
			data:     &frame,
		}
		pager.freeList.PushTail(&page)
	}
	return pager
}

// HasFile checks if the pager is backed by disk.
func (pager *Pager) HasFile() bool {
	return pager.file != nil
}

// GetFileName returns the file name.
func (pager *Pager) GetFileName() string {
	return pager.file.Name()
}

// GetNumPages returns the number of pages.
func (pager *Pager) GetNumPages() int64 {
	return pager.maxPageNum
}

// GetFreePN returns the next available page number.
func (pager *Pager) GetFreePN() int64 {
	// Assign the first page number beyond the end of the file.
	return pager.maxPageNum
}

// Open initializes our page with a given database file.
func (pager *Pager) Open(filename string) (err error) {
	// Create the necessary prerequisite directories.
	if idx := strings.LastIndex(filename, "/"); idx != -1 {
		err = os.MkdirAll(filename[:idx], 0775)
		if err != nil {
			return err
		}
	}
	// Open or create the db file.
	pager.file, err = directio.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	// Get info about the size of the pager.
	var info os.FileInfo
	var len int64
	if info, err = pager.file.Stat(); err == nil {
		len = info.Size()
		if len%PAGESIZE != 0 {
			return errors.New("open: DB file has been corrupted")
		}
	}
	// Set the number of pages and hand off initialization to someone else.
	pager.maxPageNum = len / PAGESIZE
	return nil
}

// Close signals our pager to flush all dirty pages to disk.
func (pager *Pager) Close() (err error) {
	// Prevent new data from being paged in.
	pager.ptMtx.Lock()
	// Check if all refcounts are 0.
	curLink := pager.pinnedList.PeekHead()
	if curLink != nil {
		fmt.Println("ERROR: pages are still pinned on close")
	}
	// Cleanup.
	pager.FlushAllPages()
	if pager.file != nil {
		err = pager.file.Close()
	}
	pager.ptMtx.Unlock()
	return err
}

// Populate a page's data field, given a pagenumber.
func (pager *Pager) ReadPageFromDisk(page *Page, pagenum int64) error {
	if _, err := pager.file.Seek(pagenum*PAGESIZE, 0); err != nil {
		return err
	}
	if _, err := pager.file.Read(*page.data); err != nil && err != io.EOF {
		return err
	}
	return nil
}

// NewPage returns an unused buffer from the free or unpinned list
// the ptMtx should be locked on entry
func (pager *Pager) NewPage(pagenum int64) (*Page, error) {


	// CASE 1
	// Take from freeList
	link := pager.freeList.PeekHead()
	if link != nil {
		// update the fields
		pager.pageTable[pagenum] = link
		page := link.GetKey().(*Page) 
		page.pager = pager
		page.pagenum = pagenum
		return page, nil
	}

	// CASE 2
	// Unpinned not backed by file
	if pager.HasFile() == false {
		// throw error
		return nil, errors.New("No free pages, unpinned pages not backed by disk")
	}

	// CASE 3
	// try retrieving the first page in the unpinned list. If it exists, 
	// flush contents & reset/update the page fields.
	link2 := pager.unpinnedList.PeekHead()
	if link2 != nil {
		// update the fields
		pager.pageTable[pagenum] = link2
		page2 := link2.GetKey().(*Page)
		pager.FlushPage(page2)
		page2.pager = pager
		page2.pagenum = pagenum
		page2.SetDirty(false)
		return page2, nil
	}
	// CASE 4
	// All fails
	return nil, errors.New("no available pages")
}



// getPage returns the page corresponding to the given pagenum.
func (pager *Pager) GetPage(pagenum int64) (page *Page, err error) {
	// CASE 1
	// invalid page
	if pagenum < 0 {
		return nil, errors.New("invalid pagenum")
	}
	pager.ptMtx.Lock()
	defer pager.ptMtx.Unlock()
	// CASE 2
	// Already in pagetable
	link, found := pager.pageTable[pagenum]
	if found {
		page_ret := link.GetKey().(*Page)
		page_ret.Get()
		// CASE 2.1
		// if previously pinned, return
		if page_ret.pinCount > 1 {
			return page_ret, nil
		} else {
			// CASE 2.2
			// if not previously pinned, move from unpinned to pinned list
			link.PopSelf()
			pager.pinnedList.PushTail(page_ret)
			return page_ret, nil
		}
	}
	// CASE 3
	// make new page
	page_ret2, error := pager.NewPage(pagenum)
	// CASE 3.1
	// failure building page
	if error != nil {
		return nil, error
	}
	// Update pinned/unpinned
	page_ret2.Get()
	pager.pageTable[pagenum].PopSelf()
	pager.pinnedList.PushTail(page_ret2)
	// CASE 3.2
	// Goes beyond pagenum size
	if pagenum > pager.maxPageNum {
		pager.maxPageNum = pagenum
		page_ret2.SetDirty(false) // in case it is a new blank page and dirty is set incorrectly
		pager.ReadPageFromDisk(page_ret2, pagenum) // not sure if i should read from disk
	// CASE 3.3
	// Within range of pagenum size
	} else {
		pager.ReadPageFromDisk(page_ret2, pagenum)
	}
	return page_ret2, nil
}

// Flush a particular page to disk.
func (pager *Pager) FlushPage(page *Page) {
	if page.IsDirty() {
		offset := page.pagenum*PAGESIZE
		pager.file.WriteAt(*page.GetData(), offset)
	}
}


// Flushes all dirty pages.
func (pager *Pager) FlushAllPages() {
	pin := pager.pinnedList.PeekHead()
	for pin != nil {
		pager.FlushPage(pin.GetKey().(*Page))
		pin = pin.GetNext()
	}
	
	unpin := pager.unpinnedList.PeekHead()
	for unpin != nil {
		pager.FlushPage(unpin.GetKey().(*Page))
		unpin = unpin.GetNext()
	}
}
