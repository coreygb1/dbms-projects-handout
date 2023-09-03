package list

import (
	"errors"
	"fmt"
	"io"
	"strings"

	repl "github.com/csci1270-fall-2023/db/pkg/repl"
)

// List struct.
type List struct {
	head *Link
	tail *Link
}

// Create a new list.
func NewList() *List {
	panic("function not yet implemented");
}

// Get a pointer to the head of the list.
func (list *List) PeekHead() *Link {
	panic("function not yet implemented");
}

// Get a pointer to the tail of the list.
func (list *List) PeekTail() *Link {
	panic("function not yet implemented");
}

// Add an element to the start of the list. Returns the added link.
func (list *List) PushHead(value interface{}) *Link {
	panic("function not yet implemented");
}

// Add an element to the end of the list. Returns the added link.
func (list *List) PushTail(value interface{}) *Link {
	panic("function not yet implemented");
}

// Find an element in a list given a boolean function, f, that evaluates to true on the desired element.
func (list *List) Find(f func(*Link) bool) *Link {
	panic("function not yet implemented");
}

// Apply a function to every element in the list. f should alter Link in place.
func (list *List) Map(f func(*Link)) {
	panic("function not yet implemented");
}

// Link struct.
type Link struct {
	list  *List
	prev  *Link
	next  *Link
	value interface{}
}

// Get the list that this link is a part of.
func (link *Link) GetList() *List {
	panic("function not yet implemented");
}

// Get the link's value.
func (link *Link) GetKey() interface{} {
	panic("function not yet implemented");
}

// Set the link's value.
func (link *Link) SetKey(value interface{}) {
	panic("function not yet implemented");
}

// Get the link's prev.
func (link *Link) GetPrev() *Link {
	panic("function not yet implemented");
}

// Get the link's next.
func (link *Link) GetNext() *Link {
	panic("function not yet implemented");
}

// Remove this link from its list.
func (link *Link) PopSelf() {
	panic("function not yet implemented");
}

// List REPL.
func ListRepl(list *List) *repl.REPL {
	panic("function not yet implemented");
}
