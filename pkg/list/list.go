package list

import (
	"errors"
	"fmt"
	"strings"

	repl "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/repl"
)

// List struct.
type List struct {
	head *Link
	tail *Link
}

// Create a new list.
func NewList() *List {
	nlist := List{nil, nil}
	return &nlist
}

// Get a pointer to the head of the list.
func (list *List) PeekHead() *Link {
	return list.head
}

// Get a pointer to the tail of the list.
func (list *List) PeekTail() *Link {
	return list.tail
}

// Add an element to the start of the list. Returns the added link.
func (list *List) PushHead(value interface{}) *Link {
	newlink := &Link{list, nil, list.head, value}
	if list.head != nil {
		list.head.prev = newlink
	}
	list.head = newlink
	if list.tail == nil {
		list.tail = newlink
	}
	return newlink
}

// Add an element to the end of the list. Returns the added link.
func (list *List) PushTail(value interface{}) *Link {
	newlink := &Link{list, list.tail, nil, value}
	if list.tail != nil {
		list.tail.next = newlink
	}
	list.tail = newlink
	if list.head == nil {
		list.head = newlink
	}
	return newlink
}

// Find an element in a list given a boolean function, f, that evaluates to true on the desired element.
func (list *List) Find(f func(*Link) bool) *Link {
	newlist := &List{list.head, list.tail}
	for newlist.head != nil {
		if f(newlist.head) {
			return newlist.head
		}
		newlist.head = newlist.head.next
	}
	return nil
}

// Apply a function to every element in the list. f should alter Link in place.
func (list *List) Map(f func(*Link)) {
	newlist := &List{list.head, list.tail}
	for newlist.head != nil {
		f(newlist.head)
		newlist.head = newlist.head.next
	}
	list = newlist
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
	return link.list
}

// Get the link's value.
func (link *Link) GetKey() interface{} {
	return link.value
}

// Set the link's value.
func (link *Link) SetKey(value interface{}) {
	link.value = value
}

// Get the link's prev.
func (link *Link) GetPrev() *Link {
	return link.prev
}

// Get the link's next.
func (link *Link) GetNext() *Link {
	return link.next
}

// Remove this link from its list.
// Suppose list [2,3,4]
func (link *Link) PopSelf() {
	// so it's the first one
	if link.prev == nil && link.next == nil {
		link.list.head = nil
		link.list.tail = nil
	} else if link.prev == nil {
		link.next.prev = nil
		link.list.head = link.next
	} else if link.next == nil {
		link.prev.next = nil
		link.list.tail = link.prev
	} else {
		prevlink := link.prev
		prevlink.next = link.next
		link.prev.next = link.next
		link.next.prev = prevlink
		link.list = nil
		link.next = nil
		link.prev = nil
	}
}

// List REPL.
// use dispatcher
func ListRepl(list *List) *repl.REPL {
	newrepl := repl.NewRepl()
	newrepl.AddCommand("list_print", func(str string, repl *repl.REPLConfig) error {
		if len(strings.Split(str, " ")) == 2 {
			list.Map(func(linkput *Link) { fmt.Println(linkput.value) })
			return nil
		} else {
			return errors.New("the format is not well-informed")
		}
	}, "Input: List of anything. Prints out all of the elements in the list in order")
	newrepl.AddCommand("list_push_head", func(str string, repl *repl.REPLConfig) error {
		if len(strings.Split(str, " ")) == 2 {
			list.PushHead(strings.Split(str, " ")[1])
			return nil
		} else {
			return errors.New("the format is not well-informed")
		}
	}, "Inserts the given element to the List as a string")
	newrepl.AddCommand("list_push_tail", func(str string, repl *repl.REPLConfig) error {
		if len(strings.Split(str, " ")) == 2 {
			list.PushTail(strings.Split(str, " ")[1])
			return nil
		} else {
			return errors.New("the format is not well-informed")
		}
	},
		"Inserts the given element to the end of the List as a string")
	newrepl.AddCommand("list_remove", func(str string, repl *repl.REPLConfig) error {
		if len(strings.Split(str, " ")) == 2 {
			list.Find(func(linkfind *Link) bool { return linkfind.value == strings.Split(str, " ")[2] }).PopSelf()
			return nil
		} else {
			return errors.New("the format is not well-informed")
		}
	},
		"Removes the given element from the list")
	newrepl.AddCommand("list_contains", func(str string, repl *repl.REPLConfig) error {
		if len(strings.Split(str, " ")) == 2 {
			if list.Find(func(linkfind *Link) bool { return linkfind.value == strings.Split(str, " ")[2] }) != nil {
				fmt.Print("found!")
			} else {
				fmt.Print("not found")
			}
			return nil
		} else {
			return errors.New("the format is not well-informed")
		}
	},
		"Check whether the element is in the list or not")
	return newrepl
}



// package list

// import (
// 	// "errors"
// 	"fmt"
// 	"io"
// 	"strings"

// 	repl "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/repl"
// )

// // List struct.
// type List struct {
// 	head *Link
// 	tail *Link
// }

// // Create a new list.
// func NewList() *List {
// 	return &List{head: nil, tail: nil}
// }

// // Get a pointer to the head of the list.
// func (list *List) PeekHead() *Link {
// 	return list.head
// }

// // Get a pointer to the tail of the list.
// func (list *List) PeekTail() *Link {
// 	return list.tail
// }

// // Add an element to the start of the list. Returns the added link.
// func (list *List) PushHead(value interface{}) *Link {
// 	newLink := &Link{
// 		list:  list,
// 		prev:  nil,
// 		next:  list.head,
// 		value: value,
// 	}
// 	if list.head != nil {
// 		list.head.prev = newLink
// 	}
// 	list.head = newLink
// 	if list.tail == nil { // handle the case when the list was empty
// 		list.tail = newLink
// 	}
// 	return newLink
// }

// // Add an element to the end of the list. Returns the added link.
// func (list *List) PushTail(value interface{}) *Link {
// 	newLink := &Link{
// 		list:  list,
// 		prev:  list.tail,
// 		next:  nil,
// 		value: value,
// 	}
// 	if list.tail != nil {
// 		list.tail.next = newLink
// 	}
// 	list.tail = newLink
// 	if list.head == nil { // handle the case when the list was empty
// 		list.head = newLink
// 	}
// 	return newLink
// }

// // Find an element in a list given a boolean function, f, that evaluates to true on the desired element.
// func (list *List) Find(f func(*Link) bool) *Link {
// 	check := list.head
// 	for check != nil {
// 		if f(check) {
// 			return check
// 		}
// 		check = check.next
// 	}
// 	return nil
// }

// // Apply a function to every element in the list. f should alter Link in place.
// func (list *List) Map(f func(*Link)) {
// 	current := list.head
// 	for current != nil {
// 		f(current)
// 		current = current.next
// 	}
// }

// // Link struct.
// type Link struct {
// 	list  *List
// 	prev  *Link
// 	next  *Link
// 	value interface{}
// }

// // Get the list that this link is a part of.
// func (link *Link) GetList() *List {
// 	return link.list
// }

// // Get the link's value.
// func (link *Link) GetKey() interface{} {
// 	return link.value
// }

// // Set the link's value.
// func (link *Link) SetKey(value interface{}) {
// 	link.value = value
// }

// // Get the link's prev.
// func (link *Link) GetPrev() *Link {
// 	return link.prev
// }

// // Get the link's next.
// func (link *Link) GetNext() *Link {
// 	return link.next
// }

// // Remove this link from its list.
// func (link *Link) PopSelf() {
// 	if link == nil {
// 		return
// 	}
// 	if link.prev != nil && link.next == nil {
// 		link.prev.next = nil
// 		link.list.tail = link.prev
// 	}
// 	if link.prev == nil && link.next != nil {
// 		link.next.prev = nil
// 		link.list.head = link.next
// 	}
// 	if link.prev != nil && link.next != nil {
// 		link.next.prev = link.prev
// 		link.prev.next = link.next
// 	}
// 	if link.prev == nil && link.next == nil {
// 		link.list.head = nil
// 		link.list.tail = nil
// 	}
// 	link.next = nil
// 	link.prev = nil
// }

// // List REPL.
// func ListRepl(list *List) *repl.REPL {
//     r := repl.NewRepl()

//     listPrintCommand := func(input string, cfg *repl.REPLConfig) error {
// 		var sb strings.Builder
//         current := list.head
//         first := true
//         for current != nil {
//             if !first {
//                 sb.WriteString(", ")
//             }
//             sb.WriteString(fmt.Sprintf("%v", current.value))
//             current = current.next
//             first = false
//         }
//         _, err := io.WriteString(cfg.GetWriter(), sb.String()+"\n")
//         return err
//     }

//     r.AddCommand("list_print", listPrintCommand, "Prints out of the elements in the list in order")
//     // ... add other commands ...

//     return r
// }