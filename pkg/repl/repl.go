package repl

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	uuid "github.com/google/uuid"
)

// REPL struct.
type REPL struct {
	commands map[string]func(string, *REPLConfig) error
	help     map[string]string
}

// REPL Config struct.
type REPLConfig struct {
	writer   io.Writer
	clientId uuid.UUID
}

// Get writer.
func (replConfig *REPLConfig) GetWriter() io.Writer {
	return replConfig.writer
}

// Get address.
func (replConfig *REPLConfig) GetAddr() uuid.UUID {
	return replConfig.clientId
}

// Construct an empty REPL.
func NewRepl() *REPL {
	return &REPL{
		commands: make(map[string]func(string, *REPLConfig) error), 
		help: make(map[string]string),
	}
}

// Combines a slice of REPLs.
func CombineRepls(repls []*REPL) (*REPL, error) {
	newREPL := &REPL {
		commands: make(map[string]func(string, *REPLConfig) error), 
		help: make(map[string]string),
	}
	count := 0
	for count < len(repls) {
		for k, v := range repls[count].commands {
			_, exists := newREPL.commands[k]
			if exists {
				return nil, errors.New("overlapping trigger found")
			}
			newREPL.commands[k] = v
		}
		for k, v := range repls[count].help {
			_, exists := newREPL.help[k]
			if exists {
				return nil, errors.New("overlapping trigger found")
			}
			newREPL.help[k] = v
		}
		count = count + 1
	}
	return newREPL, nil
}

// Get commands.
func (r *REPL) GetCommands() map[string]func(string, *REPLConfig) error {
	return r.commands
}

// Get help.
func (r *REPL) GetHelp() map[string]string {
	return r.help
}

// Add a command, along with its help string, to the set of commands.
func (r *REPL) AddCommand(trigger string, action func(string, *REPLConfig) error, help string) {
	_, exists := r.commands[trigger]
	_, exists2 := r.help[trigger]
	if !exists && !exists2 && trigger[0] != '.' {
		r.commands[trigger] = action
		r.help[trigger] = help	
	}
}

// Return all REPL usage information as a string.
func (r *REPL) HelpString() string {
	var sb strings.Builder
	for k, v := range r.help {
		sb.WriteString(fmt.Sprintf("%s: %s\n", k, v))
	}
	return sb.String()
}

// Run the REPL.
func (r *REPL) Run(c net.Conn, clientId uuid.UUID, prompt string) {
	var reader io.Reader
	var writer io.Writer
	
	if c == nil {
        reader = os.Stdin
        writer = os.Stdout
    } else {
        reader = c
        writer = c
    }

	scanner := bufio.NewScanner((reader))
	replConfig := &REPLConfig{writer: writer, clientId: clientId}
	
	for {
		io.WriteString(writer, prompt)

		if !scanner.Scan() {
            break
        }
		
		input := cleanInput(scanner.Text())
		
		if input == ".help" {
			io.WriteString(writer, r.HelpString())
			continue
		}

		action := r.commands[input]
		if action != nil {
			err := action(scanner.Text(), replConfig)
			if err != nil {
                io.WriteString(writer, err.Error()+"\n")
            }
        	} else {
            	io.WriteString(writer, "Unknown command. Type 'help' for available commands.\n")
        }
	}
}

// Run the REPL.
func (r *REPL) RunChan(c chan string, clientId uuid.UUID, prompt string) {
    writer := os.Stdout
    replConfig := &REPLConfig{writer: writer, clientId: clientId}

    for {
        io.WriteString(writer, prompt)

        raw_input, exists := <-c
        if !exists { 
            break
        }

        input := cleanInput(raw_input)

		if input == ".help" {
			io.WriteString(writer, r.HelpString())
			continue
		}

		action := r.commands[input]
		if action != nil {
			err := action(input, replConfig)
			if err != nil {
                io.WriteString(writer, err.Error()+"\n")
            }
        	} else {
            	io.WriteString(writer, "Unknown command. Type 'help' for available commands.\n")
        }
    }
}

// cleanInput preprocesses input to the db repl.
func cleanInput(text string) string {
	output := strings.TrimSpace(text)
	output = strings.ToLower(output)
	return output
}