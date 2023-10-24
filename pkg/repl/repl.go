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
	panic("function not yet implemented")
}

// Combines a slice of REPLs.
func CombineRepls(repls []*REPL) (*REPL, error) {
	panic("function not yet implemented")
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
	panic("function not yet implemented")
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
	// Get reader and writer; stdin and stdout if no conn.
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
	// Begin the repl loop!
	panic("function not yet implemented")
}

// Run the REPL.
func (r *REPL) RunChan(c chan string, clientId uuid.UUID, prompt string) {
	// Get reader and writer; stdin and stdout if no conn.
	writer := os.Stdout
	replConfig := &REPLConfig{writer: writer, clientId: clientId}
	// Begin the repl loop!
	io.WriteString(writer, prompt)
	for payload := range c {
		// Emit the payload for debugging purposes.
		io.WriteString(writer, payload+"\n")
		// Parse the payload.
		fields := strings.Fields(payload)
		if len(fields) == 0 {
			io.WriteString(writer, prompt)
			continue
		}
		trigger := cleanInput(fields[0])
		// Check for a meta-command.
		if trigger == ".help" {
			io.WriteString(writer, r.HelpString())
			io.WriteString(writer, prompt)
			continue
		}
		// Else, check user commands.
		if command, exists := r.commands[trigger]; exists {
			// Call a hardcoded function.
			err := command(payload, replConfig)
			if err != nil {
				io.WriteString(writer, fmt.Sprintf("%v\n", err))
			}
		} else {
			io.WriteString(writer, "command not found\n")
		}
		io.WriteString(writer, prompt)
	}
	// Print an additional line if we encountered an EOF character.
	io.WriteString(writer, "\n")
}

// cleanInput preprocesses input to the db repl.
func cleanInput(text string) string {
	output := strings.TrimSpace(text)
	output = strings.ToLower(output)
	return output
}
