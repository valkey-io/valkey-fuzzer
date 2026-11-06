// Command porcupine checks a key/value history for linearizability with
// Porcupine (https://github.com/anishathalye/porcupine): each key is a
// register that writes set (a null value deletes it) and reads observe, and
// the history is linearizable if and only if every key's is.
//
// Usage:
//
//	porcupine [-timeout 60s] [-visualize illegal.html] history.jsonl
//
// Each line of the history is one operation:
//
//	{"process": 3, "key": "k", "f": "write", "value": "v", "call": 10, "return": 12}
//
// with "f" "write" or "read", "value" the value written or read (null:
// absent), and "call" and "return" when it was invoked and completed, in
// nanoseconds. "return" is null for an indeterminate operation (a timeout or
// a lost connection): it may take effect at any point after its call, or
// never. Every key starts absent.
//
// It prints {"result": "Ok" | "Illegal" | "Unknown", "illegal_keys": [...]}:
// Unknown when the check timed out. With -visualize, an Illegal result also
// writes Porcupine's visualization of the illegal keys' histories.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/anishathalye/porcupine"
)

type record struct {
	Process int     `json:"process"`
	Key     string  `json:"key"`
	F       string  `json:"f"`
	Value   *string `json:"value"`
	Call    int64   `json:"call"`
	Return  *int64  `json:"return"`
}

// register is a key's value; the zero value is an absent key.
type register struct {
	present bool
	value   string
}

func (r register) String() string {
	if !r.present {
		return "nil"
	}
	return fmt.Sprintf("%q", r.value)
}

type input struct {
	key   string
	write bool
	value register
	// The operation's outcome is unknown: a read observed nothing.
	indeterminate bool
}

func partition(history []porcupine.Operation) [][]porcupine.Operation {
	byKey := map[string][]porcupine.Operation{}
	for _, op := range history {
		key := op.Input.(input).key
		byKey[key] = append(byKey[key], op)
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	partitions := make([][]porcupine.Operation, len(keys))
	for i, key := range keys {
		partitions[i] = byKey[key]
	}
	return partitions
}

var model = porcupine.Model{
	Partition: partition,
	Init:      func() interface{} { return register{} },
	Step: func(state, in, _ interface{}) (bool, interface{}) {
		op := in.(input)
		if op.write {
			return true, op.value
		}
		return op.indeterminate || op.value == state.(register), state
	},
	DescribeOperation: func(in, _ interface{}) string {
		op := in.(input)
		if op.write {
			return fmt.Sprintf("write(%q, %v)", op.key, op.value)
		}
		if op.indeterminate {
			return fmt.Sprintf("read(%q) -> ?", op.key)
		}
		return fmt.Sprintf("read(%q) -> %v", op.key, op.value)
	},
	DescribeState: func(state interface{}) string { return state.(register).String() },
}

func load(path string) ([]porcupine.Operation, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var ops []porcupine.Operation
	var indeterminate []int
	var end int64
	decoder := json.NewDecoder(file)
	for {
		var r record
		if err := decoder.Decode(&r); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("%s: operation %d: %w", path, len(ops)+1, err)
		}
		if r.F != "write" && r.F != "read" {
			return nil, fmt.Errorf("%s: operation %d: f is %q, not write or read", path, len(ops)+1, r.F)
		}
		in := input{key: r.Key, write: r.F == "write", indeterminate: r.Return == nil}
		if r.Value != nil {
			in.value = register{present: true, value: *r.Value}
		}
		op := porcupine.Operation{ClientId: r.Process, Input: in, Call: r.Call}
		if r.Return == nil {
			indeterminate = append(indeterminate, len(ops))
		} else {
			op.Return = *r.Return
		}
		if op.Call > end {
			end = op.Call
		}
		if op.Return > end {
			end = op.Return
		}
		ops = append(ops, op)
	}
	// An indeterminate operation stays open past every other one, as
	// Porcupine's own Jepsen parser closes the operations a client never
	// saw complete.
	for _, i := range indeterminate {
		ops[i].Return = end + 1
	}
	return ops, nil
}

func main() {
	timeout := flag.Duration("timeout", 0, "give up, with result Unknown, after this long (0: never)")
	visualize := flag.String("visualize", "", "on an Illegal result, write the illegal keys' visualization to this HTML file")
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: porcupine [-timeout 60s] [-visualize illegal.html] history.jsonl")
		os.Exit(2)
	}
	ops, err := load(flag.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	result, info := porcupine.CheckOperationsVerbose(model, ops, *timeout)
	illegal := []string{}
	if result == porcupine.Illegal {
		// A key is illegal when no partial linearization covers its history.
		var illegalOps []porcupine.Operation
		for i, part := range partition(ops) {
			complete := false
			for _, partial := range info.PartialLinearizations()[i] {
				complete = complete || len(partial) == len(part)
			}
			if !complete {
				illegal = append(illegal, part[0].Input.(input).key)
				illegalOps = append(illegalOps, part...)
			}
		}
		if *visualize != "" {
			_, illegalInfo := porcupine.CheckOperationsVerbose(model, illegalOps, *timeout)
			if err := porcupine.VisualizePath(model, illegalInfo, *visualize); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]interface{}{"result": result, "illegal_keys": illegal}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
