package main

import (
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/gagliardetto/hashsearch"
	"github.com/gagliardetto/streamject"
	. "github.com/gagliardetto/utilz"
)

type Message struct {
	Text string `msgpack:"text"`
	Name string `msgpack:"name"`
	ID   int    `msgpack:"id"`
	Sub  *SubMessage
}
type SubMessage struct {
	Text string `msgpack:"text"`
	Name string `msgpack:"name"`
	ID   int    `msgpack:"id"`
}

func main() {

	fileName := "./file.json"
	stm, err := streamject.NewJSON(fileName)
	if err != nil {
		panic(err)
	}
	defer stm.Close()

	{
		// Add objects to the streamject file:
		for i := 0; i < 10; i++ {
			newItem := &Message{
				ID:   i,
				Name: "Message ID " + Itoa(i),
				Text: RandomString(256),
				Sub: &SubMessage{
					Name: RandomString(256),
					Text: RandomString(256),
				},
			}
			err = stm.Append(newItem)
			if err != nil {
				panic(err)
			}
		}
	}

	{
		// You can iterate over all lines:
		err = stm.Iterate(func(line streamject.Line) bool {
			var msg Message
			err := line.Decode(&msg)
			if err != nil {
				panic(err)
			}

			spew.Dump(msg)
			return true
		})
		if err != nil {
			panic(err)
		}
	}

	{
		// Create an index on the hash of the name:
		err := stm.CreateIndexByUint64(nameHashIndex, func(line streamject.Line) uint64 {
			var msg Message
			err := line.Decode(&msg)
			if err != nil {
				panic(err)
			}

			return hashsearch.HashString(msg.Name)
		})
		if err != nil {
			panic(err)
		}

		// Check whether a value is present:
		// Has:
		fmt.Println("has:", HasByName(stm, "Message ID 1"))
		// Doesn't have:
		fmt.Println("has:", HasByName(stm, "Message ID 99"))
	}
}

const (
	nameHashIndex = "messagge-name-hash"
)

func HasByName(stm *streamject.Stream, name string) bool {
	return stm.HasUint64ByIndex(nameHashIndex, hashsearch.HashString(name))
}
