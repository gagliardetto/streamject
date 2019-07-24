package main

import (
	"fmt"
	"time"

	rtd "runtime/debug"

	"github.com/gagliardetto/streamject"
	. "github.com/gagliardetto/utils"
)

func main() {
	stm, err := streamject.NewJSON("./owner.buildlist.json")
	//stm, err := streamject.NewMsgPack("./owner.buildlist.msgpk")
	if err != nil {
		panic(err)
	}
	defer stm.Close()

	start := time.Now()

	if true {
		for i := 0; i < 600000; i++ {
			newItem := &Message{
				Name: RandomString(256),
				Text: RandomString(256),
			}
			err = stm.Append(newItem)
			if err != nil {
				panic(err)
			}
		}
	}

	newItem := &Message{
		Name: "hello",
		Text: "world\nwooooo",
	}
	err = stm.Append(newItem)
	if err != nil {
		panic(err)
	}

	err = stm.Iterate(func(line *streamject.Line) bool {
		var msg Message
		err := line.Decode(&msg)
		if err != nil {
			panic(err)
		}

		//spew.Dump(msg)
		return true
	})
	if err != nil {
		panic(err)
	}

	has := hasByID(stm, 2)
	fmt.Println("has:", has)
	has = hasByID(stm, 1)
	fmt.Println("has:", has)
	has = hasByID(stm, 3)
	fmt.Println("has:", has)
	has = hasByID(stm, 4)
	fmt.Println("has:", has)
	// TODO:
	// - parse line json.
	// - obj is kept in memory until this function is not exited.
	// - when this function exits, the object is destroyed, and GC is performed.
	// - this means that any use of the object must be done inside this funtion.
	// -
	// -
	// -

	// NOTES:
	// - the object will be kept in memory as long as this function is not closed.
	// - it's up to you to load as many lines as you want or can, and close them.
	rtd.FreeOSMemory()

	fmt.Println("all done in", time.Now().Sub(start))
	time.Sleep(time.Minute)
}

func hasByID(stm *streamject.Stream, id int) bool {

	var has bool
	err := stm.Iterate(func(line *streamject.Line) bool {
		var msg MessageIDOnly
		err := line.Decode(&msg)
		if err != nil {
			panic(err)
		}

		//spew.Dump(msg)
		if msg.ID == id {
			has = true
			return false
		}

		return true
	})
	if err != nil {
		panic(err)
	}
	return has
}

type Message struct {
	Name, Text string
	ID         int
}
type MessageIDOnly struct {
	ID int
}
