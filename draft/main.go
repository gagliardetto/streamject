package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/gagliardetto/streamject"
)

func insert(s []int64, i int, elem int64) []int64 {
	s = append(s, 0 /* use the zero value of the element type */)
	copy(s[i+1:], s[i:])
	s[i] = elem
	return s
}
func main() {

	file, err := os.OpenFile(
		"./batch.strj",
		os.O_CREATE|os.O_RDWR,
		0666,
	)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var lineEnds []int64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		b := scanner.Bytes()
		lineEnds = append(lineEnds, int64(len(b))+2)
	}

	fmt.Println(lineEnds)

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	if false { // insert new line in middle of file:
		insertAfterLine := 1
		afterX := lineEnds[insertAfterLine]

		if _, err := file.Seek(afterX, 0); err != nil {
			panic(err)
		}
		remainder, err := ioutil.ReadAll(file)
		if err != nil {
			panic(err)
		}
		file.Seek(afterX, 0)
		toAdd := []byte("\n|bella ciao")
		file.Write(toAdd)
		file.Write(remainder)
		fmt.Println("reminder:", string(remainder))
		// keep track of new line:
		lineEnds = insert(lineEnds, insertAfterLine, int64(len(toAdd)))
		fmt.Println(lineEnds)
	}
	{ // replace line with a new line of whatever length:
		replacement := []byte("this is the new, updated line" + "\n")

		replaceAt := 0

		lensOfLinesBefore := lineEnds[:replaceAt]
		fmt.Println("lensOfLinesBefore", lensOfLinesBefore)
		var offsetToAfterLineToBeReplaced int64
		for _, v := range lensOfLinesBefore {
			offsetToAfterLineToBeReplaced += v - 1
		}

		fmt.Println("offsetToAfterLineToBeReplaced", offsetToAfterLineToBeReplaced)

		// 3+4-2+5+1
		if _, err := file.Seek(offsetToAfterLineToBeReplaced+lineEnds[replaceAt]-1, 0); err != nil {
			panic(err)
		}
		remainder, err := ioutil.ReadAll(file)
		if err != nil {
			panic(err)
		}

		if _, err := file.Seek(offsetToAfterLineToBeReplaced, 0); err != nil {
			panic(err)
		}
		file.Write([]byte(replacement))
		file.Write(remainder)
	}

	return
	{
		// insert a new line in the middle somewhere:
		idx := lineEnds[1]
		if _, err := file.Seek(idx, 0); err != nil {
			panic(err)
		}
		remainder, err := ioutil.ReadAll(file)
		if err != nil {
			panic(err)
		}
		file.Seek(idx, 0)
		file.Write([]byte("beta"))
		file.Write(remainder)
	}

	return
	stm, err := streamject.New("./batch.strj")
	if err != nil {
		panic(err)
	}

	for line := stm.Next(); line != nil; {
		line.Parse(func(ct *Contact) error {
			// TODO:
			// - parse line json.
			// - modify it.
			// - when this function exits, the line will be automatically marshaled and saved.

			// NOTES:
			// - the object will be kept in memory as long as this function is not closed.
			// - it's up to you to load as many lines as you want or can, and close them.

			return nil
		})
	}
}

type Contact struct {
	Name   string
	Number int
}
