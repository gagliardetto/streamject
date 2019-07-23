package streamject

import (
	"encoding/json"

	"github.com/gagliardetto/listfile"
	"github.com/vmihailenco/msgpack"
)

type Stream struct {
	list      *listfile.ListFile
	marshal   MarshalFunc
	unmarshal UnmarshalFunc
}

type MarshalFunc func(v interface{}) ([]byte, error)
type UnmarshalFunc func(data []byte, v interface{}) error

func getJSONFuncs() (MarshalFunc, UnmarshalFunc) {
	return json.Marshal, json.Unmarshal
}
func getMsgPackFuncs() (MarshalFunc, UnmarshalFunc) {
	return msgpack.Marshal, msgpack.Unmarshal
}

type Line struct {
	index     int64
	body      []byte
	unmarshal UnmarshalFunc
}

// Index returns the line number that this object
// was scanned from from the file.
// Starts from 0.
func (l *Line) Index() int64 {
	return l.index
}

func (l *Line) Decode(f interface{}) error {
	return msgpack.Unmarshal(l.body, &f)
}
func (s *Stream) Append(v interface{}) error {
	marshaled, err := msgpack.Marshal(v)
	if err != nil {
		return err
	}
	return s.list.Append(string(marshaled))
}

func (s *Stream) Iterate(callback func(line *Line) bool) error {
	var index int64
	return s.list.IterateLinesAsBytes(func(val []byte) bool {
		line := &Line{
			index:     index,
			body:      val,
			unmarshal: s.unmarshal,
		}

		index++
		return callback(line)
	})
}

func (s *Stream) Close() error {
	return s.list.Close()
}

// Len returns the total size in bytes
// of the stream.
func (s *Stream) Len() int {
	return s.list.Len()
}
func (s *Stream) LenInt64() int64 {
	return s.list.LenInt64()
}

// LenLines returns the number of objects
// contained in the stream.
func (s *Stream) LenLines() int {
	return s.list.LenLines()
}

func New(path string) (*Stream, error) {
	return NewJSON(path)
}
func NewJSON(path string) (*Stream, error) {
	ma, un := getJSONFuncs()
	return newStream(
		path,
		ma,
		un,
	)
}
func NewMsgPack(path string) (*Stream, error) {
	ma, un := getMsgPackFuncs()
	return newStream(
		path,
		ma,
		un,
	)
}
func newStream(
	path string,
	marshal MarshalFunc,
	unmarshal UnmarshalFunc,
) (*Stream, error) {
	list, err := listfile.New(path)
	if err != nil {
		return nil, err
	}
	str := &Stream{
		list:      list,
		marshal:   marshal,
		unmarshal: unmarshal,
	}
	return str, nil
}
