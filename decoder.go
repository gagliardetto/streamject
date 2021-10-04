package streamject

import (
	"github.com/gagliardetto/listfile"
	jsoniter "github.com/json-iterator/go"
)

type Stream struct {
	list      *listfile.ListFile
	marshal   MarshalFunc
	unmarshal UnmarshalFunc
}

type MarshalFunc func(v interface{}) ([]byte, error)
type UnmarshalFunc func(data []byte, v interface{}) error

func getJSONFuncs(customTagkey string) (MarshalFunc, UnmarshalFunc) {
	jsIterConfig := jsoniter.Config{
		//EscapeHTML: true,
		// NOTE: using this TagKey because the payloads are in json and require their json tagkey to be complete;
		// I want to use short versions to save space, so I have to use another TagKey here:
		TagKey: customTagkey,
	}.Froze()

	return jsIterConfig.Marshal, jsIterConfig.Unmarshal
}

type Line struct {
	lineNum   int64
	body      []byte
	unmarshal UnmarshalFunc
}

func (l *Line) Body() []byte {
	return l.body
}

// LineNum returns the line number that this object
// was scanned from from the file.
// Starts from 1.
func (l *Line) LineNum() int64 {
	return l.lineNum
}

// Decode can be called only once
func (l *Line) Decode(f interface{}) error {
	err := l.unmarshal(l.body, &f)
	if err != nil {
		return err
	}
	l.body = nil
	return nil
}
func (s *Stream) Append(v interface{}) error {
	marshaled, err := s.marshal(v)
	if err != nil {
		return err
	}
	return s.list.AppendBytes(marshaled)
}

func (s *Stream) Iterate(callback func(line Line) bool) error {
	lineNum := int64(1)
	return s.list.IterateLinesAsBytes(func(val []byte) bool {
		line := Line{
			lineNum:   lineNum,
			body:      val,
			unmarshal: s.unmarshal,
		}

		lineNum++
		return callback(line)
	})
}

func (s *Stream) Close() error {
	return s.list.Close()
}

// NumBytes returns the total size in bytes
// of the file.
func (s *Stream) NumBytes() int64 {
	return s.list.NumBytes()
}

// NumLines returns the number of objects
// contained in the file.
func (s *Stream) NumLines() int64 {
	return s.list.NumLines()
}

func New(path string) (*Stream, error) {
	return NewJSON(path)
}
func NewJSON(path string) (*Stream, error) {
	ma, un := getJSONFuncs("json")
	return newStream(
		path,
		ma,
		un,
	)
}
func NewJSONWithCustomTagKey(path string, tagKey string) (*Stream, error) {
	ma, un := getJSONFuncs(tagKey)
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

func (s *Stream) CreateIndexByUint64(indexName string, intColGetter func(line Line) uint64) error {
	return s.list.CreateIndexByUint64(indexName, func(val []byte) uint64 {
		return intColGetter(Line{
			body:      val,
			unmarshal: s.unmarshal,
			// TODO: add line index, or does not matter?
		})
	})
}

func (s *Stream) HasUint64ByIndex(indexName string, val uint64) bool {
	return s.list.HasUint64ByIndex(indexName, val)
}

func (s *Stream) CreateIndexByInt(indexName string, intColGetter func(line Line) int) error {
	return s.list.CreateIndexByInt(indexName, func(val []byte) int {
		return intColGetter(Line{
			body:      val,
			unmarshal: s.unmarshal,
			// TODO: add line index, or does not matter?
		})
	})
}

func (s *Stream) HasIntByIndex(indexName string, val int) bool {
	return s.list.HasIntByIndex(indexName, val)
}
