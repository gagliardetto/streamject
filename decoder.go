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
	index     int64
	body      []byte
	unmarshal UnmarshalFunc
}

func (l *Line) Body() []byte {
	return l.body
}

// Index returns the line number that this object
// was scanned from from the file.
// Starts from 0.
func (l *Line) Index() int64 {
	return l.index
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
	var index int64
	return s.list.IterateLinesAsBytes(func(val []byte) bool {
		line := Line{
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

// Size returns the total size in bytes
// of the stream.
func (s *Stream) Size() int {
	return s.list.Size()
}
func (s *Stream) SizeInt64() int64 {
	return s.list.SizeInt64()
}

// Lines returns the number of objects
// contained in the stream.
func (s *Stream) Lines() int {
	return s.list.Lines()
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
	list, err := listfile.NewWithoutIndex(path)
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

func (s *Stream) CreateIndexOnInt(indexName string, intColGetter func(line Line) int) error {
	return s.list.CreateIndexOnInt(indexName, func(val []byte) int {
		return intColGetter(Line{
			body:      val,
			unmarshal: s.unmarshal,
			// TODO: add line index, or does not matter?
		})
	})
}
func (s *Stream) HasIntByIndex(indexName string, v int) bool {
	return s.list.HasIntByIndex(indexName, v)
}
