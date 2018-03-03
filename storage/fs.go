package storage

import (
	"bufio"
	"github.com/bitly/go-simplejson"
	"os"
	"path"
)

type FsStore struct {
	Base    string
	files   map[string]*os.File
	writers map[string]*bufio.Writer
}

func (s *FsStore) Insert(topic string, data *simplejson.Json) error {
	w, err := s.getOrCreateWriter(topic)
	if err != nil {
		return err
	}
	return s.writeRecord(w, data)
}

func (s *FsStore) Close() error {
	for k, f := range s.files {
		w, _ := s.writers[k]
		w.Flush()
		f.Close()
	}
	return nil
}

func (s *FsStore) getOrCreateWriter(topic string) (*bufio.Writer, error) {
	w, ok := s.writers[topic]
	if ok {
		return w, nil
	} else {
		f, err := os.OpenFile(
			path.Join(s.Base, topic+".jsonlines"),
			os.O_APPEND|os.O_CREATE|os.O_WRONLY,
			0755,
		)
		if err != nil {
			return nil, err
		}
		w := bufio.NewWriter(f)
		s.files[topic] = f
		s.writers[topic] = w
		return w, nil
	}
}

func (s *FsStore) writeRecord(w *bufio.Writer, data *simplejson.Json) error {
	bytes, err := data.MarshalJSON()
	if err != nil {
		return err
	} else {
		w.Write(bytes)
		w.WriteByte('\n')
		w.Flush()
		return nil
	}
}

func NewFsStore(base string) *FsStore {
	return &FsStore{
		base,
		make(map[string]*os.File),
		make(map[string]*bufio.Writer),
	}
}
