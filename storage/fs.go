package storage

import (
	"bufio"
	"compress/gzip"
	"github.com/bitly/go-simplejson"
	"os"
	"path"
)

type gzipFile struct {
	f      *os.File
	gf     *gzip.Writer
	writer *bufio.Writer
}

type FsStore struct {
	Base    string
	writers map[string]gzipFile
}

func (s *FsStore) Insert(topic string, data *simplejson.Json) error {
	w, err := s.getOrCreateWriter(topic)
	if err != nil {
		return err
	}
	return s.writeRecord(w, data)
}

func (s *FsStore) Close() error {
	for _, w := range s.writers {
		w.writer.Flush()
		w.gf.Close()
		w.f.Close()
	}
	return nil
}

func (s *FsStore) getOrCreateWriter(topic string) (*bufio.Writer, error) {
	w, ok := s.writers[topic]
	if ok {
		return w.writer, nil
	} else {
		f, err := os.OpenFile(
			path.Join(s.Base, topic+".jsonlines.gz"),
			os.O_APPEND|os.O_CREATE|os.O_WRONLY,
			0755,
		)
		if err != nil {
			return nil, err
		}
		gf := gzip.NewWriter(f)
		w := bufio.NewWriter(gf)
		s.writers[topic] = gzipFile{f, gf, w}
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
		make(map[string]gzipFile),
	}
}
