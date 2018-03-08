package storage

import (
	"bufio"
	"compress/gzip"
	"github.com/bitly/go-simplejson"
	"os"
	"path"
	"time"
)

type GzipFile struct {
	path   string
	f      *os.File
	gf     *gzip.Writer
	writer *bufio.Writer
}

func makeGzipFile(fullPath string, append bool) (*GzipFile, error) {
	openFlag := os.O_CREATE | os.O_WRONLY
	if append {
		openFlag = openFlag | os.O_APPEND
	}
	f, err := os.OpenFile(fullPath, openFlag, 0755)
	if err != nil {
		return nil, err
	}
	gf := gzip.NewWriter(f)
	w := bufio.NewWriter(gf)
	return &GzipFile{fullPath, f, gf, w}, nil
}

func (gzFile *GzipFile) Close() error {
	gzFile.writer.Flush()
	gzFile.gf.Close()
	gzFile.f.Close()
	return nil
}

func (gzFile *GzipFile) WriteRecord(bytes []byte) error {
	gzFile.writer.Write(bytes)
	gzFile.writer.WriteByte('\n')
	gzFile.writer.Flush()
	return nil
}

type FsStore struct {
	Base  string
	files map[string]*GzipFile
}

func (s *FsStore) Insert(topic string, data *simplejson.Json) error {
	w, err := s.getOrCreateWriter(topic, data)
	if err != nil {
		return err
	}
	bytes, err := data.MarshalJSON()
	if err != nil {
		return err
	}
	return w.WriteRecord(bytes)
}

func (s *FsStore) makeFileName(topic string, data *simplejson.Json) string {
	currentHour := time.Now().Format("2006010215")
	return path.Join(
		s.Base,
		topic+"-"+currentHour+".jsonlines.gz",
	)
}

func (s *FsStore) Close() error {
	for _, gzf := range s.files {
		gzf.Close()
	}
	return nil
}

func (s *FsStore) getOrCreateWriter(topic string, data *simplejson.Json) (*GzipFile, error) {
	fullPath := s.makeFileName(topic, data)
	gzf, ok := s.files[topic]
	if ok {
		if gzf.path == fullPath {
			return gzf, nil
		} else {
			gzf.Close()
			gzf = nil
			delete(s.files, topic)
		}
	}
	gzf, err := makeGzipFile(fullPath, true)
	if err != nil {
		return nil, err
	}
	s.files[topic] = gzf
	return gzf, nil
}

func NewFsStore(base string) *FsStore {
	os.MkdirAll(base, 0755)
	return &FsStore{
		base,
		make(map[string]*GzipFile),
	}
}
