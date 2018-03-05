package common

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"time"
)

/// 取毫秒时间戳
func GetUinxMillisecond() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

/// 解压gzip的数据
func unGzipData(buf []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(r)
}
