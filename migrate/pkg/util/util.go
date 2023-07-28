package util

import (
	"crypto/md5"
	"io"
)

// MD5Checksum MD5 实现的校验和
func MD5Checksum(val string) string {
	h := md5.New()
	_, _ = io.WriteString(h, val)
	return string(h.Sum(nil))
}
