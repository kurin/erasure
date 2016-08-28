package erasure

import (
	"io"
	"io/ioutil"
	"testing"
)

type null int

func (n null) Encode(p []byte) [][]byte {
	var bs [][]byte
	size := len(p) / int(n)
	for i := 0; i < int(n); i++ {
		bs = append(bs, p[i*size:(i+1)*size])
	}
	return bs
}

func (n null) Decode(ps [][]byte) []byte {
	var p []byte
	for i := range ps {
		p = append(p, ps[i]...)
	}
	return p
}

func (n null) Shards() int {
	return int(n)
}

type random struct{}

func (r random) Read(p []byte) (int, error) {
	var n int
	for n < len(p) {
		n += copy(p, []byte{0x01, 0x10, 0xff, 0x00, 0xa0, 0x0a})
	}
	return len(p), nil
}

func TestWriter(t *testing.T) {
	k := null(5)
	r := io.LimitReader(random{}, 1e8)
	w := NewWriter(k)
	rs := w.Readers()
	for i := range rs {
		go io.Copy(ioutil.Discard, rs[i])
	}
	if _, err := io.Copy(w, r); err != nil {
		t.Fatal(err)
	}
}
