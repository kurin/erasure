// Package erasure provides a common API for various erasure codes.  It adds
// optional checksumming.
package erasure

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"io"
)

// Code is a simple abstraction on top of a specific erasure code
// implementation.
type Code interface {
	// Encode accepts a byte slice and returns a slice of sharded data.
	Encode([]byte) [][]byte

	// Decode accepts a slice of (possibly nil) shards and returns the original
	// byte slice.
	Decode([][]byte) []byte

	// Shards returns the total number of data + parity shards employed by this
	// erasure code.
	Shards() int
}

// Writer implements the io.Writer interface.
type Writer struct {
	c    Code
	rs   []io.Reader
	ws   []io.Writer
	encs []*gob.Encoder
	buf  bytes.Buffer
}

type frame struct {
	SHA1 [20]byte
	Data []byte
}

// NewWriter creates a Writer implementing the given erasure code.
func NewWriter(c Code) *Writer {
	w := &Writer{
		c: c,
	}
	w.rs = make([]io.Reader, c.Shards())
	w.ws = make([]io.Writer, c.Shards())
	w.encs = make([]*gob.Encoder, c.Shards())
	for i := 0; i < c.Shards(); i++ {
		w.rs[i], w.ws[i] = io.Pipe()
		w.encs[i] = gob.NewEncoder(w.ws[i])
	}
	return w
}

// Readers returns Code.Shards() readers.  Each reader corresponds to one shard
// from the erasure code.  Readers must be read concurrently with writes.
func (w *Writer) Readers() []io.Reader {
	return w.rs
}

func (w *Writer) Write(p []byte) (int, error) {
	r := 1e7 - w.buf.Len()
	var n int
	if r < len(p) {
		n, _ = w.buf.Write(p[:r])
		p = p[r:]
		parts := w.c.Encode(w.buf.Bytes())
		for i := range parts {
			w.encs[i].Encode(frame{
				SHA1: sha1.Sum(parts[i]),
				Data: parts[i],
			})
		}
		w.buf.Reset()
	}
	k, _ := w.buf.Write(p)
	n += k
	return n, nil
}
