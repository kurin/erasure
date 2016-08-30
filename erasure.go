// Package erasure provides a common API for various erasure codes.  It adds
// optional checksumming.
package erasure

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"fmt"
	"io"
	"sort"
	"sync"
)

// Code is a simple abstraction on top of a specific erasure code
// implementation.
type Code interface {
	// Encode accepts a byte slice and returns a slice of sharded data.
	Encode([]byte) ([][]byte, error)

	// Decode accepts a slice of (possibly nil) shards and returns the original
	// byte slice.
	Decode([][]byte) ([]byte, error)

	// Shards returns the total number of data + parity shards employed by this
	// erasure code.
	Shards() int
}

// Writer implements the io.Writer interface.
type Writer struct {
	c    Code
	gen  int
	rs   []io.Reader
	ws   []*io.PipeWriter
	encs []*gob.Encoder
	buf  bytes.Buffer
}

type frame struct {
	Generation int
	Member     int
	SHA1       [20]byte
	Data       []byte
	Size       int
}

type byMember []frame

func (b byMember) Len() int           { return len(b) }
func (b byMember) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byMember) Less(i, j int) bool { return b[i].Member < b[j].Member }

// NewWriter creates a Writer implementing the given erasure code.
func NewWriter(c Code) *Writer {
	w := &Writer{
		c: c,
	}
	w.rs = make([]io.Reader, c.Shards())
	w.ws = make([]*io.PipeWriter, c.Shards())
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

func (w *Writer) send() error {
	parts, err := w.c.Encode(w.buf.Bytes())
	if err != nil {
		return err
	}
	for i := range parts {
		if err := w.encs[i].Encode(frame{
			SHA1:       sha1.Sum(parts[i]),
			Data:       parts[i],
			Generation: w.gen,
			Member:     i,
			Size:       len(w.buf.Bytes()),
		}); err != nil {
			return err
		}
	}
	w.gen++
	w.buf.Reset()
	return nil
}

func (w *Writer) Close() error {
	err := w.send()
	for i := range w.ws {
		w.ws[i].CloseWithError(err)
	}
	return err
}

var ChunkSize int = 1e7

func (w *Writer) Write(p []byte) (int, error) {
	r := ChunkSize - w.buf.Len()
	var n int
	if r < len(p) {
		n, _ = w.buf.Write(p[:r])
		p = p[r:]
		if err := w.send(); err != nil {
			return n, err
		}
	}
	k, _ := w.buf.Write(p)
	n += k
	return n, nil
}

// Reader reconstructs a data stream.
type Reader struct {
	c    Code
	decs []*gob.Decoder
	gen  int
	buf  bytes.Buffer
}

// NewReader creates a new reader.  The Code passed should be identical to the
// Code that created the shards, and each reader should point to one of those
// shards.  Readers can be given in any order.
func NewReader(c Code, readers ...io.Reader) *Reader {
	var ds []*gob.Decoder
	for _, r := range readers {
		ds = append(ds, gob.NewDecoder(r))
	}
	return &Reader{
		c:    c,
		decs: ds,
	}
}

func (r *Reader) pull() error {
	frames := make([]frame, len(r.decs))
	wg := &sync.WaitGroup{}
	for i := range r.decs {
		wg.Add(1)
		go func(i, gen int) {
			// TODO: return errors.  with channels I guess.  ugh.
			defer wg.Done()
			var f frame
			if err := r.decs[i].Decode(&f); err != nil {
				if err == io.EOF {
					return
				}
				fmt.Println(err)
				return
			}
			// TODO: allow sources to get out of sync
			if f.Generation != gen {
				fmt.Println(fmt.Errorf("generation error: %d != %d", f.Generation, r.gen))
				return
			}
			frames[i] = f
		}(i, r.gen)
	}
	wg.Wait()
	r.gen++
	sort.Sort(byMember(frames))
	bs := make([][]byte, r.c.Shards())
	var size int
Frames:
	for _, f := range frames {
		sh := sha1.Sum(f.Data)
		for i := range sh {
			if sh[i] != f.SHA1[i] {
				continue Frames
			}
		}
		bs[f.Member] = f.Data
		size = f.Size
	}
	if size == 0 {
		return nil
	}
	data, err := r.c.Decode(bs)
	if err != nil {
		return err
	}
	r.buf.Write(data[:size])
	return nil
}

func (r *Reader) Read(p []byte) (int, error) {
	if r.buf.Len() == 0 {
		r.buf.Reset()
		if err := r.pull(); err != nil {
			return 0, err
		}
	}
	return r.buf.Read(p)
}
