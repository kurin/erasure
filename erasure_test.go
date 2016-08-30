package erasure

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"fmt"
	"hash"
	"io"
	"sync"
	"testing"

	"github.com/kurin/erasure/null"
	"github.com/kurin/erasure/reedsolomon"
)

type random struct{}

func (r random) Read(p []byte) (int, error) {
	n := len(p)
	for len(p) > 0 {
		k := copy(p, []byte{0x01, 0x10, 0xff, 0x00, 0xa0, 0x0a})
		p = p[k:]
	}
	return n, nil
}

func TestReadWrite(t *testing.T) {
	rsCode, err := reedsolomon.New(17, 3)
	if err != nil {
		t.Fatal(err)
	}
	table := []struct {
		c Code
		l int64
	}{
		{
			c: null.Code(10),
			l: 1e8,
		},
		{
			c: null.Code(10),
			l: 31,
		},
		{
			c: rsCode,
			l: 1e8,
		},
	}

	for _, ent := range table {
		r, shaWant := shaReader(ent.l)
		w := NewWriter(ent.c)
		rs := w.Readers()
		rr := NewReader(ent.c, rs...)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		shaGot := sha1.New()
		go func() {
			defer wg.Done()
			if _, err := io.Copy(shaGot, rr); err != nil {
				t.Fatal(err)
			}
		}()
		if _, err := io.Copy(w, r); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		wg.Wait()

		want := fmt.Sprintf("%x", shaWant.Sum(nil))
		got := fmt.Sprintf("%x", shaGot.Sum(nil))
		if got != want {
			t.Fatalf("bad hash, got %q, want %q", got, want)
		}
	}
}

func TestReedSolomon(t *testing.T) {
	table := []struct {
		data, parity int
		size         int64
		kill         []int
		corrupt      []int
	}{
		{
			size:   1e5,
			data:   17,
			parity: 3,
		},
		{
			size:   82,
			data:   1,
			parity: 2,
			kill:   []int{0, 2},
		},
		{
			size:    99,
			data:    1,
			parity:  1,
			corrupt: []int{0},
		},
		{
			size:    1e8,
			data:    17,
			parity:  3,
			kill:    []int{10},
			corrupt: []int{17, 18},
		},
	}

	for _, ent := range table {
		r, shaWant := shaReader(ent.size)
		code, err := reedsolomon.New(ent.data, ent.parity)
		if err != nil {
			t.Error(err)
			continue
		}
		w := NewWriter(code)
		rs := w.Readers()
		rbufs := make([]*bytes.Buffer, len(rs))
		wg := &sync.WaitGroup{}
		for i := range rs {
			rbufs[i] = &bytes.Buffer{}
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if _, err := io.Copy(rbufs[i], rs[i]); err != nil {
					t.Errorf("io.Copy %d: %v", i, err)
				}
			}(i)
		}
		if _, err := io.Copy(w, r); err != nil {
			t.Error(err)
			continue
		}
		if err := w.Close(); err != nil {
			t.Error(err)
			continue
		}
		wg.Wait()

		for _, b := range ent.kill {
			rbufs[b] = nil
		}
		var nrbufs []io.Reader
		for _, buf := range rbufs {
			if buf != nil {
				nrbufs = append(nrbufs, buf)
			}
		}

		for _, c := range ent.corrupt {
			nrbufs[c] = &corruptReader{r: nrbufs[c]}
		}

		rr := NewReader(code, nrbufs...)
		shaGot := sha1.New()
		if _, err := io.Copy(shaGot, rr); err != nil {
			t.Error(err)
			continue
		}
		want := fmt.Sprintf("%x", shaWant.Sum(nil))
		got := fmt.Sprintf("%x", shaGot.Sum(nil))
		if got != want {
			t.Errorf("bad hash, got %q, want %q", got, want)
		}
	}
}

func shaReader(size int64) (io.Reader, hash.Hash) {
	l := io.LimitReader(random{}, size)
	sha := sha1.New()
	r := io.TeeReader(l, sha)
	return r, sha
}

type corruptReader struct {
	r    io.Reader
	buf  bytes.Buffer
	gr   *gob.Decoder
	gw   *gob.Encoder
	once sync.Once
}

func (cr *corruptReader) Read(p []byte) (int, error) {
	cr.once.Do(func() {
		cr.gr = gob.NewDecoder(cr.r)
		cr.gw = gob.NewEncoder(&cr.buf)
	})
	if cr.buf.Len() == 0 {
		var f frame
		if err := cr.gr.Decode(&f); err != nil {
			return 0, err
		}
		if len(f.Data) > 0 {
			f.Data[0] = f.Data[0] + 0x01
		}
		if err := cr.gw.Encode(f); err != nil {
			return 0, err
		}
	}
	return cr.buf.Read(p)
}
