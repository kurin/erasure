package erasure

import (
	"crypto/sha1"
	"fmt"
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
			c: rsCode,
			l: 1e8,
		},
	}

	for i, ent := range table {
		l := io.LimitReader(random{}, ent.l)
		shaWant := sha1.New()
		r := io.TeeReader(l, shaWant)
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
			t.Fatalf("%d: bad hash, got %q, want %q", i, got, want)
		}
	}
}
