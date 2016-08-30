// Shard provides a very simple interface for sharding data coming from stdin
// and saving it to shard files, or reading from shard files and writing the
// output to stdout.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/kurin/erasure"
	"github.com/kurin/erasure/reedsolomon"
)

var (
	prefix = flag.String("prefix", "", "the file prefix; implies input mode")
	omode  = flag.Bool("output", false, "output mode, pass file names on the command line")
	data   = flag.Int("data", 17, "number of data shards")
	parity = flag.Int("parity", 3, "number of parity shards")
)

var runFunc func() error

func main() {
	flag.Parse()

	if *prefix != "" {
		runFunc = input
	}
	if *omode {
		runFunc = output
	}
	if runFunc == nil {
		fmt.Println("one of -prefix or -output must be specified")
		return
	}

	if err := runFunc(); err != nil {
		log.Print(err)
	}
}

func output() error {
	code, err := reedsolomon.New(*data, *parity)
	if err != nil {
		return err
	}
	var rs []io.Reader
	for _, name := range flag.Args() {
		f, err := os.Open(name)
		if err != nil {
			return err
		}
		defer f.Close()
		rs = append(rs, f)
	}
	r := erasure.NewReader(code, rs...)
	if _, err := io.Copy(os.Stdout, r); err != nil {
		return err
	}
	return nil
}

func input() error {
	code, err := reedsolomon.New(*data, *parity)
	if err != nil {
		return err
	}
	w := erasure.NewWriter(code)
	rs := w.Readers()
	wg := &sync.WaitGroup{}
	for i := range rs {
		f, err := os.Create(fmt.Sprintf("%s.%d", *prefix, i))
		if err != nil {
			return err
		}
		defer func() {
			if err := f.Close(); err != nil {
				log.Print(err)
			}
		}()
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := io.Copy(f, rs[i]); err != nil {
				log.Print(err)
			}
		}(i)
	}
	if _, err := io.Copy(w, os.Stdin); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	wg.Wait()
	return nil
}
