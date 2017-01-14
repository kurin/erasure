// Package reedsolomon provides a Reed-Solomon erasure code.
package reedsolomon

import (
	"errors"

	"github.com/klauspost/reedsolomon"
)

type Code struct {
	enc       reedsolomon.Encoder
	data, tot int
}

func New(data, parity int) (*Code, error) {
	enc, err := reedsolomon.New(data, parity)
	if err != nil {
		return nil, err
	}
	return &Code{
		enc:  enc,
		tot:  data + parity,
		data: data,
	}, nil
}

func (c *Code) Encode(p []byte) ([][]byte, error) {
	bs, err := c.enc.Split(p)
	if err != nil {
		return nil, err
	}
	if err := c.enc.Encode(bs); err != nil {
		return nil, err
	}
	return bs, nil
}

func (c *Code) Decode(bs [][]byte) ([]byte, error) {
	var redo bool
	for i := range bs[:c.data] {
		if bs[i] == nil {
			redo = true
			break
		}
	}
	if redo {
		if err := c.enc.Reconstruct(bs); err != nil {
			return nil, err
		}
		ok, err := c.enc.Verify(bs)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.New("dataset doesn't verify")
		}
	}
	var b []byte
	for i := range bs[:c.data] {
		b = append(b, bs[i]...)
	}
	return b, nil
}

func (c *Code) Shards() int { return c.tot }
