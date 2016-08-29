// Package reedsolomon provides a Reed-Solomon erasure code.
package reedsolomon

import "github.com/klauspost/reedsolomon"

type Code struct {
	enc reedsolomon.Encoder
	tot int
}

func New(data, parity int) (*Code, error) {
	enc, err := reedsolomon.New(data, parity)
	if err != nil {
		return nil, err
	}
	return &Code{
		enc: enc,
		tot: data + parity,
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
	if err := c.enc.Reconstruct(bs); err != nil {
		return nil, err
	}
	var b []byte
	for i := range bs {
		b = append(b, bs[i]...)
	}
	return b, nil
}

func (c *Code) Shards() int { return c.tot }
