// Package null provides a null encoding for erasure.
package null

// Code satisfies erasure.Code, and splits the incoming stream into a number of
// shards.  Code provides no redundancy.
type Code int

func (n Code) Encode(p []byte) ([][]byte, error) {
	var bs [][]byte
	size := (len(p) + int(n-1)) / int(n)
	for i := 0; i < int(n); i++ {
		if len(p) >= (i+1)*size {
			bs = append(bs, p[i*size:(i+1)*size])
		} else {
			bs = append(bs, make([]byte, size))
		}
	}
	return bs, nil
}

func (n Code) Decode(ps [][]byte) ([]byte, error) {
	var p []byte
	for i := range ps {
		p = append(p, ps[i]...)
	}
	return p, nil
}

func (n Code) Shards() int {
	return int(n)
}
