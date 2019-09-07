package statsdinfluxdb

import "github.com/prometheus/client_golang/prometheus"

/*

Copyright (c) 2017 Andrey Smirnov

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/

var (
	lostBuffers = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "statsdinfluxdb_lost_buffers",
		})
)

type pool struct {
	poolSize int
	bufSize  int
	p        chan []byte
}

func newPool(opts *ClientOptions) *pool {
	rv := &pool{
		bufSize:  opts.MaxPacketSize + 1024,
		poolSize: opts.BufPoolCapacity,
	}

	pool := make(chan []byte, rv.poolSize)
	rv.p = pool

	return rv
}

func init() {
	prometheus.MustRegister(lostBuffers)
}
