package statsdinfluxdb

/*

Copyright (c) 2019 Edward Ceaser
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

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	droppedMetrics = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "statsdinfluxdb_dropped_metrics",
		},
	)
	packetSendDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "statsdinfluxdb_packet_send_duration",
			Buckets: prometheus.DefBuckets,
		})
)

type transport struct {
	maxPacketSize int

	buf       []byte
	bufLock   sync.Mutex
	bufPool   *pool
	sendQueue chan []byte

	shutdown     chan struct{}
	shutdownOnce sync.Once
	shutdownWg   sync.WaitGroup

	lostPacketsPeriod  int64
	lostPacketsOverall int64
}

func newTransport(opts *ClientOptions) *transport {
	t := &transport{
		shutdown: make(chan struct{}),
		bufPool:  newPool(opts),
	}

	t.maxPacketSize = opts.MaxPacketSize

	// can't block, we just initialized pool
	t.buf = <-t.bufPool.p
	t.buf = t.buf[0:0]

	t.sendQueue = make(chan []byte, opts.SendQueueCapacity)

	go t.flushLoop(opts.FlushInterval)

	for i := 0; i < opts.SendLoopCount; i++ {
		t.shutdownWg.Add(1)
		go t.sendLoop(opts.Addr, opts.ReconnectInterval, opts.RetryTimeout, opts.Logger)
	}

	if opts.ReportInterval > 0 {
		t.shutdownWg.Add(1)
		go t.reportLoop(opts.ReportInterval, opts.Logger)
	}

	return t
}

func (t *transport) close() {
	t.shutdownOnce.Do(func() {
		close(t.shutdown)
	})
	t.shutdownWg.Wait()
}

// flushLoop makes sure metrics are flushed every flushInterval
func (t *transport) flushLoop(flushInterval time.Duration) {
	var flushC <-chan time.Time

	if flushInterval > 0 {
		flushTicker := time.NewTicker(flushInterval)
		defer flushTicker.Stop()
		flushC = flushTicker.C
	}

	for {
		select {
		case <-t.shutdown:
			t.bufLock.Lock()
			if len(t.buf) > 0 {
				t.flushBuf(len(t.buf))
			}
			t.bufLock.Unlock()

			close(t.sendQueue)
			return
		case <-flushC:
			t.bufLock.Lock()
			if len(t.buf) > 0 {
				t.flushBuf(len(t.buf))
			}
			t.bufLock.Unlock()
		}
	}
}

// sendLoop handles packet delivery over UDP and periodic reconnects
func (t *transport) sendLoop(addr string, reconnectInterval, retryTimeout time.Duration, log SomeLogger) {
	var (
		sock       net.Conn
		err        error
		reconnectC <-chan time.Time
	)

	defer t.shutdownWg.Done()

	if reconnectInterval > 0 {
		reconnectTicker := time.NewTicker(reconnectInterval)
		defer reconnectTicker.Stop()
		reconnectC = reconnectTicker.C
	}

RECONNECT:
	// Attempt to connect
	sock, err = func() (net.Conn, error) {
		// Dial with context which is aborted when client is shut down
		ctx, ctxCancel := context.WithCancel(context.Background())
		defer ctxCancel()

		go func() {
			select {
			case <-t.shutdown:
				ctxCancel()
			case <-ctx.Done():
			}
		}()

		var d net.Dialer
		return d.DialContext(ctx, "udp", addr)
	}()

	if err != nil {
		log.Printf("[STATSD] Error connecting to server: %s", err)
		goto WAIT
	}

	for {
		select {
		case buf, ok := <-t.sendQueue:
			// Get a buffer from the queue
			if !ok {
				_ = sock.Close() // nolint: gosec
				return
			}

			if len(buf) > 0 {
				// cut off \n in the end
				begin := time.Now()
				_, err := sock.Write(buf[0 : len(buf)-1])
				duration := time.Since(begin).Seconds()
				packetSendDuration.Observe(duration)
				if err != nil {
					log.Printf("[STATSD] Error writing to socket: %s", err)
					_ = sock.Close() // nolint: gosec
					goto WAIT
				}
			}

			// return buffer to the pool
			select {
			case t.bufPool.p <- buf:
			default:
				// pool is full, let GC handle the buf
			}
		case <-reconnectC:
			_ = sock.Close() // nolint: gosec
			goto RECONNECT
		}
	}

WAIT:
	// Wait for a while
	select {
	case <-time.After(retryTimeout):
		goto RECONNECT
	case <-t.shutdown:
	}

	// drain send queue waiting for flush loops to terminate
	for range t.sendQueue {
	}
}

// reportLoop reports periodically number of packets lost
func (t *transport) reportLoop(reportInterval time.Duration, log SomeLogger) {
	defer t.shutdownWg.Done()

	reportTicker := time.NewTicker(reportInterval)
	defer reportTicker.Stop()

	for {
		select {
		case <-t.shutdown:
			return
		case <-reportTicker.C:
			lostPeriod := atomic.SwapInt64(&t.lostPacketsPeriod, 0)
			if lostPeriod > 0 {
				log.Printf("[STATSD] %d packets lost (overflow)", lostPeriod)
			}
		}
	}
}

// checkBuf checks current buffer for overflow, and flushes buffer up to lastLen bytes on overflow
//
// overflow part is preserved in flushBuf
func (t *transport) checkBuf(lastLen int) {
	if len(t.buf) > t.maxPacketSize {
		t.flushBuf(lastLen)
	}
}

// flushBuf sends buffer to the queue and initializes new buffer
func (t *transport) flushBuf(length int) {
	sendBuf := t.buf[0:length]
	tail := t.buf[length:len(t.buf)]

	// get new buffer
	select {
	case t.buf = <-t.bufPool.p:
		t.buf = t.buf[0:0]
	default:
		droppedMetrics.Inc()
		return
	}

	// copy tail to the new buffer
	t.buf = append(t.buf, tail...)

	// flush current buffer
	select {
	case t.sendQueue <- sendBuf:
	default:
		// flush failed, we lost some data
		droppedMetrics.Inc()
		lostBuffers.Inc()
		atomic.AddInt64(&t.lostPacketsPeriod, 1)
		atomic.AddInt64(&t.lostPacketsOverall, 1)
	}
}

func init() {
	prometheus.MustRegister(droppedMetrics)
}
