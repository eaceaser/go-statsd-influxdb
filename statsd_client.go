package statsd_influxdb

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

import (
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

// StatsdClient implements statsd client
type StatsdClient struct {
	trans        *transport
	metricPrefix string
	defaultTags  []Tag
	tagFormat    *TagFormat
}

// NewStatsdClient creates new statsd client and starts background processing
//
// StatsdClient connects to statsd server at addr ("host:port")
//
// StatsdClient settings could be controlled via functions of type Option
func NewStatsdClient(addr string, options ...Option) *StatsdClient {
	opts := ClientOptions{
		Addr:              addr,
		MetricPrefix:      DefaultMetricPrefix,
		MaxPacketSize:     DefaultMaxPacketSize,
		FlushInterval:     DefaultFlushInterval,
		ReconnectInterval: DefaultReconnectInterval,
		ReportInterval:    DefaultReportInterval,
		RetryTimeout:      DefaultRetryTimeout,
		Logger:            log.New(os.Stderr, DefaultStatsdLogPrefix, log.LstdFlags),
		BufPoolCapacity:   DefaultBufPoolCapacity,
		SendQueueCapacity: DefaultSendQueueCapacity,
		SendLoopCount:     DefaultSendLoopCount,
		TagFormat:         TagFormatInfluxDB,
	}

	for _, option := range options {
		option(&opts)
	}

	t := newTransport(&opts)

	c := &StatsdClient{
		trans: t,
	}

	c.metricPrefix = opts.MetricPrefix
	c.defaultTags = opts.DefaultTags
	c.tagFormat = opts.TagFormat

	return c
}

// Close stops the client and all its clones. Calling it on a clone has the
// same effect as calling it on the original client - it is stopped with all
// its clones.
func (c *StatsdClient) Close() error {
	c.trans.close()
	return nil
}

// CloneWithPrefix returns a clone of the original client with different metricPrefix.
func (c *StatsdClient) CloneWithPrefix(prefix string) *StatsdClient {
	clone := *c
	clone.metricPrefix = prefix
	return &clone
}

// CloneWithPrefixExtension returns a clone of the original client with the
// original prefixed extended with the specified string.
func (c *StatsdClient) CloneWithPrefixExtension(extension string) *StatsdClient {
	clone := *c
	clone.metricPrefix = clone.metricPrefix + extension
	return &clone
}

// GetLostPackets returns number of packets lost during client lifecycle
func (c *StatsdClient) GetLostPackets() int64 {
	return atomic.LoadInt64(&c.trans.lostPacketsOverall)
}

// Incr increments a counter metric
//
// Often used to note a particular event, for example incoming web request.
func (c *StatsdClient) Incr(stat string, count int64, tags ...Tag) {
	if count != 0 {
		c.trans.bufLock.Lock()
		lastLen := len(c.trans.buf)

		c.trans.buf = append(c.trans.buf, []byte(c.metricPrefix)...)
		c.trans.buf = append(c.trans.buf, []byte(stat)...)
		if c.tagFormat.Placement == TagPlacementName {
			c.trans.buf = c.formatTags(c.trans.buf, tags)
		}
		c.trans.buf = append(c.trans.buf, ':')
		c.trans.buf = strconv.AppendInt(c.trans.buf, count, 10)
		c.trans.buf = append(c.trans.buf, []byte("|c")...)
		if c.tagFormat.Placement == TagPlacementSuffix {
			c.trans.buf = c.formatTags(c.trans.buf, tags)
		}
		c.trans.buf = append(c.trans.buf, '\n')

		c.trans.checkBuf(lastLen)
		c.trans.bufLock.Unlock()
	}
}

// Decr decrements a counter metri
//
// Often used to note a particular event
func (c *StatsdClient) Decr(stat string, count int64, tags ...Tag) {
	c.Incr(stat, -count, tags...)
}

// FIncr increments a float counter metric
func (c *StatsdClient) FIncr(stat string, count float64, tags ...Tag) {
	if count != 0 {
		c.trans.bufLock.Lock()
		lastLen := len(c.trans.buf)

		c.trans.buf = append(c.trans.buf, []byte(c.metricPrefix)...)
		c.trans.buf = append(c.trans.buf, []byte(stat)...)
		if c.tagFormat.Placement == TagPlacementName {
			c.trans.buf = c.formatTags(c.trans.buf, tags)
		}
		c.trans.buf = append(c.trans.buf, ':')
		c.trans.buf = strconv.AppendFloat(c.trans.buf, count, 'f', -1, 64)
		c.trans.buf = append(c.trans.buf, []byte("|c")...)
		if c.tagFormat.Placement == TagPlacementSuffix {
			c.trans.buf = c.formatTags(c.trans.buf, tags)
		}
		c.trans.buf = append(c.trans.buf, '\n')

		c.trans.checkBuf(lastLen)
		c.trans.bufLock.Unlock()
	}
}

// FDecr decrements a float counter metric
func (c *StatsdClient) FDecr(stat string, count float64, tags ...Tag) {
	c.FIncr(stat, -count, tags...)
}

// Timing tracks a duration event, the time delta must be given in milliseconds
func (c *StatsdClient) Timing(stat string, delta int64, tags ...Tag) {
	c.trans.bufLock.Lock()
	lastLen := len(c.trans.buf)

	c.trans.buf = append(c.trans.buf, []byte(c.metricPrefix)...)
	c.trans.buf = append(c.trans.buf, []byte(stat)...)
	if c.tagFormat.Placement == TagPlacementName {
		c.trans.buf = c.formatTags(c.trans.buf, tags)
	}
	c.trans.buf = append(c.trans.buf, ':')
	c.trans.buf = strconv.AppendInt(c.trans.buf, delta, 10)
	c.trans.buf = append(c.trans.buf, []byte("|ms")...)
	if c.tagFormat.Placement == TagPlacementSuffix {
		c.trans.buf = c.formatTags(c.trans.buf, tags)
	}
	c.trans.buf = append(c.trans.buf, '\n')

	c.trans.checkBuf(lastLen)
	c.trans.bufLock.Unlock()
}

// PrecisionTiming track a duration event, the time delta has to be a duration
//
// Usually request processing time, time to run database query, etc. are used with
// this metric type.
func (c *StatsdClient) PrecisionTiming(stat string, delta time.Duration, tags ...Tag) {
	c.trans.bufLock.Lock()
	lastLen := len(c.trans.buf)

	c.trans.buf = append(c.trans.buf, []byte(c.metricPrefix)...)
	c.trans.buf = append(c.trans.buf, []byte(stat)...)
	if c.tagFormat.Placement == TagPlacementName {
		c.trans.buf = c.formatTags(c.trans.buf, tags)
	}
	c.trans.buf = append(c.trans.buf, ':')
	c.trans.buf = strconv.AppendFloat(c.trans.buf, float64(delta)/float64(time.Millisecond), 'f', -1, 64)
	c.trans.buf = append(c.trans.buf, []byte("|ms")...)
	if c.tagFormat.Placement == TagPlacementSuffix {
		c.trans.buf = c.formatTags(c.trans.buf, tags)
	}
	c.trans.buf = append(c.trans.buf, '\n')

	c.trans.checkBuf(lastLen)
	c.trans.bufLock.Unlock()
}

func (c *StatsdClient) igauge(stat string, sign []byte, value int64, tags ...Tag) {
	c.trans.bufLock.Lock()
	lastLen := len(c.trans.buf)

	c.trans.buf = append(c.trans.buf, []byte(c.metricPrefix)...)
	c.trans.buf = append(c.trans.buf, []byte(stat)...)
	if c.tagFormat.Placement == TagPlacementName {
		c.trans.buf = c.formatTags(c.trans.buf, tags)
	}
	c.trans.buf = append(c.trans.buf, ':')
	c.trans.buf = append(c.trans.buf, sign...)
	c.trans.buf = strconv.AppendInt(c.trans.buf, value, 10)
	c.trans.buf = append(c.trans.buf, []byte("|g")...)
	if c.tagFormat.Placement == TagPlacementSuffix {
		c.trans.buf = c.formatTags(c.trans.buf, tags)
	}
	c.trans.buf = append(c.trans.buf, '\n')

	c.trans.checkBuf(lastLen)
	c.trans.bufLock.Unlock()
}

// Gauge sets or updates constant value for the interval
//
// Gauges are a constant data type. They are not subject to averaging,
// and they don’t change unless you change them. That is, once you set a gauge value,
// it will be a flat line on the graph until you change it again. If you specify
// delta to be true, that specifies that the gauge should be updated, not set. Due to the
// underlying protocol, you can't explicitly set a gauge to a negative number without
// first setting it to zero.
func (c *StatsdClient) Gauge(stat string, value int64, tags ...Tag) {
	if value < 0 {
		c.igauge(stat, nil, 0, tags...)
	}

	c.igauge(stat, nil, value, tags...)
}

// GaugeDelta sends a change for a gauge
func (c *StatsdClient) GaugeDelta(stat string, value int64, tags ...Tag) {
	// Gauge Deltas are always sent with a leading '+' or '-'. The '-' takes care of itself but the '+' must added by hand
	if value < 0 {
		c.igauge(stat, nil, value, tags...)
	} else {
		c.igauge(stat, []byte{'+'}, value, tags...)
	}
}

func (c *StatsdClient) fgauge(stat string, sign []byte, value float64, tags ...Tag) {
	c.trans.bufLock.Lock()
	lastLen := len(c.trans.buf)

	c.trans.buf = append(c.trans.buf, []byte(c.metricPrefix)...)
	c.trans.buf = append(c.trans.buf, []byte(stat)...)
	if c.tagFormat.Placement == TagPlacementName {
		c.trans.buf = c.formatTags(c.trans.buf, tags)
	}
	c.trans.buf = append(c.trans.buf, ':')
	c.trans.buf = append(c.trans.buf, sign...)
	c.trans.buf = strconv.AppendFloat(c.trans.buf, value, 'f', -1, 64)
	c.trans.buf = append(c.trans.buf, []byte("|g")...)
	if c.tagFormat.Placement == TagPlacementSuffix {
		c.trans.buf = c.formatTags(c.trans.buf, tags)
	}
	c.trans.buf = append(c.trans.buf, '\n')

	c.trans.checkBuf(lastLen)
	c.trans.bufLock.Unlock()
}

// FGauge sends a floating point value for a gauge
func (c *StatsdClient) FGauge(stat string, value float64, tags ...Tag) {
	if value < 0 {
		c.igauge(stat, nil, 0, tags...)
	}

	c.fgauge(stat, nil, value, tags...)
}

// FGaugeDelta sends a floating point change for a gauge
func (c *StatsdClient) FGaugeDelta(stat string, value float64, tags ...Tag) {
	if value < 0 {
		c.fgauge(stat, nil, value, tags...)
	} else {
		c.fgauge(stat, []byte{'+'}, value, tags...)
	}
}

// SetAdd adds unique element to a set
//
// Statsd server will provide cardinality of the set over aggregation period.
func (c *StatsdClient) SetAdd(stat string, value string, tags ...Tag) {
	c.trans.bufLock.Lock()
	lastLen := len(c.trans.buf)

	c.trans.buf = append(c.trans.buf, []byte(c.metricPrefix)...)
	c.trans.buf = append(c.trans.buf, []byte(stat)...)
	if c.tagFormat.Placement == TagPlacementName {
		c.trans.buf = c.formatTags(c.trans.buf, tags)
	}
	c.trans.buf = append(c.trans.buf, ':')
	c.trans.buf = append(c.trans.buf, []byte(value)...)
	c.trans.buf = append(c.trans.buf, []byte("|s")...)
	if c.tagFormat.Placement == TagPlacementSuffix {
		c.trans.buf = c.formatTags(c.trans.buf, tags)
	}
	c.trans.buf = append(c.trans.buf, '\n')

	c.trans.checkBuf(lastLen)
	c.trans.bufLock.Unlock()
}
