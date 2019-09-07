package statsdinfluxdb

import (
	"log"
	"os"
	"strconv"
	"time"
	"unicode/utf8"
)

// InfluxDBClient implements an InfluxDB line protocol client
type InfluxDBClient struct {
	trans *transport
}

// InfluxDB tag represents a key=value tag pair
type InfluxDBTag struct {
	Name  string
	Value string
}

type influxDBFieldType byte

const (
	influxFieldString = iota
	influxFieldInt
	influxFieldFloat
	influxFieldBool
)

// InfluxDBField represents a typed key=value field pair
type InfluxDBField struct {
	name       string
	t          influxDBFieldType
	strvalue   string
	intvalue   int64
	floatvalue float64
	boolvalue  bool
}

// StringField constructs a string-typed InfluxDBField
func StringField(name string, value string) InfluxDBField {
	return InfluxDBField{
		name:     name,
		t:        influxFieldString,
		strvalue: value,
	}
}

// IntField constructs an int-typed InfluxDBField
func IntField(name string, value int64) InfluxDBField {
	return InfluxDBField{
		name:     name,
		t:        influxFieldInt,
		intvalue: value,
	}
}

// FloatField constructs a float-typed InfluxDBField
func FloatField(name string, value float64) InfluxDBField {
	return InfluxDBField{
		name:       name,
		t:          influxFieldFloat,
		floatvalue: value,
	}
}

// BoolField constructs a bool-typed InfluxDBField
func BoolField(name string, value bool) InfluxDBField {
	return InfluxDBField{
		name:      name,
		t:         influxFieldBool,
		boolvalue: value,
	}
}

// Append appends the representation of the InfluxDBField to the given buffer
func (f *InfluxDBField) Append(buf []byte) []byte {
	buf = quoteString(buf, f.name, true)
	buf = append(buf, '=')

	switch f.t {
	case influxFieldString:
		buf = strconv.AppendQuote(buf, f.strvalue)
	case influxFieldInt:
		buf = strconv.AppendInt(buf, f.intvalue, 10)
		buf = append(buf, 'i')
	case influxFieldFloat:
		buf = strconv.AppendFloat(buf, f.floatvalue, 'f', -1, 64)
	case influxFieldBool:
		if f.boolvalue {
			buf = append(buf, 't')
		} else {
			buf = append(buf, 'f')
		}
	}

	return buf
}

// NewInfluxDBClient returns a newly constructed UDP client for the InfluxDB line protocol
func NewInfluxDBClient(addr string, options ...Option) *InfluxDBClient {
	opts := ClientOptions{
		Addr:              addr,
		MetricPrefix:      DefaultMetricPrefix,
		MaxPacketSize:     DefaultMaxPacketSize,
		FlushInterval:     DefaultFlushInterval,
		ReconnectInterval: DefaultReconnectInterval,
		ReportInterval:    DefaultReportInterval,
		RetryTimeout:      DefaultRetryTimeout,
		Logger:            log.New(os.Stderr, DefaultInfluxDBLogPrefix, log.LstdFlags),
		BufPoolCapacity:   DefaultBufPoolCapacity,
		SendQueueCapacity: DefaultSendQueueCapacity,
		SendLoopCount:     DefaultSendLoopCount,
		TagFormat:         StatsdTagFormatInfluxDB,
	}

	for _, option := range options {
		option(&opts)
	}

	t := newTransport(&opts)

	c := &InfluxDBClient{
		trans: t,
	}

	return c
}

// Close closes the InfluxDB client connection
func (c *InfluxDBClient) Close() error {
	c.trans.close()
	return nil
}

// Send sends a non-timestamped metric
func (c *InfluxDBClient) Send(measurement string, tags []InfluxDBTag, fields []InfluxDBField) {
	c.append(measurement, tags, fields, nil)
}

// SendWithTimestamp sends a timestamped metric
func (c *InfluxDBClient) SendWithTimestamp(measurement string, tags []InfluxDBTag, fields []InfluxDBField, ts time.Time) {
	c.append(measurement, tags, fields, &ts)
}

func (c *InfluxDBClient) append(measurement string, tags []InfluxDBTag, fields []InfluxDBField, ts *time.Time) {
	if len(fields) == 0 {
		return
	}

	var buf []byte
	select {
	case buf = <-c.trans.bufPool.p:
	default:
		return
	}

	buf = buf[0:0]

	buf = quoteString(buf, measurement, false)
	if len(tags) > 0 {
		buf = appendTags(buf, tags)
	}
	buf = append(buf, ' ')

	for i, field := range fields {
		buf = field.Append(buf)

		if i < len(fields)-1 {
			buf = append(buf, ',')
		}
	}

	if ts != nil {
		buf = append(buf, ' ')
		buf = strconv.AppendInt(buf, ts.UnixNano(), 10)
	}

	buf = append(buf, '\n')

	c.trans.bufLock.Lock()
	lastLen := len(c.trans.buf)
	c.trans.buf = append(c.trans.buf, buf...)
	c.trans.checkBuf(lastLen)
	c.trans.bufLock.Unlock()
	c.trans.bufPool.p <- buf
}

func appendTags(buf []byte, tags []InfluxDBTag) []byte {
	for _, tag := range tags {
		buf = append(buf, ',')
		buf = quoteString(buf, tag.Name, true)
		buf = append(buf, '=')
		buf = quoteString(buf, tag.Value, true)
	}
	return buf
}

func quoteString(buf []byte, s string, escapeEquals bool) []byte {
	for _, r := range s {
		switch {
		case r == '\\':
			buf = append(buf, '\\', '\\')
		case r == ',':
			buf = append(buf, '\\', ',')
		case escapeEquals && r == '=':
			buf = append(buf, '\\', '=')
		case r == ' ':
			buf = append(buf, '\\', ' ')
		default:
			var tmp [utf8.UTFMax]byte
			cnt := utf8.EncodeRune(tmp[:], r)
			buf = append(buf, tmp[:cnt]...)
		}
	}
	return buf
}
