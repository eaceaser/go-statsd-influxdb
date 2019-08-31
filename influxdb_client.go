package statsd_influxdb

import (
	"log"
	"os"
	"strconv"
	"time"
)

type InfluxDBClient struct {
	trans *transport
}

type InfluxTag struct {
	Name  string
	Value string
}

type influxFieldType byte

const (
	influxFieldString = iota
	influxFieldInt
	influxFieldFloat
	influxFieldBool
)

type InfluxField struct {
	name       string
	t          influxFieldType
	strvalue   string
	intvalue   int64
	floatvalue float64
	boolvalue  bool
}

func StringField(name string, value string) InfluxField {
	return InfluxField{
		name:     name,
		t:        influxFieldString,
		strvalue: value,
	}
}

func IntField(name string, value int64) InfluxField {
	return InfluxField{
		name:     name,
		t:        influxFieldInt,
		intvalue: value,
	}
}

func FloatField(name string, value float64) InfluxField {
	return InfluxField{
		name:       name,
		t:          influxFieldFloat,
		floatvalue: value,
	}
}

func BoolField(name string, value bool) InfluxField {
	return InfluxField{
		name:      name,
		t:         influxFieldBool,
		boolvalue: value,
	}
}

func (f *InfluxField) Append(buf []byte) []byte {
	buf = append(buf, []byte(f.name)...)
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
		TagFormat:         TagFormatInfluxDB,
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

func (c *InfluxDBClient) Close() error {
	c.trans.close()
	return nil
}

func (c *InfluxDBClient) Send(measurement string, tags []InfluxTag, fields []InfluxField) {
	c.append(measurement, tags, fields, nil)
}

func (c *InfluxDBClient) SendWithTimestamp(measurement string, tags []InfluxTag, fields []InfluxField, ts time.Time) {
	c.append(measurement, tags, fields, &ts)
}

func (c *InfluxDBClient) append(measurement string, tags []InfluxTag, fields []InfluxField, ts *time.Time) {
	if len(fields) == 0 {
		return
	}

	c.trans.bufLock.Lock()
	lastLen := len(c.trans.buf)

	c.trans.buf = append(c.trans.buf, []byte(measurement)...)
	if len(tags) > 0 {
		c.trans.buf = appendTags(c.trans.buf, tags)
	}
	c.trans.buf = append(c.trans.buf, ' ')

	for i, field := range fields {
		c.trans.buf = field.Append(c.trans.buf)

		if i < len(fields)-1 {
			c.trans.buf = append(c.trans.buf, ',')
		}
	}

	if ts != nil {
		c.trans.buf = append(c.trans.buf, ' ')
		c.trans.buf = strconv.AppendInt(c.trans.buf, ts.UnixNano(), 10)
	}

	c.trans.buf = append(c.trans.buf, '\n')

	c.trans.checkBuf(lastLen)
	c.trans.bufLock.Unlock()
}

func appendTags(buf []byte, tags []InfluxTag) []byte {
	for _, tag := range tags {
		buf = append(buf, ',')
		buf = append(buf, []byte(tag.Name)...)
		buf = append(buf, '=')
		buf = append(buf, []byte(tag.Value)...)
	}
	return buf
}
