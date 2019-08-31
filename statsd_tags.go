package statsd_influxdb

/*

Copyright (c) 2018 Andrey Smirnov

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

import "strconv"

// StatsdTag placement constants
const (
	TagPlacementName = iota
	TagPlacementSuffix
)

// StatsdTagFormat controls tag formatting style
type StatsdTagFormat struct {
	// FirstSeparator is put after metric name and before first tag
	FirstSeparator string
	// Placement specifies part of the message to append tags to
	Placement byte
	// OtherSeparator separates 2nd and subsequent tags from each other
	OtherSeparator byte
	// KeyValueSeparator separates tag name and tag value
	KeyValueSeparator byte
}

// StatsdTag types
const (
	typeString = iota
	typeInt64
)

// StatsdTag is metric-specific tag
type StatsdTag struct {
	name     string
	strvalue string
	intvalue int64
	typ      byte
}

// Append formats tag and appends it to the buffer
func (tag StatsdTag) Append(buf []byte, style *StatsdTagFormat) []byte {
	buf = append(buf, []byte(tag.name)...)
	buf = append(buf, style.KeyValueSeparator)
	if tag.typ == typeString {
		return append(buf, []byte(tag.strvalue)...)
	}
	return strconv.AppendInt(buf, tag.intvalue, 10)
}

// StringTag creates StatsdTag with string value
func StringTag(name, value string) StatsdTag {
	return StatsdTag{name: name, strvalue: value, typ: typeString}
}

// IntTag creates StatsdTag with integer value
func IntTag(name string, value int) StatsdTag {
	return StatsdTag{name: name, intvalue: int64(value), typ: typeInt64}
}

// Int64Tag creates StatsdTag with integer value
func Int64Tag(name string, value int64) StatsdTag {
	return StatsdTag{name: name, intvalue: value, typ: typeInt64}
}

func (c *StatsdClient) formatTags(buf []byte, tags []StatsdTag) []byte {
	tagsLen := len(c.defaultTags) + len(tags)
	if tagsLen == 0 {
		return buf
	}

	buf = append(buf, []byte(c.tagFormat.FirstSeparator)...)
	for i := range c.defaultTags {
		buf = c.defaultTags[i].Append(buf, c.tagFormat)
		if i != tagsLen-1 {
			buf = append(buf, c.tagFormat.OtherSeparator)
		}
	}

	for i := range tags {
		buf = tags[i].Append(buf, c.tagFormat)
		if i+len(c.defaultTags) != tagsLen-1 {
			buf = append(buf, c.tagFormat.OtherSeparator)
		}
	}

	return buf
}

var (
	// StatsdTagFormatInfluxDB is format for InfluxDB StatsD telegraf plugin
	//
	// Docs: https://github.com/influxdata/telegraf/tree/master/plugins/inputs/statsd
	StatsdTagFormatInfluxDB = &StatsdTagFormat{
		Placement:         TagPlacementName,
		FirstSeparator:    ",",
		OtherSeparator:    ',',
		KeyValueSeparator: '=',
	}

	// StatsdTagFormatDatadog is format for DogStatsD (Datadog Agent)
	//
	// Docs: https://docs.datadoghq.com/developers/dogstatsd/#datagram-format
	StatsdTagFormatDatadog = &StatsdTagFormat{
		Placement:         TagPlacementSuffix,
		FirstSeparator:    "|#",
		OtherSeparator:    ',',
		KeyValueSeparator: ':',
	}
)
