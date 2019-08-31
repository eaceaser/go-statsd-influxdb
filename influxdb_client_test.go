package statsd_influxdb

import (
	"net"
	"testing"
	"time"
)

func TestInfluxDBClient(t *testing.T) {
	inSocket, received := setupListener(t)

	fields := []InfluxDBField{
		StringField("string", "blah"),
		StringField("quotedstring", "\"quoted\""),
		IntField("int", 12),
		FloatField("float", 10.23)}

	tags := []InfluxDBTag{{"herp", "derp"}, {"foo", "bar"}}
	client := NewInfluxDBClient(inSocket.LocalAddr().String())
	ts := time.Unix(1555734000, 0) // 2019-04-20 04:20 UTC

	compareOutput := func(actions func(), expected []string) func(*testing.T) {
		return func(t *testing.T) {
			actions()

			for _, exp := range expected {
				buf := <-received

				if string(buf) != exp {
					t.Errorf("unexpected part received: %#v != %#v", string(buf), exp)
				}
			}
		}
	}

	t.Run("NoTS", compareOutput(
		func() { client.Send("testmetric", tags, fields) },
		[]string{`testmetric,herp=derp,foo=bar string="blah",quotedstring="\"quoted\"",int=12i,float=10.23`}))
	t.Run("TS", compareOutput(
		func() { client.SendWithTimestamp("testmetric", nil, []InfluxDBField{FloatField("test", 1.2)}, ts) },
		[]string{"testmetric test=1.2 1555734000000000000"}))

	_ = client.Close()
	_ = inSocket.Close()
	close(received)
}

func BenchmarkInfluxDBClient(b *testing.B) {
	inSocket, err := net.ListenUDP("udp4", &net.UDPAddr{
		IP: net.IPv4(127, 0, 0, 1),
	})
	if err != nil {
		b.Error(err)
	}

	go func() {
		buf := make([]byte, 1500)
		for {
			_, err := inSocket.Read(buf)
			if err != nil {
				return
			}
		}

	}()

	c := NewInfluxDBClient(inSocket.LocalAddr().String(), MaxPacketSize(1432),
		FlushInterval(100*time.Millisecond), SendLoopCount(2))

	ts := time.Now()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.SendWithTimestamp("test",
			[]InfluxDBTag{{"t1", "v1"}, {"t2", "v2"}},
			[]InfluxDBField{StringField("s", "f"), IntField("i", 420), FloatField("f", 4.20), BoolField("b", true)},
			ts)
	}
	_ = c.Close()
	_ = inSocket.Close()
}
