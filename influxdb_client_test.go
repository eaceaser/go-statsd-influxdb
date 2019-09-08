package statsdinfluxdb

import (
	"net"
	"testing"
	"time"
)

func setupTcpListener(t *testing.T) (net.Listener, chan []byte) {
	l, err := net.ListenTCP("tcp4", &net.TCPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
	})

	if err != nil {
		t.Fatal(err)
	}

	received := make(chan []byte)

	go func() {
		// accept 1 connection
		conn, err := l.Accept()
		if err != nil {
			t.Error(err)
		}

		for {
			var buf [1500]byte

			n, err := conn.Read(buf[:])
			if err != nil {
				return

			}

			received <- buf[0:n]
		}
	}()

	return l, received
}

func TestInfluxDBClient(t *testing.T) {
	inSocket, received := setupListener(t)

	fields := []InfluxDBField{
		StringField("string", "blah"),
		StringField("quotedstring", "\"quoted\""),
		IntField("int", 12),
		FloatField("float", 10.23)}

	tags := []InfluxDBTag{{"herp", "derp"}, {"foo", "bar"}, {"herp,", "esc=aped"}}
	client := NewInfluxDBClient("udp://" + inSocket.LocalAddr().String())
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
		[]string{`testmetric,herp=derp,foo=bar,herp\,=esc\=aped string="blah",quotedstring="\"quoted\"",int=12i,float=10.23`}))
	t.Run("TS", compareOutput(
		func() { client.SendWithTimestamp("testmetric", nil, []InfluxDBField{FloatField("test", 1.2)}, ts) },
		[]string{"testmetric test=1.2 1555734000000000000"}))

	_ = client.Close()
	_ = inSocket.Close()
	close(received)
}

func TestInfluxDBTcpClient(t *testing.T) {
	listener, received := setupTcpListener(t)

	fields := []InfluxDBField{
		StringField("string", "blah"),
		StringField("quotedstring", "\"quoted\""),
		IntField("int", 12),
		FloatField("float", 10.23)}

	tags := []InfluxDBTag{{"herp", "derp"}, {"foo", "bar"}, {"herp,", "esc=aped"}}
	client := NewInfluxDBClient("tcp://"+listener.Addr().String())
	ts := time.Unix(1555734000, 0) // 2019-04-20 04:20 UTC
	client.SendWithTimestamp("testmetric", tags, fields, ts)

	buf := <-received
	if string(buf) !=
		`testmetric,herp=derp,foo=bar,herp\,=esc\=aped string="blah",quotedstring="\"quoted\"",int=12i,float=10.23 1555734000000000000` + "\n" {
		t.Fatalf("%s does not match expected value", string(buf))
	}

	_ = client.Close()
	_ = listener.Close()
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

	c := NewInfluxDBClient("udp://"+inSocket.LocalAddr().String(), MaxPacketSize(1432),
		FlushInterval(100*time.Millisecond), SendLoopCount(2), BufPoolCapacity(1024))

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

func TestQuoteString(t *testing.T) {
	cmp := func(src, expected string) func(*testing.T) {
		return func(t *testing.T) {
			buf := make([]byte, 0)
			buf = quoteString(buf, src, true)
			str := string(buf)
			if str != expected {
				t.Errorf("%s did not match %s", str, expected)
			}
		}
	}

	t.Run("unescaped", cmp("derp", "derp"))
	t.Run("backslashes", cmp("str\\ing\\\\", "str\\\\ing\\\\\\\\"))
	t.Run("spaces_commas", cmp("str in,g", "str\\ in\\,g"))
	t.Run("equals", cmp("str=ing", "str\\=ing"))
	t.Run("unicode", cmp("st=r🍁i,ng", "st\\=r🍁i\\,ng"))
	t.Run("unescaped_equals", func(t *testing.T) {
		src := "str=ing"
		expected := "str=ing"
		buf := make([]byte, 0)
		buf = quoteString(buf, src, false)
		str := string(buf)
		if str != expected {
			t.Errorf("%s did not match %s", str, expected)
		}
	})
}
