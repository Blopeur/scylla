package lib

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"
)

// Test the creation of listener, connection to it and reception of a message via a throttled net.Conn
// Strange behavior, if it use a standard listener, i can start the go routing before the listener
// But if i use a Throttled listener i need to start the go routing after creating it, Initialisation takes too much time ??
func TestThrottledListener(t *testing.T) {
	message := "Hi there!\n"
	l, err := net.Listen("tcp", ":3000")
	if err != nil {
		t.Fatal(err)
	}
	tl := NewThrottledListener(l, 1024, 1024, 1024)
	defer tl.Close()

	go func() {
		conn, err := net.Dial("tcp", ":3000")
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := fmt.Fprintf(conn, message); err != nil {
			t.Fatal(err)
		}
	}()

	for {
		conn, err := tl.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf, err := ioutil.ReadAll(conn)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(string(buf[:]))
		if msg := string(buf[:]); msg != message {
			t.Fatalf("Unexpected message:\nGot:\t\t%s\nExpected:\t%s\n", msg, message)
		}
		return
	}
}

// Benchmark Listener
func BenchmarkNewThrottledListener(b *testing.B) {
	toCopy := 10 * 1024
	go func() {
		l, err := net.Listen("tcp", ":3000")
		if err != nil {
			b.Fatal(err)
		}
		tl := NewThrottledListener(l, 1024*1024, 1024, 1024)
		defer tl.Close()
		for {
			conn, err := tl.Accept()
			if err != nil {
				b.Fatal("tcp server accept error", err)
			}
			go func(conn2 net.Conn) {
				buf, err := ioutil.ReadAll(conn)
				if err != nil {
					b.Fatal(err)
				}
				if len(buf) != toCopy {
					b.Fatal("wrong buffer length", len(buf), toCopy)
				}
			}(conn)
		}
	}()
	copyToListener := func(bytesToCopy int, connCount int) {
		b.StopTimer()
		files := make([]*os.File, connCount)
		conns := make([]net.Conn, connCount)
		for i := 0; i != connCount; i++ {
			const fileName = "/dev/zero"
			file, err := os.Open(fileName)
			assertNil(err, nil)
			defer file.Close()
			files[i] = file
			conn, err := net.Dial("tcp", ":3000")
			if err != nil {
				b.Fatal(err)
			}
			conns[i] = conn
			defer conn.Close()
		}
		var dst bytes.Buffer
		b.StartTimer()
		for i, file := range files {
			dst.Reset()
			written, err := io.CopyN(conns[i], file, int64(bytesToCopy))
			assertNil(err, nil)
			if written != int64(bytesToCopy) {
				b.Fatalf("Should have copied %v but only copied %v", bytesToCopy, written)
			}
		}
	}

	//for some reason if we try to write to fast, the connection is not always up
	time.Sleep(time.Second)
	// get enough info
	for i := 0; i != b.N; i++ {
		for j := 0; j != 2; j++ {

			conns := 10
			if j == 0 {
				// More reader than bytes
				conns *= 10
			} else {
				// More byte than readers
				toCopy *= 10
			}
			copyToListener(toCopy, conns)
		}
	}

}
