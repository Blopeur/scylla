package lib

import (
	"bytes"
	"errors"
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	defaultRate  = 100 * 1024
	defaultBurst = 100 * 1024
)

func assertNil(i interface{}, t *testing.T) {
	if i != nil {
		if t != nil {
			t.Fatalf("Expecting %v to be nil but it isn't.", i)
		} else {
			panic(fmt.Sprintf("Expecting %v to be nil but it isn't.", i))
		}
	}
}

func assertNotNil(i interface{}, t *testing.T) {
	if i == nil {
		if t != nil {
			t.Fatalf("Expecting %v to be not nil but it is.", i)
		} else {
			panic(fmt.Sprintf("Expecting %v to be not nil but it is.", i))
		}
	}
}

func createTcpPipe(t *testing.T) (net.Conn, net.Conn) {
	addr := "localhost:8080"
	conn := make(chan net.Conn)
	ln, err := net.Listen("tcp", addr)
	assertNil(err, t)
	defer ln.Close()
	go func() {
		server, _ := ln.Accept()
		conn <- server
	}()
	client, err := net.Dial("tcp", addr)
	assertNil(err, t)
	return client, <-conn
}

func TestBasicOps(t *testing.T) {
	pool := NewIOThrottlerPool(defaultRate, defaultBurst)
	assertNotNil(pool, t)
	ids := pool.GetIDs()
	if len(ids) != 0 {
		t.Fatalf("Expecting %v to be not got %v.", 0, len(ids))
	}

	// make sure we error out looking for a non existent buffer
	_, _, err := pool.GetLimitByID("reader")
	assertNotNil(err, t)

	// make sure we can change the global limits
	r, b := pool.GetGlobalLimit()
	if r != defaultRate {
		t.Fatalf("Expecting %v to be not got %v.", defaultRate, r)
	}
	if b != defaultBurst {
		t.Fatalf("Expecting %v to be not got %v.", defaultBurst, b)
	}

	pool.SetGlobalLimit(defaultRate*2, defaultBurst*2)
	r, b = pool.GetGlobalLimit()
	if r != defaultRate*2 {
		t.Fatalf("Expecting %v to be not got %v.", defaultRate*2, r)
	}
	if b != defaultBurst*2 {
		t.Fatalf("Expecting %v to be not got %v.", defaultBurst*2, b)
	}

	test := []byte("test")
	rc := ioutil.NopCloser(bytes.NewReader(test))
	reader := pool.NewThrottledReadCloser(rc, defaultRate, defaultBurst, "reader")
	assertNotNil(reader, t)
	ids = pool.GetIDs()
	if len(ids) != 1 {
		t.Fatalf("Expecting %v to be not got %v.", 1, len(ids))
	}
	r, b, err = pool.GetLimitByID("reader")
	assertNil(err, t)

	if r != defaultRate {
		t.Fatalf("Expecting %v to be not got %v.", defaultRate, r)
	}
	if b != defaultBurst {
		t.Fatalf("Expecting %v to be not got %v.", defaultBurst, b)
	}
	err = pool.SetLimitByID(defaultRate*2, defaultBurst*2, "reader")
	assertNil(err, t)
	r, b, err = pool.GetLimitByID("reader")
	assertNil(err, t)

	if r != defaultRate*2 {
		t.Fatalf("Expecting %v to be not got %v.", defaultRate*2, r)
	}
	if b != defaultBurst*2 {
		t.Fatalf("Expecting %v to be not got %v.", defaultBurst*2, b)
	}

	// make sure we can read the buffer
	buff := make([]byte, 200)
	n, err := reader.Read(buff)
	assertNil(err, t)
	if n != len(test) {
		t.Fatalf("Expecting %v to be not got %v.", len(test), n)
	}

	//close the buffer and make sure we have no more buffer
	reader.Close()
	ids = pool.GetIDs()
	if len(ids) != 0 {
		t.Fatalf("Expecting %v to be not got %v.", 0, len(ids))
	}

	// make sure that the original is closed
	n, err = rc.Read(test)
	if n != 0 {
		t.Fatalf("Expected to read %v but got %v", 0, n)
	}
	assertNotNil(err, t)

	// add two buffer
	r1 := pool.NewThrottledReadCloser(rc, defaultRate, defaultBurst, "r1")
	assertNotNil(reader, t)
	ids = pool.GetIDs()
	if len(ids) != 1 {
		t.Fatalf("Expecting %v to be not got %v", 1, len(ids))
	}
	r2 := pool.NewThrottledReadCloser(rc, defaultRate, defaultBurst, "r2")
	assertNotNil(reader, t)
	ids = pool.GetIDs()
	if len(ids) != 2 {
		t.Fatalf("Expecting %v to be not got %v", 2, len(ids))
	}

	pool.SetLimitForAll(defaultRate*2, defaultBurst*2)
	r, b, err = pool.GetLimitByID("r1")
	assertNil(err, t)

	if r != defaultRate*2 {
		t.Fatalf("Expecting %v to be not got %v", defaultRate*2, r)
	}
	if b != defaultBurst*2 {
		t.Fatalf("Expecting %v to be not got %v", defaultBurst*2, b)
	}
	r, b, err = pool.GetLimitByID("r2")
	assertNil(err, t)

	if r != defaultRate*2 {
		t.Fatalf("Expecting %v to be not got %v", defaultRate*2, r)
	}
	if b != defaultBurst*2 {
		t.Fatalf("Expecting %v to be not got %v", defaultBurst*2, b)
	}
	r2.Close()
	ids = pool.GetIDs()
	if len(ids) != 1 {
		t.Fatalf("Expecting %v to be not got %v", 1, len(ids))
	}
	r1.Close()
	ids = pool.GetIDs()
	if len(ids) != 0 {
		t.Fatalf("Expecting %v to be not got %v", 0, len(ids))
	}

	// make sure we can close net.Conn
	client, server := createTcpPipe(t)
	defer client.Close()
	defer server.Close()
	rtcp := pool.NewThrottledReadCloser(server, defaultRate, defaultBurst, server.RemoteAddr().String())
	ids = pool.GetIDs()
	if len(ids) != 1 {
		t.Fatalf("Expecting %v to be not got %v", 1, len(ids))
	}
	if ids[0] != server.RemoteAddr().String() {
		t.Fatalf("Expecting %v to be not got %v", server.RemoteAddr().String(), ids[0])
	}
	rtcp.Close()
	ids = pool.GetIDs()
	if len(ids) != 0 {
		t.Fatalf("Expecting %v to be not got %v", 0, len(ids))
	}
	n, err = rtcp.Read(test)
	if n != 0 {
		t.Fatalf("Expected to read %v but got %v", 0, n)
	}
	assertNotNil(err, t)
}

func TestBandwidthBasicOps(t *testing.T) {
	pool, err := NewBandwidthThrottlerPool(1, defaultBurst)
	assertNotNil(err, t)
	assertNil(pool, t)

	pool, err = NewBandwidthThrottlerPool(defaultRate, 1)
	assertNotNil(err, t)
	assertNil(pool, t)

	pool, err = NewBandwidthThrottlerPool(defaultRate, defaultBurst)
	assertNil(err, t)
	assertNotNil(pool, t)

	ids := pool.GetIDs()
	if len(ids) != 0 {
		t.Fatalf("Expecting %v to be not got %v.", 0, len(ids))
	}

	test := []byte("test")
	rc := ioutil.NopCloser(bytes.NewReader(test))
	reader, err := pool.NewBandwidthThrottledReadCloser(rc, 1, defaultBurst, "reader")
	assertNotNil(err, t)
	assertNil(reader, t)
	reader, err = pool.NewBandwidthThrottledReadCloser(rc, defaultRate, defaultBurst, "reader")
	assertNil(err, t)
	assertNotNil(reader, t)

	ids = pool.GetIDs()
	if len(ids) != 1 {
		t.Fatalf("Expecting %v to be not got %v.", 1, len(ids))
	}
	reader.Close()
	ids = pool.GetIDs()
	if len(ids) != 0 {
		t.Fatalf("Expecting %v to be not got %v.", 0, len(ids))
	}

}

func elapsedSeconds(t time.Time) int64 {
	return int64(time.Duration(time.Now().Sub(t)).Seconds())
}

func timeTransfer(data []byte, reader io.Reader, writer io.Writer) (int64, error) {

	serverError := make(chan error)
	// Write the data
	go func() {
		count, err := writer.Write(data)
		if count < len(data) {
			serverError <- errors.New("Didn't write full bytes")
		} else {
			serverError <- err
		}
	}()

	buffer := make([]byte, len(data))

	timer := time.Now()
	count, err := io.ReadAtLeast(reader, buffer, len(data))
	elapsed := elapsedSeconds(timer)
	var countError error = nil
	if count != len(data) {
		countError = errors.New(fmt.Sprintf("Expected to read %v but got %v", count, len(data)))
	}

	serr, _ := <-serverError
	var returnErr error = nil
	if countError != nil {
		returnErr = countError
	} else if err != nil {
		returnErr = err
	} else if serr != nil {
		returnErr = serr
	}
	return elapsed, returnErr
}

func TestThrottlingGlobal(t *testing.T) {
	// One byte a second
	pool := NewIOThrottlerPool(1, 1)

	r, w := io.Pipe()
	tr := pool.NewThrottledReadCloser(r, 100000, 100000, "r")
	assertNotNil(tr, t)
	data := []byte("01234")
	elapsedSeconds, err := timeTransfer(data, tr, w)
	if elapsedSeconds != int64(len(data)-1) {
		t.Fatalf("Expecting read to take %v seconds but it took %v instead", int64(len(data)-1), elapsedSeconds)
	}
	assertNil(err, t)

}

func TestThrottlingLocal(t *testing.T) {
	pool := NewIOThrottlerPool(10000, 100000)
	r, w := io.Pipe()
	// One byte a second
	tr := pool.NewThrottledReadCloser(r, 1, 1, "r")
	assertNotNil(tr, t)
	data := []byte("01234")
	elapsedSeconds, err := timeTransfer(data, tr, w)
	if elapsedSeconds != int64(len(data)-1) {
		t.Fatalf("Expecting read to take %v seconds but it took %v instead", int64(len(data)-1), elapsedSeconds)
	}
	assertNil(err, t)
}

func TestFairness(t *testing.T) {

	pool := NewIOThrottlerPool(100000, 1000000)

	r, w := io.Pipe()
	gr, gw := io.Pipe()
	// One byte a second
	tr := pool.NewThrottledReadCloser(r, 1, 1, "r")
	assertNotNil(tr, t)
	tgr := pool.NewThrottledReadCloser(gr, 100000, 100000, "gr")
	assertNotNil(tgr, t)
	// Hammer one pipe
	go func() {
		data := []byte("safsa341qfea")
		for {
			_, err := gw.Write(data)
			if nil != err {
				return
			}
		}
	}()
	go func() {
		for {
			buffer := make([]byte, 10)
			_, err := tgr.Read(buffer)
			if nil != err {
				return
			}
		}
	}()
	data := []byte("01234")
	elapsedSeconds, err := timeTransfer(data, tr, w)
	// the other pipe should still be able to complete in time
	if elapsedSeconds != int64(len(data)-1) {
		t.Fatalf("Expecting read to take %v seconds but it took %v instead", int64(len(data)-1), elapsedSeconds)
	}
	assertNil(err, t)
}

func TestLoadTransferSlow(t *testing.T) {
	// One byte a second
	pool := NewIOThrottlerPool(100000, 100000)
	var wg sync.WaitGroup
	transfer := func(wg *sync.WaitGroup, i int) {
		defer wg.Done()
		r, w := io.Pipe()
		tr := pool.NewThrottledReadCloser(r, 1, 1, fmt.Sprintf("%d", i))
		assertNotNil(tr, t)
		data := []byte("01234")
		elapsedSeconds, err := timeTransfer(data, tr, w)
		if elapsedSeconds != int64(len(data)-1) {
			t.Fatalf("Expecting read to take %v seconds but it took %v instead", int64(len(data)-1), elapsedSeconds)
		}
		assertNil(err, t)
	}

	for i := 0; i < 30; i++ {
		wg.Add(1)
		go transfer(&wg, i)
	}
	wg.Wait()
}

func TestLoadTransferFast(t *testing.T) {
	nbWorker := 30
	pool := NewIOThrottlerPool(1024*1024, 1024*1024)
	var wg sync.WaitGroup
	data := make([]byte, 1024*1024)
	rand.Read(data)
	transfer := func(wg *sync.WaitGroup, i int) {
		defer wg.Done()
		r, w := io.Pipe()
		tr := pool.NewThrottledReadCloser(r, 1024*1024, 1024*1024, fmt.Sprintf("%d", i))
		assertNotNil(tr, t)
		elapsedSeconds, err := timeTransfer(data, tr, w)
		if elapsedSeconds > int64(nbWorker) {
			t.Fatalf("Expecting read to take %v seconds but it took %v instead", 30, elapsedSeconds)
		}
		assertNil(err, t)
	}

	for i := 0; i < nbWorker; i++ {
		wg.Add(1)
		go transfer(&wg, i)
	}
	wg.Wait()
}

func TestLoadTransferFastNoConstraintServer(t *testing.T) {
	nbWorker := 30
	pool := NewIOThrottlerPool(10240*1024, 1024*1024)
	var wg sync.WaitGroup
	data := make([]byte, 1024*1024)
	rand.Read(data)
	transfer := func(wg *sync.WaitGroup, i int) {
		defer wg.Done()
		r, w := io.Pipe()
		tr := pool.NewThrottledReadCloser(r, 1024*1024, 1024*1024, fmt.Sprintf("%d", i))
		assertNotNil(tr, t)
		elapsedSeconds, err := timeTransfer(data, tr, w)
		// we should finish in a tenth as the top level has 10x bandwith
		if elapsedSeconds > int64(nbWorker/10) {
			t.Fatalf("Expecting read to take %v seconds but it took %v instead", nbWorker/10, elapsedSeconds)
		}
		assertNil(err, t)
	}

	for i := 0; i < nbWorker; i++ {
		wg.Add(1)
		go transfer(&wg, i)
	}
	wg.Wait()
}

func TestLoadTransferFastConstraintReader(t *testing.T) {
	nbWorker := 30
	pool := NewIOThrottlerPool(10240*1024, 1024*1024)
	var wg sync.WaitGroup
	data := make([]byte, 1024*1024)
	rand.Read(data)
	transfer := func(wg *sync.WaitGroup, i int) {
		defer wg.Done()
		r, w := io.Pipe()
		tr := pool.NewThrottledReadCloser(r, 1024*1024/10, 1024*1024/10, fmt.Sprintf("%d", i))
		assertNotNil(tr, t)
		elapsedSeconds, err := timeTransfer(data, tr, w)
		// we have a lot of bandwidth per server but only a tenth per client, it should roughly take 10s to download
		if elapsedSeconds > int64(nbWorker/3) {
			t.Fatalf("Expecting read to take %v seconds but it took %v instead", nbWorker/3, elapsedSeconds)
		}
		assertNil(err, t)
	}

	for i := 0; i < nbWorker; i++ {
		wg.Add(1)
		go transfer(&wg, i)
	}
	wg.Wait()
}

func Benchmark(b *testing.B) {

	copyToReaders := func(bytesToCopy int, readerCount int) {
		b.StopTimer()
		files := make([]*os.File, readerCount)
		for i := 0; i != readerCount; i++ {
			const fileName = "/dev/zero"
			file, err := os.Open(fileName)
			assertNil(err, nil)
			defer file.Close()
			files[i] = file
		}
		pool := NewIOThrottlerPool(rate.Limit(bytesToCopy*readerCount), bytesToCopy*readerCount)
		var dst bytes.Buffer
		b.StartTimer()
		timer := time.Now()
		for _, file := range files {
			dst.Reset()

			tr := pool.NewThrottledReadCloser(file, rate.Limit(bytesToCopy*readerCount), bytesToCopy*readerCount, "r")
			written, err := io.CopyN(&dst, tr, int64(bytesToCopy))
			assertNil(err, nil)
			if written != int64(bytesToCopy) {
				b.Fatalf("Should have copied %v but only copied %v", bytesToCopy, written)
			}
		}
		// time should be zero as we always have enough token available
		const expected = 0
		if elapsedSeconds(timer) != expected {
			b.Fatalf("Should have taken %v seconds but it took %v instead", expected, elapsedSeconds(timer))
		}
	}

	// get enough info
	for i := 0; i != b.N; i++ {
		for j := 0; j != 2; j++ {
			toCopy := 1024 * 10
			readers := 10
			if j == 0 {
				// More reader than bytes
				readers *= 10
			} else {
				// More byte than readers
				toCopy *= 10
			}
			copyToReaders(toCopy, readers)
		}
	}

}
