package server

import (
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"log"
	"net"
	"os"
	throttle "tcpThrottling/lib"
)

const (
	defaultRate  = 100 * 1024
	defaultBurst = 100 * 1024
)

type fileServer struct {
	address string
	logPath string
	pool    throttle.IOThrottlerPool
}

// FileServer provide the basic fileserver interface
type FileServer interface {
	ListenAndServe()
	SetGlobalLimit(r rate.Limit, b int)
	SetConnectionLimit(r rate.Limit, b int, id string) error
	SetConnectionsLimit(r rate.Limit, b int)
}

//	SetGlobalLimit set the rate limiting and burst for the whole server
func (f *fileServer) SetGlobalLimit(r rate.Limit, b int) {
	f.pool.SetGlobalLimit(r, b)
}

//	SetConnectionLimit set the rate limiting and burst for a specific connection
func (f *fileServer) SetConnectionLimit(r rate.Limit, b int, id string) error {
	return f.pool.SetLimitByID(r, b, id)
}

//	SetConnectionLimit set the rate limiting and burst for all connections
func (f *fileServer) SetConnectionsLimit(r rate.Limit, b int) {
	f.pool.SetLimitForAll(r, b)
}

// NewFileServer return a new file server
func NewFileServer(address string, logPath string, bandwidth int64) FileServer {
	pool, err := throttle.NewBandwidthThrottlerPool(bandwidth, defaultBurst)
	if err != nil {
		log.Fatal("can't create dest file", err)
		return nil
	}
	return &fileServer{
		address: address,
		logPath: logPath,
		pool:    pool,
	}
}

// handleConnection will simply take the connection and pipe the data to a file using the client address as file name
func (f *fileServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	buff, err := f.pool.NewBandwidthThrottledReadCloser(conn, defaultRate, defaultBurst, conn.RemoteAddr().String())
	if err != nil {
		log.Fatal("can't create readBuffer", err)
		return
	}
	// we use naively the client address as log address in this case, solely for the purpose of the exercise
	clientAddr := conn.RemoteAddr().String()
	fo, err := os.Create(fmt.Sprintf("%s/%s.log", f.logPath, clientAddr))
	if err != nil {
		log.Fatal("can't create dest file", err)
		return
	}
	defer fo.Close()
	for err == nil {
		_, err = io.Copy(fo, buff)
	}

}

// ListenAndServer wait for connection on the file tcp server
func (f *fileServer) ListenAndServe() {
	listener, err := net.Listen("tcp", f.address)
	if err != nil {
		log.Fatal("tcp server listener error:", err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("tcp server accept error", err)
		}
		go f.handleConnection(conn)
	}

}
