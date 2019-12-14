package server

import (
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"log"
	"net"
	"os"
	throttle "tcpThrottling/lib"
	"time"
)

const (
	defaultRate  = 100 * 1024
	defaultBurst = 100 * 1024
)

type fileServer struct {
	address   string
	logPath   string
	poolRead  throttle.IOThrottlerPool
	poolWrite throttle.IOThrottlerPool
}

// FileServer provide the basic fileserver interface
type FileServer interface {
	ListenAndServe()
	SetGlobalReadLimit(r rate.Limit, b int)
	SetGlobalWriteLimit(r rate.Limit, b int)
	SetConnectionReadLimit(r rate.Limit, b int, id string) error
	SetConnectionWriteLimit(r rate.Limit, b int, id string) error
	SetConnectionsReadLimit(r rate.Limit, b int)
	SetConnectionsWriteLimit(r rate.Limit, b int)
}

//	SetGlobalLimit set the rate limiting and burst for the whole server
func (f *fileServer) SetGlobalReadLimit(r rate.Limit, b int) {
	f.poolRead.SetGlobalLimit(r, b)
}

//	SetConnectionLimit set the rate limiting and burst for a specific connection
func (f *fileServer) SetConnectionReadLimit(r rate.Limit, b int, id string) error {
	return f.poolRead.SetLimitByID(r, b, id)
}

//	SetConnectionLimit set the rate limiting and burst for all connections
func (f *fileServer) SetConnectionsReadLimit(r rate.Limit, b int) {
	f.poolRead.SetLimitForAll(r, b)
}

//	SetGlobalLimit set the rate limiting and burst for the whole server
func (f *fileServer) SetGlobalWriteLimit(r rate.Limit, b int) {
	f.poolWrite.SetGlobalLimit(r, b)
}

//	SetConnectionLimit set the rate limiting and burst for a specific connection
func (f *fileServer) SetConnectionWriteLimit(r rate.Limit, b int, id string) error {
	return f.poolWrite.SetLimitByID(r, b, id)
}

//	SetConnectionLimit set the rate limiting and burst for all connections
func (f *fileServer) SetConnectionsWriteLimit(r rate.Limit, b int) {
	f.poolWrite.SetLimitForAll(r, b)
}

// NewFileServer return a new file server
func NewFileServer(address string, logPath string, bandwidthRead, bandwidthWrite int64) FileServer {
	poolRead := throttle.NewIOThrottlerPool(rate.Every(convertBandwidthToLimit(bandwidthRead)), defaultBurst)
	poolWrite := throttle.NewIOThrottlerPool(rate.Every(convertBandwidthToLimit(bandwidthWrite)), defaultBurst)
	return &fileServer{
		address:   address,
		logPath:   logPath,
		poolRead:  poolRead,
		poolWrite: poolWrite,
	}
}

// convertBandwidthToLimit convert bandiwth in byte/s using 1k chunks block into a time.duration for the limiter
func convertBandwidthToLimit(bandwidth int64) time.Duration {
	b := bandwidth / 1024
	if b == 0 {
		b = 1
	}
	//we use 1KB block chunck instead of 1 B for calculating events as we use 1KB buffer
	return time.Duration(1000000000 / b)
}

// handleConnection will simply take the connection and pipe the data to a file using the client address as file name
func (f *fileServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	buff := throttle.NewThrottledThrottledConn(f.poolRead, f.poolWrite, conn, defaultRate, defaultBurst, defaultRate, defaultBurst)
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
