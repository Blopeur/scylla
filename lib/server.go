package lib

import (
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"log"
	"net"
	"os"
)

const (
	defaultRate  = 100 * 1024
	defaultBurst = 100 * 1024
)

type fileServer struct {
	address string
	logPath string
	pool    IOThrottlerPool
}

type FileServer interface {
	ListenAndServe()
	SetGlobalLimit(r rate.Limit, b int)
	SetConnectionLimit(r rate.Limit, b int, id string) error
	SetConnectionsLimit(r rate.Limit, b int)
}

func (f *fileServer) SetGlobalLimit(r rate.Limit, b int) {
	f.pool.SetGlobalLimit(r, b)
}

func (f *fileServer) SetConnectionLimit(r rate.Limit, b int, id string) error {
	return f.pool.SetLimitByID(r, b, id)
}

func (f *fileServer) SetConnectionsLimit(r rate.Limit, b int) {
	f.pool.SetLimitForAll(r, b)
}

func NewFileServer(address string, logPath string) FileServer {
	return &fileServer{
		address: address,
		logPath: logPath,
		pool:    NewIOThrottlerPool(defaultRate, defaultBurst),
	}
}

func (f *fileServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	buff := f.pool.NewThrottledReadCloser(conn, defaultRate, defaultBurst, conn.RemoteAddr().String())

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
