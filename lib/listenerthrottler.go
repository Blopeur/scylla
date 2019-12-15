package lib

import (
	"golang.org/x/time/rate"
	"net"
)

type throttledListener struct {
	origListener net.Listener
	poolRead     IOThrottlerPool
	poolWrite    IOThrottlerPool
	globalLimit  int
	connLimit    int
	globalBurst  int
	connBurst    int
}

// GetGlobalBurstSize return the max globalBurst amount of data transfer can occur (how many bytes can be read at once)
// Careful buffer are pre allocated based on the globalBurst size, stay conservative
func (l *throttledListener) GetGlobalBurstSize() int {
	return l.globalBurst
}

// SetGlobalBurstSize change the default globalBurst size
func (l *throttledListener) SetGlobalBurstSize(burst int) int {
	l.globalBurst = burst
	lim := rate.Limit(l.globalLimit)
	l.poolWrite.SetGlobalLimit(lim, l.globalBurst)
	l.poolRead.SetGlobalLimit(lim, l.globalBurst)
	return l.globalBurst
}

// GetConnBurstSize return the max connBurst amount of data transfer can occur (how many bytes can be read at once) per conn
// Careful buffer are pre allocated based on the connBurst size, stay conservative
func (l *throttledListener) GetConnBurstSize() int {
	return l.connBurst
}

// SetConnBurstSize change the default connLimit size
func (l *throttledListener) SetConnBurstSize(burst int) int {
	l.connBurst = burst
	lim := rate.Limit(l.connLimit)
	l.poolWrite.SetLimitForAll(lim, l.connBurst)
	l.poolRead.SetLimitForAll(lim, l.connBurst)
	return l.connBurst
}

// GetGlobalLimit return the current value of the limit for the server
func (l *throttledListener) GetGlobalLimit() int {
	return l.globalLimit
}

// GetConnLimit return the current value of the limit per connection
func (l *throttledListener) GetConnLimit() int {
	return l.connLimit
}

// SetConnLimit set the bandwidth limit for the each connection server
// limit is in byte/s
func (l *throttledListener) SetConnLimit(limit int) int {
	l.connLimit = limit
	lim := rate.Limit(l.connLimit)
	l.poolRead.SetLimitForAll(lim, l.globalBurst)
	l.poolWrite.SetLimitForAll(lim, l.globalBurst)
	return limit
}

// SetGlobalLimit set the bandwidth limit for the whole server
// limit is in byte/s
func (l *throttledListener) SetGlobalLimit(limit int) int {
	l.globalLimit = limit
	lim := rate.Limit(l.globalLimit)
	l.poolWrite.SetGlobalLimit(lim, l.globalBurst)
	l.poolRead.SetGlobalLimit(lim, l.globalBurst)
	return l.globalLimit
}

// ThrottledListener implement the listener interface
type ThrottledListener interface {
	net.Listener
	GetGlobalLimit() int
	GetConnLimit() int
	GetGlobalBurstSize() int
	GetConnBurstSize() int
	SetConnLimit(int) int
	SetGlobalLimit(int) int
	SetGlobalBurstSize(int) int
	SetConnBurstSize(int) int
}

// Implement the Accept of net.Listener
// This will return a throttled connection
func (l *throttledListener) Accept() (net.Conn, error) {
	conn, err := l.origListener.Accept()
	if err != nil {
		return nil, err
	}
	throttledConn := NewThrottledThrottledConn(l.poolRead, l.poolWrite, conn,
		rate.Limit(l.connLimit), l.globalBurst, rate.Limit(l.connLimit), l.globalBurst)
	return throttledConn, err
}

// Implement the Close of net.Listener
func (l *throttledListener) Close() error {
	return l.origListener.Close()
}

// Implement the Addr of net.Listener
func (l *throttledListener) Addr() net.Addr {
	return l.origListener.Addr()
}

// NewThrottledListener return a ThrottledListener Interface that implement the net.Listener interface
// Note: the globalBurst size will limit how much you will be able to read from the con in a slot, this is to allow fair(ish) throttling between connection
// Please size carefully your globalBurst buffer.
// Note2: the limit are in int, which limit you to max ~2 Gbyte / s
// globalLimit and connLimit are both in Byte/s
// NewThrottledListener will return nil if any of the limit / globalBurst value are negative
func NewThrottledListener(listener net.Listener, globalLimit, connLimit, globalBurst, connBurst int) ThrottledListener {
	poolRead := NewIOThrottlerPool(rate.Limit(globalLimit), globalBurst)
	poolWrite := NewIOThrottlerPool(rate.Limit(globalLimit), globalBurst)
	return &throttledListener{listener, poolRead, poolWrite,
		globalLimit, connLimit, globalBurst, connBurst}
}
