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
	burst        int
}

// GetBurstSize return the max burst amount of data transfer can occur (how many bytes can be read at once)
// Careful buffer are pre allocated based on the burst size, stay conservative
func (l *throttledListener) GetBurstSize() int {
	return l.burst
}

// SetBurstSize change the default burst size
func (l *throttledListener) SetBurstSize(burst int) int {
	l.burst = burst
	// We need to update all settings for global / conn
	lim := rate.Limit(l.connLimit)
	l.poolRead.SetLimitForAll(lim, l.burst)
	l.poolWrite.SetLimitForAll(lim, l.burst)
	lim = rate.Limit(l.globalLimit)
	l.poolWrite.SetGlobalLimit(lim, l.burst)
	l.poolRead.SetGlobalLimit(lim, l.burst)
	return l.burst
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
	l.poolRead.SetLimitForAll(lim, l.burst)
	l.poolWrite.SetLimitForAll(lim, l.burst)
	return limit
}

// SetGlobalLimit set the bandwidth limit for the whole server
// limit is in byte/s
func (l *throttledListener) SetGlobalLimit(limit int) int {
	l.globalLimit = limit
	lim := rate.Limit(l.globalLimit)
	l.poolWrite.SetGlobalLimit(lim, l.burst)
	l.poolRead.SetGlobalLimit(lim, l.burst)
	return l.globalLimit
}

// ThrottledListener implement the listener interface
type ThrottledListener interface {
	net.Listener
	GetGlobalLimit() int
	GetConnLimit() int
	GetBurstSize() int
	SetConnLimit(int) int
	SetGlobalLimit(int) int
	SetBurstSize(int) int
}

// Implement the Accept of net.Listener
// This will return a throttled connection
func (l *throttledListener) Accept() (net.Conn, error) {
	conn, err := l.origListener.Accept()
	if err != nil {
		return nil, err
	}
	throttledConn := NewThrottledThrottledConn(l.poolRead, l.poolWrite, conn,
		rate.Limit(l.connLimit), l.burst, rate.Limit(l.connLimit), l.burst)
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
// Note: the burst size will limit how much you will be able to read from the con in a slot, this is to allow fair(ish) throttling between connection
// Please size carefully your burst buffer.
// Note2: the limit are in int, which limit you to max ~2 Gbyte / s
// globalLimit and connLimit are both in Byte/s
// NewThrottledListener will return nil if any of the limit / burst value are negative
func NewThrottledListener(listener net.Listener, globalLimit, connLimit, burst int) ThrottledListener {
	poolRead := NewIOThrottlerPool(rate.Limit(globalLimit), burst)
	poolWrite := NewIOThrottlerPool(rate.Limit(globalLimit), burst)
	return &throttledListener{listener, poolRead, poolWrite,
		globalLimit, connLimit, burst}
}
