package lib

import (
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"sync"
	"time"
)

type ioThrottler struct {
	limiter *rate.Limiter
}

type ioThrottlerPool struct {
	globalLimiter *rate.Limiter
	mu            *sync.RWMutex
	connections   map[string]*ioThrottler
}

// ThrottledReadCloser is a throttled IO reader
type ThrottledReadCloser struct {
	origReadCloser io.ReadCloser
	id             string
	pool           *ioThrottlerPool
}

// IOThrottlerPool provide a hierarchical throttling for a collection of readers
// Note: we use rate.limiter from golang.org/x/time/rate to provide the throttling capabilities
type IOThrottlerPool interface {
	GetGlobalLimit() (r rate.Limit, b int)
	GetIDs() []string
	GetLimitByID(id string) (r rate.Limit, b int, err error)
	SetGlobalLimit(r rate.Limit, b int)
	SetLimitByID(r rate.Limit, b int, id string) error
	SetLimitForAll(r rate.Limit, b int)
	NewThrottledReadCloser(reader io.ReadCloser, r rate.Limit, b int, id string) io.ReadCloser
	NewBandwidthThrottledReadCloser(reader io.ReadCloser, bandwidth int64, b int, id string) (io.ReadCloser, error)
}

// NewBandwidthThrottlerPool create a new pool using the bandwidth rather than rate.limit
func NewBandwidthThrottlerPool(bandwidth int64, burstSize int) (IOThrottlerPool, error) {
	if bandwidth < 1024 {
		return nil, fmt.Errorf("bandwith needs to be at least 1KB")
	}
	if burstSize < 1024 {
		return nil, fmt.Errorf("buffer needs to be at least 1KB")
	}
	return NewIOThrottlerPool(rate.Every(convertBandwidthToLimit(bandwidth)), burstSize), nil
}

// NewIOThrottlerPool create a new Reader Pool
func NewIOThrottlerPool(r rate.Limit, b int) IOThrottlerPool {
	i := &ioThrottlerPool{
		globalLimiter: rate.NewLimiter(r, b),
		connections:   make(map[string]*ioThrottler),
		mu:            &sync.RWMutex{},
	}
	return i
}

func convertBandwidthToLimit(bandwidth int64) time.Duration {
	b := bandwidth / 1024
	if b == 0 {
		b = 1
	}
	//we use 1KB block chunck instead of 1 B for calculating events as we use 1KB buffer
	return time.Duration(1000000000 / b)
}

// GetGlobalLimit get the global limit for the pool
func (p *ioThrottlerPool) GetGlobalLimit() (r rate.Limit, b int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.globalLimiter.Limit(), p.globalLimiter.Burst()
}

// GetIDs get the id associated with each readers in the pool
func (p *ioThrottlerPool) GetIDs() []string {
	var ids []string
	p.mu.Lock()
	defer p.mu.Unlock()
	for id := range p.connections {
		ids = append(ids, id)
	}
	return ids
}

// GetLimitByID return the limit associated with a specific reader
func (p *ioThrottlerPool) GetLimitByID(id string) (r rate.Limit, b int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	l, ok := p.connections[id]
	if !ok {
		return 0, 0, fmt.Errorf("limiter for connection %s not found", id)
	}
	return l.limiter.Limit(), l.limiter.Burst(), nil
}

// SetGlobalLimit set the limit associated for the whole pool
func (p *ioThrottlerPool) SetGlobalLimit(r rate.Limit, b int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.globalLimiter.SetBurst(b)
	p.globalLimiter.SetLimit(r)
}

// SetLimitForAll set same limit for each reader in the pool
func (p *ioThrottlerPool) SetLimitForAll(r rate.Limit, b int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, l := range p.connections {
		l.limiter.SetBurst(b)
		l.limiter.SetLimit(r)
	}
}

// SetLimitForAll set a limit for a specific reader in the pool
func (p *ioThrottlerPool) SetLimitByID(r rate.Limit, b int, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	l, ok := p.connections[id]
	if !ok {
		return fmt.Errorf("limiter for connection %s not found", id)
	}
	l.limiter.SetBurst(b)
	l.limiter.SetLimit(r)
	return nil
}

// NewBandwidthThrottledReadCloser Return A reader where the limits where defined using bandwidth instead of rate.limit
func (p *ioThrottlerPool) NewBandwidthThrottledReadCloser(reader io.ReadCloser, bandwidth int64, b int, id string) (io.ReadCloser, error) {
	if bandwidth < 1024 {
		return nil, fmt.Errorf("bandwith needs to be at least 1KB")
	}
	return p.NewThrottledReadCloser(reader, rate.Every(convertBandwidthToLimit(bandwidth)), b, id), nil
}

// NewThrottledReadCloser return a new Throttled Reader
func (p *ioThrottlerPool) NewThrottledReadCloser(reader io.ReadCloser, r rate.Limit, b int, id string) io.ReadCloser {
	p.mu.Lock()
	defer p.mu.Unlock()
	throttler := ioThrottler{
		limiter: rate.NewLimiter(r, b),
	}
	p.connections[id] = &throttler
	return &ThrottledReadCloser{
		origReadCloser: reader,
		id:             id,
		pool:           p,
	}

}

func (p *ioThrottlerPool) globalThrottle(n int) (time.Duration, error) {
	now := time.Now()
	// This is suboptimal as we do not guarantee a fair allocation across all reader/writer
	rvGlobal := p.globalLimiter.ReserveN(now, n)
	if !rvGlobal.OK() {
		return 0, fmt.Errorf("exceeds limiter's burst")
	}
	delay := rvGlobal.DelayFrom(now)
	return delay, nil
}

func (p *ioThrottlerPool) throttle(n int, l *ioThrottler) (time.Duration, error) {

	now := time.Now()
	rvLocal := l.limiter.ReserveN(now, n)
	if !rvLocal.OK() {
		return 0, fmt.Errorf("exceeds limiter's burst")
	}
	delay := rvLocal.DelayFrom(now)
	return delay, nil
}

// Read , implement the Read function from the Reader interface
func (r *ThrottledReadCloser) Read(buf []byte) (int, error) {
	r.pool.mu.Lock()
	globalBurst := r.pool.globalLimiter.Burst()
	l, ok := r.pool.connections[r.id]
	if !ok {
		r.pool.mu.Unlock()
		return 0, fmt.Errorf("limiter for connection %s not found", r.id)
	}
	readerBurst := l.limiter.Burst()
	// we try to be fair as a result we split the number of token evenly by Reader
	b := globalBurst / len(r.pool.connections)
	if b == 0 {
		b = 1
	}
	if b > readerBurst {
		b = readerBurst
	}

	if len(buf) <= b {
		b = len(buf)
	}
	globalDelay, err := r.pool.globalThrottle(b)
	if err != nil {
		r.pool.mu.Unlock()
		return 0, err
	}
	r.pool.mu.Unlock()
	r.pool.mu.RLock()
	bufferDelay, err := r.pool.throttle(b, l)
	if err != nil {
		r.pool.mu.RUnlock()
		return 0, err
	}
	r.pool.mu.RUnlock()
	if globalDelay > bufferDelay {
		time.Sleep(globalDelay)
	} else {
		time.Sleep(bufferDelay)
	}
	// if the amount of bytes allocated for read is smaller than the input buffer, we use a temp buffer for copy
	if b < len(buf) {
		tmp := make([]byte, b)
		n, err := r.origReadCloser.Read(tmp)
		if n <= 0 {
			return n, err
		}
		copy(tmp[:n], buf)
		return n, err
	}
	n, err := r.origReadCloser.Read(buf)
	return n, err
}

// Close implement the close function from the ReadCloser interface
func (r *ThrottledReadCloser) Close() error {
	r.pool.mu.Lock()
	defer r.pool.mu.Unlock()
	delete(r.pool.connections, r.id)
	return r.origReadCloser.Close()
}
