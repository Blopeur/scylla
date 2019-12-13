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

type ThrottledReadCloser struct {
	origReadCloser io.ReadCloser
	id             string
	pool           *ioThrottlerPool
}

type IOThrottlerPool interface {
	GetGlobalLimit() (r rate.Limit, b int)
	GetIDs() []string
	GetLimitByID(id string) (r rate.Limit, b int, err error)
	SetGlobalLimit(r rate.Limit, b int)
	SetLimitByID(r rate.Limit, b int, id string) error
	SetLimitForAll(r rate.Limit, b int)
	NewThrottledReadCloser(reader io.ReadCloser, r rate.Limit, b int, id string) io.ReadCloser
}

func NewIOThrottlerPool(r rate.Limit, b int) IOThrottlerPool {
	i := &ioThrottlerPool{
		globalLimiter: rate.NewLimiter(r, b),
		connections:   make(map[string]*ioThrottler),
		mu:            &sync.RWMutex{},
	}
	return i
}

func (p *ioThrottlerPool) GetGlobalLimit() (r rate.Limit, b int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.globalLimiter.Limit(), p.globalLimiter.Burst()
}
func (p *ioThrottlerPool) GetIDs() []string {
	var ids []string
	p.mu.Lock()
	defer p.mu.Unlock()
	for id := range p.connections {
		ids = append(ids, id)
	}
	return ids
}

func (p *ioThrottlerPool) GetLimitByID(id string) (r rate.Limit, b int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	l, ok := p.connections[id]
	if !ok {
		return 0, 0, fmt.Errorf("limiter for connection %s not found", id)
	}
	return l.limiter.Limit(), l.limiter.Burst(), nil
}

func (p *ioThrottlerPool) SetGlobalLimit(r rate.Limit, b int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.globalLimiter.SetBurst(b)
	p.globalLimiter.SetLimit(r)
}

func (p *ioThrottlerPool) SetLimitForAll(r rate.Limit, b int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, l := range p.connections {
		l.limiter.SetBurst(b)
		l.limiter.SetLimit(r)
	}
}

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

func (p *ioThrottlerPool) globalThrottle(n int) error {
	now := time.Now()
	// This is suboptimal as we do not guarantee a fair allocation across all reader/writer
	rvGlobal := p.globalLimiter.ReserveN(now, n)
	if !rvGlobal.OK() {
		return fmt.Errorf("exceeds limiter's burst")
	}
	delay := rvGlobal.DelayFrom(now)
	time.Sleep(delay)
	return nil
}

func (p *ioThrottlerPool) throttle(n int, l *ioThrottler) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	rvLocal := l.limiter.ReserveN(now, n)
	if !rvLocal.OK() {
		return fmt.Errorf("exceeds limiter's burst")
	}
	delay := rvLocal.DelayFrom(now)
	time.Sleep(delay)
	return nil
}

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
	err := r.pool.globalThrottle(b)
	if err != nil {
		r.pool.mu.Unlock()
		return 0, err
	}
	r.pool.mu.Unlock()

	err = r.pool.throttle(b, l)
	if err != nil {
		r.pool.mu.Unlock()
		return 0, err
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

func (r *ThrottledReadCloser) Close() error {
	r.pool.mu.Lock()
	defer r.pool.mu.Unlock()
	delete(r.pool.connections, r.id)
	return r.origReadCloser.Close()
}
