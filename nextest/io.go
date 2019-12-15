package netxtest

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"time"
)

type errors []error

func (e errors) Error() string {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "Failed with %d errors:\n\n", len(e))

	for _, e := range e {
		fmt.Fprintf(&buf, "\t* %s\n", e)
	}

	fmt.Fprintln(&buf)

	return buf.String()
}

func (e errors) Err() error {
	if len(e) == 0 {
		return nil
	}
	return e
}

type byteReader byte

var _ io.Reader = byteReader(0)

func (br byteReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(br)
	}
	return len(p), nil
}

var null byteReader

type expiringConn struct {
	ttl   time.Duration
	first time.Time
	last  time.Time
}

func (c *expiringConn) Write(p []byte) (int, error) {
	return len(p), c.step()
}

func (c *expiringConn) Read(p []byte) (int, error) {
	return len(p), c.step()
}

func (c *expiringConn) step() error {
	if c.first.IsZero() {
		c.first = time.Now()
	}
	c.last = time.Now()
	if c.duration() >= c.ttl {
		return fmt.Errorf("stop")
	}
	return nil
}

func (c *expiringConn) duration() time.Duration {
	return c.last.Sub(c.first)
}

type clients []int64

func (c clients) run(i int, ttl, rampUpDown time.Duration, mode byte, addr net.Addr) func() error {
	var fake io.ReadWriter = &expiringConn{ttl: ttl}
	var rampDown io.ReadWriter = &expiringConn{ttl: rampUpDown}
	var rampUp io.ReadWriter = &expiringConn{ttl: rampUpDown}

	return func() error {
		conn, err := net.DialTimeout(addr.Network(), addr.String(), 2*time.Second)
		if err != nil {
			return err
		}
		defer conn.Close()

		if _, err := conn.Write([]byte{mode}); err != nil {
			return err
		}

		switch mode {
		case 'r':
			io.Copy(rampUp, conn)
		case 'w':
			io.Copy(conn, rampUp)
		default:
			return fmt.Errorf("unexpected control byte: %c", rune(mode))
		}

		switch mode {
		case 'r':
			c[i], _ = io.Copy(fake, conn)
		case 'w':
			c[i], _ = io.Copy(conn, fake)
		default:
			return fmt.Errorf("unexpected control byte: %c", rune(mode))
		}

		switch mode {
		case 'r':
			io.Copy(rampDown, conn)
		case 'w':
			io.Copy(conn, rampDown)
		default:
			return fmt.Errorf("unexpected control byte: %c", rune(mode))
		}

		return nil
	}
}
