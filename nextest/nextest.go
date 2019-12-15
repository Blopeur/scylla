package netxtest

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	kb = 1024
	mb = 1024 * kb
)

type LimitListenerTest struct {
	Limit     int
	LimitConn int
	Count     int
	Duration  time.Duration
	RampUp    time.Duration
	RampDown  time.Duration
	Epsilon   float64
	Mode      string
}

func (t *LimitListenerTest) RegisterFlags(f *flag.FlagSet) {
	flag.IntVar(&t.Limit, "limit", 100*256*kb, "Limit bandwidth (bytes per second)")
	flag.IntVar(&t.LimitConn, "limit-conn", 256*kb, "Limit per connection bandwidth (bytes per second)")
	flag.IntVar(&t.Count, "count", 100, "Number of clients")
	flag.DurationVar(&t.Duration, "time", 30*time.Second, "Test duration")
	flag.DurationVar(&t.RampUp, "rampup", 50*time.Millisecond, "Amount of time for ramping up the load")
	flag.DurationVar(&t.RampDown, "rampdown", 50*time.Millisecond, "Amount of time spent for ramping down the load")
	flag.Float64Var(&t.Epsilon, "epsilon", 0.04, "Tolerance")
	flag.StringVar(&t.Mode, "mode", "read", "Test mode (either read or write)")
}

type LimitListenFunc func(l net.Listener, limitGlobal, limitPerConn int) net.Listener

func (t *LimitListenerTest) Run(limit LimitListenFunc) error {
	var mode byte

	switch strings.ToLower(t.Mode) {
	case "read":
		mode = 'r'
	case "write":
		mode = 'w'
	default:
		return fmt.Errorf("unrecognized mode: %q", t.Mode)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer l.Close()

	liml := limit(l, t.Limit, t.LimitConn)
	defer liml.Close()

	go serve(liml)

	want := int(t.Duration.Seconds()+0.5) * minNonZero(t.LimitConn, t.Limit/t.Count)
	eps := int(float64(want)*t.Epsilon + 0.5)

	rang := [2]int{want - eps, want + eps}

	log.Printf("clients: %d", t.Count)
	log.Printf("global limit: %d [kB/s], per connection: %d [kB/s]", t.Limit/kb, t.LimitConn/kb)
	log.Printf("transfer duration: %s", t.Duration)
	log.Printf("Ramp ops duration - up %s - down %s ", t.RampUp, t.RampDown)
	log.Printf("expected bandwidth within range (%d, %d) [B] (epsilon=%.2f)", rang[0], rang[1], t.Epsilon)
	log.Print("running test ...")

	if err := t.run(mode, liml.Addr(), rang); err != nil {
		return err
	}

	log.Print("OK")

	return nil
}

func (t *LimitListenerTest) run(mode byte, addr net.Addr, rang [2]int) error {
	var wg errgroup.Group
	var c = make(clients, t.Count)

	for i := range c {
		wg.Go(c.run(i, t.Duration, t.RampUp, t.RampDown, mode, addr))
	}

	if err := wg.Wait(); err != nil {
		return err
	}

	var err errors

	for i := range c {
		got := int(c[i])

		log.Printf("client[%d]: transferred bytes: %d [B]", i, got)

		if got < rang[0] || got > rang[1] {
			err = append(err, fmt.Errorf("client[%d] mode=%c: got %d not within range (%d, %d)",
				i, rune(mode), got, rang[0], rang[1]))
		}
	}

	return err.Err()
}

func serve(l net.Listener) {
	for {
		c, err := l.Accept()
		if err != nil {
			return
		}

		var p [1]byte

		if _, err := c.Read(p[:]); err != nil {
			panic("unable to read control byte: " + err.Error())
		}

		switch p[0] {
		case 'r':
			go io.Copy(c, null)
		case 'w':
			go io.Copy(ioutil.Discard, c)
		default:
			panic("unexpected control byte: " + string(p[:]))
		}
	}
}

func minNonZero(i, j int) int {
	if (i < j && i != 0) || j == 0 {
		return i
	}
	return j
}
