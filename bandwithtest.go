package main

import (
	"flag"
	"log"
	"net"
	"tcpThrottling/lib"
	netxtest "tcpThrottling/nextest"
)

func myLimitedListener(l net.Listener, limitGlobal, limitPerConn int) net.Listener {
	limited := lib.NewThrottledListener(l, limitGlobal, limitPerConn, limitGlobal, limitPerConn)
	return limited
}

func main() {
	var test netxtest.LimitListenerTest

	test.RegisterFlags(flag.CommandLine)
	flag.Parse()

	if err := test.Run(myLimitedListener); err != nil {
		log.Fatal(err)
	}
}
