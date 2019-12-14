
# IO Throttler 

## Problem 
Imagine a service that is serving big log files over raw TCP, you decided to implement simple QoS for an existing server.

The goal of the task is to create small Go package that would allow for throttling bandwidth for TCP connections.

## Requirements

- keep it simple
- use only stdlib and/or supplementary repositories (golang.org/x/\*)
- the package should allow for:
  - setting bandwidth limit per server
  - setting bandwidth limit per connection
  - changing limits in runtime (applies to all existing connections)
- for a 30s transfer sample consumed bandwidth should be accurate +/- 5%

## Design decision

### Base structure : Read / Writer 

I decided to create a wrapper around the read / writer structure as this is the simplest way to implement the throttling solution.
The advantage is that we can implement throttling on the server side by simply throttling the Reader.
Once the Reader buffer feels up and the Socket Buffer fill up, the server will stop sending ACK to the client and the natural TCP throttling will kick in.

### Throttling Mechanism :  x/rate/limiter

In order to keep the code short and efficient I decided to use  std [rate package](https://godoc.org/golang.org/x/time/rate).
The limiter in this package implement the token bucket algorithm which is fast and efficient in this case.


## Hierarchical Throttling

Since we have a two level throttling : server and connection. I needed to implement a two level throttling mechanism while maintaining fairness.
To do so I used a two level throttling system. The top level pool has a single bucket that distribute tokens to each connection on demands.
However in order to preserve fairness. I will impose a maximum read size calculated using the burstiness factor and the number of active connection.
This allow me to easily slice the IO usage fairly across connection while maintaining performance.
One level Down , each Reader / or writer has it's own bucket for low level throttling.

### Drawback

I had to use a global lock on the pool datastructure to protect throttling operations. This is not completely ideal but based on quick test, it was more efficient than using channels.

### Design choices 

Originally I added a way to create pool and buffer by defining how much bandwidth in byte/s was allowed.
I later on deleted this as it felt that it was imposing more constraint and the library was more flexible by directly exposing the limit and burtiness variable.
As a result the library can be easily reused in other cases.

Note that the library wrap the following datastructure for convenience and flexibility  : 
    - ReadCloser
    - ReadWriter
    - ReadWriteCloser
    - Net.Conn
    
    
## Performance

Due to time constraint Performance and Fairness are only validated within the Unit Test.
But Fairness is guaranteed on read and write side.
Also we show that for 50s+ execution the bandwidth is allocated based on the desired settings

## Example of use

```go
package example 
import (
	"bytes"
	"fmt"
	"io"
	throttle "tcpThrottling/lib"
	"os"
)
// Basic usage of a IOThrottlerPool to throttle reading from a file 
func ExampleIOThrottlerPool() error{
	// Construct a bandwidth throttling pool that's limited to 100k bytes per second
	poolRead := throttle.NewIOThrottlerPool(100*1024, 100*1024)
	file, err := os.Open("/dev/zero")
	if err != nil {		
		return err
	}
	defer file.Close()
    tr := poolRead.NewThrottledReadCloser(file, 100*1024, 100*1024, fmt.Sprintf("zero"))
	var zeros bytes.Buffer
	_, err = io.CopyN(&zeros, tr, 200*1024)
	if err != nil {
		return err
	}
    return nil
}

```

## TODO 

1. add wrapper for the reader that add metrics so we can get accurate statistics 
2. Full blown client/server + integration tests

