package websocket

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// startNetServer serves ws over net/http and returns its ws:// URL. Each
// connection reports on stopped when its read and write loops have stopped.
func startNetServer(t *testing.T, ws *Server, stopped chan<- *Conn) string {
	t.Helper()

	ws.HandleClose(func(c *Conn, err error) {
		go func() {
			c.wg.Wait()
			stopped <- c
		}()
	})

	srv := httptest.NewServer(http.HandlerFunc(ws.NetUpgrade))
	t.Cleanup(srv.Close)

	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// waitStopped waits for n connections to stop.
func waitStopped(t *testing.T, stopped <-chan *Conn, n int) {
	t.Helper()

	timeout := time.After(5 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-stopped:
		case <-timeout:
			t.Fatalf("%d of %d connections did not stop", n-i, n)
		}
	}
}

func TestConcurrentCloseDoesNotPanic(t *testing.T) {
	const conns = 200

	stopped := make(chan *Conn, conns)
	ws := &Server{}
	ws.HandleOpen(func(c *Conn) {
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c.Close()
			}()
		}
		wg.Wait()
	})
	url := startNetServer(t, ws, stopped)

	for i := 0; i < conns; i++ {
		cl, err := Dial(url)
		if err != nil {
			t.Fatal(err)
		}
		go cl.Close()
	}

	waitStopped(t, stopped, conns)
}

func TestConnStopsWhenClientDrops(t *testing.T) {
	const conns = 20

	stopped := make(chan *Conn, conns)
	url := startNetServer(t, &Server{}, stopped)

	for i := 0; i < conns; i++ {
		cl, err := Dial(url)
		if err != nil {
			t.Fatal(err)
		}
		cl.c.Close() // no close frame
	}

	waitStopped(t, stopped, conns)
}

func TestConnStopsAfterFlushingQueuedFrames(t *testing.T) {
	const conns = 50

	stopped := make(chan *Conn, conns)
	ws := &Server{}
	ws.HandleOpen(func(c *Conn) {
		for i := 0; i < 20; i++ {
			c.Write([]byte("message"))
		}
		c.Close()
	})
	url := startNetServer(t, ws, stopped)
	closeHandler := ws.closeHandler
	ws.HandleClose(func(c *Conn, err error) {
		time.Sleep(100 * time.Millisecond) // let writeLoop flush before the network connection closes
		closeHandler(c, err)
	})

	for i := 0; i < conns; i++ {
		cl, err := Dial(url)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			fr := AcquireFrame()
			defer ReleaseFrame(fr)
			for {
				if _, err := cl.ReadFrame(fr); err != nil || fr.IsClose() {
					return
				}
			}
		}()
	}

	waitStopped(t, stopped, conns)
}

func TestWriteAfterCloseReturnsError(t *testing.T) {
	stopped := make(chan *Conn, 1)
	url := startNetServer(t, &Server{}, stopped)

	cl, err := Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	cl.Close()

	var c *Conn
	select {
	case c = <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("connection did not stop")
	}

	done := make(chan error, 1)
	go func() {
		var err error
		for i := 0; i < 1000; i++ { // more than the output queue holds
			_, err = c.Write([]byte("late"))
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err != ErrConnClosed {
			t.Fatalf("Write after close: got %v, want ErrConnClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Write after close blocked")
	}
}

func TestNetUpgradeKeepsRequestKey(t *testing.T) {
	const key = "dGhlIHNhbXBsZSBub25jZQ=="

	ws := &Server{}
	got := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws.NetUpgrade(w, r)
		got <- r.Header.Get("Sec-WebSocket-Key")
	}))
	defer srv.Close()

	c, err := net.Dial("tcp", strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n" +
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " + key + "\r\n\r\n"))
	if err != nil {
		t.Fatal(err)
	}

	resp := make([]byte, 512)
	n, _ := c.Read(resp)
	if !strings.Contains(string(resp[:n]), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=") {
		t.Fatalf("unexpected handshake response:\n%s", resp[:n])
	}

	if k := <-got; k != key {
		t.Fatalf("request key changed: got %q, want %q", k, key)
	}
}

func TestServerRepliesToClientClose(t *testing.T) {
	const conns = 50

	stopped := make(chan *Conn, conns)
	url := startNetServer(t, &Server{}, stopped)

	for i := 0; i < conns; i++ {
		cl, err := Dial(url)
		if err != nil {
			t.Fatal(err)
		}

		fr := AcquireFrame()
		fr.SetClose()
		fr.SetFin()
		fr.SetStatus(StatusGoAway)
		fr.Mask()
		if _, err = cl.WriteFrame(fr); err != nil {
			t.Fatal(err)
		}

		fr.Reset()
		cl.c.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err = cl.ReadFrame(fr); err != nil {
			t.Fatalf("connection %d: no close reply: %v", i, err)
		}
		if !fr.IsClose() || fr.Status() != StatusGoAway {
			t.Fatalf("connection %d: got %s frame with status %s, want close with GoAway", i, fr.Code(), fr.Status())
		}
		ReleaseFrame(fr)
		cl.c.Close()
	}

	waitStopped(t, stopped, conns)
}

func TestCloseFlushesQueuedMessages(t *testing.T) {
	const msgs = 100

	stopped := make(chan *Conn, 1)
	ws := &Server{}
	ws.HandleOpen(func(c *Conn) {
		for i := 0; i < msgs; i++ {
			c.Write([]byte("message"))
		}
		c.Close()
	})
	url := startNetServer(t, ws, stopped)

	cl, err := Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.c.Close()

	fr := AcquireFrame()
	defer ReleaseFrame(fr)

	got := 0
	cl.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		if _, err := cl.ReadFrame(fr); err != nil {
			t.Fatalf("after %d messages: %v", got, err)
		}
		if fr.IsClose() {
			break
		}
		got++
	}

	if got != msgs {
		t.Fatalf("got %d messages before close, want %d", got, msgs)
	}
}

func TestCloseHandlerGetsClientStatus(t *testing.T) {
	const conns = 50

	errs := make(chan error, conns)
	ws := &Server{}
	ws.HandleClose(func(c *Conn, err error) { errs <- err })
	srv := httptest.NewServer(http.HandlerFunc(ws.NetUpgrade))
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	for i := 0; i < conns; i++ {
		cl, err := Dial(url)
		if err != nil {
			t.Fatal(err)
		}

		fr := AcquireFrame()
		fr.SetClose()
		fr.SetFin()
		fr.SetStatus(StatusGoAway)
		fr.Mask()
		cl.WriteFrame(fr)
		ReleaseFrame(fr)

		select {
		case err := <-errs:
			if e, ok := err.(Error); !ok || e.Status != StatusGoAway {
				t.Fatalf("connection %d: close handler got %v, want GoAway status", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("close handler was not called")
		}
		cl.c.Close()
	}
}

func TestFragmentedMessageOverLimitIsRejected(t *testing.T) {
	stopped := make(chan *Conn, 1)
	got := make(chan int, 1)
	ws := &Server{}
	ws.HandleData(func(c *Conn, isBinary bool, data []byte) { got <- len(data) })
	url := startNetServer(t, ws, stopped)

	cl, err := Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.c.Close()

	// small fragmented message: delivered
	writeFragments(cl, 3, 10)
	if n := <-got; n != 30 {
		t.Fatalf("got message of %d bytes, want 30", n)
	}

	// 3 fragments of 512KB: over DefaultPayloadSize, each frame under it
	go writeFragments(cl, 3, 512<<10)

	fr := AcquireFrame()
	defer ReleaseFrame(fr)

	cl.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := cl.ReadFrame(fr); err != nil {
		t.Fatalf("no close frame: %v", err)
	}
	if !fr.IsClose() || fr.Status() != StatusTooBig {
		t.Fatalf("got %s frame with status %s, want close with TooBig", fr.Code(), fr.Status())
	}

	select {
	case n := <-got:
		t.Fatalf("message of %d bytes delivered, want rejected", n)
	default:
	}

	waitStopped(t, stopped, 1)
}

// writeFragments sends a text message as n masked frames of size bytes each.
func writeFragments(cl *Client, n, size int) {
	payload := make([]byte, size)
	for i := 0; i < n; i++ {
		fr := AcquireFrame()
		if i == 0 {
			fr.SetText()
		} else {
			fr.SetContinuation()
		}
		if i == n-1 {
			fr.SetFin()
		}
		fr.SetPayload(payload)
		fr.Mask()
		_, err := cl.WriteFrame(fr)
		ReleaseFrame(fr)
		if err != nil {
			return // the server may close the connection first
		}
	}
}
