package client

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"fknsrs.biz/p/counterd/types"
)

var (
	// ErrTimeout is returned if a read times out.
	ErrTimeout = errors.New("timeout")
	// ErrMissing is returned if a value is requested without allowing for a
	// timeout.
	ErrMissing = errors.New("missing")
)

// KV represents a key/value pair.
type KV struct {
	K uint64
	V float64
}

// Client holds all the state necessary for interacting with a counterd server.
type Client struct {
	m sync.RWMutex
	s net.Conn
	d map[uint64]float64
	o map[uint64][]chan float64
	c []chan KV
}

// Dial connects to a counterd service at a specified address.
func Dial(addr string) (*Client, error) {
	s, err := net.Dial("tcp4", addr)
	if err != nil {
		return nil, err
	}

	c := Client{
		s: s,
		d: make(map[uint64]float64),
		o: make(map[uint64][]chan float64),
	}

	go c.read()

	return &c, nil
}

func (c *Client) read() {
	for {
		m, err := types.ReadMessage(c.s)
		if err != nil {
			if err == io.EOF {
				break
			}

			panic(err)
		}

		switch m := m.(type) {
		case *types.NotifyMessage:
			c.m.Lock()

			c.d[m.Key] = m.Val

			l, ok := c.o[m.Key]
			if ok {
				delete(c.o, m.Key)
			}

			c.m.Unlock()

			for _, c := range l {
				c <- m.Val
			}

			for _, ch := range c.c {
				ch <- KV{K: m.Key, V: m.Val}
			}
		}
	}
}

// Monitor registers a channel to receive updates when a notification comes in
// from the counterd server.
func (c *Client) Monitor(ch chan KV) {
	c.c = append(c.c, ch)
}

// Close closes the client.
func (c *Client) Close() error {
	return c.s.Close()
}

// Increment adds a (potentially negative) value to a counter on the server,
// scheduled to be reverted after a certain duration.
func (c *Client) Increment(k uint64, v float64, t time.Duration) error {
	return types.WriteMessage(c.s, types.IncrementMessage{
		Key: k,
		Val: v,
		TTE: uint32(time.Now().Add(t).Unix()),
	})
}

// Subscribe makes a request to the server to begin sending update
// notifications for a particular key.
func (c *Client) Subscribe(k uint64) error {
	return types.WriteMessage(c.s, types.SubscribeMessage{
		Key: k,
	})
}

// Unsubscribe makes a request to the server to stop sending update
// notifications for a particular key.
func (c *Client) Unsubscribe(k uint64) error {
	return types.WriteMessage(c.s, types.UnsubscribeMessage{
		Key: k,
	})
}

// Read returns what the client has cached locally for a particular key,
// or ErrMissing if there's no cached value.
func (c *Client) Read(k uint64) (float64, error) {
	c.m.RLock()
	defer c.m.RUnlock()

	v, ok := c.d[k]
	if !ok {
		return 0, ErrMissing
	}

	return v, nil
}

// ReadOrQuery does what it sounds like it does. It either returns the locally
// cached value of a particular key, or it makes a request to the server.
func (c *Client) ReadOrQuery(k uint64, t time.Duration) (float64, error) {
	v, err := c.Read(k)
	if err == nil {
		return v, nil
	}

	l := make(chan float64, 1)

	c.m.Lock()
	c.o[k] = append(c.o[k], l)
	c.m.Unlock()

	if err := types.WriteMessage(c.s, types.QueryMessage{Key: k}); err != nil {
		return 0, err
	}

	select {
	case v := <-l:
		return v, nil
	case <-time.After(t):
		return 0, ErrTimeout
	}
}
