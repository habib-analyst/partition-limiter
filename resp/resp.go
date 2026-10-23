// Package resp is a minimal, stdlib-only Redis client speaking RESP2.
//
// It exists so partition-limiter has zero module dependencies: the build
// works fully offline. It supports exactly what the limiter and the chaos
// harness need — one command at a time over a single connection, with a
// hard per-command deadline. On any I/O error the connection is dropped
// and the next command redials, which is precisely the behaviour the
// partition-mode state machine wants to observe.
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Error is a Redis error reply (a RESP "-" line).
type Error string

func (e Error) Error() string { return string(e) }

// Client is a single-connection RESP client. Safe for concurrent use:
// commands are serialised, which is fine for a rate-limiter decision path
// at this scale and keeps reply framing trivially correct.
type Client struct {
	addr    string
	timeout time.Duration

	mu   sync.Mutex
	conn net.Conn
	r    *bufio.Reader
}

// Dial creates a client. The connection is established lazily on the
// first command (or eagerly by calling Ping), so constructing a client
// pointed at a dead store is not itself an error — partition mode needs
// to represent "store configured but unreachable" as a normal state.
func Dial(addr string, timeout time.Duration) *Client {
	return &Client{addr: addr, timeout: timeout}
}

// Addr returns the configured store address.
func (c *Client) Addr() string { return c.addr }

func (c *Client) ensureConn() error {
	if c.conn != nil {
		return nil
	}
	conn, err := net.DialTimeout("tcp", c.addr, c.timeout)
	if err != nil {
		return err
	}
	c.conn = conn
	c.r = bufio.NewReader(conn)
	return nil
}

func (c *Client) drop() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.r = nil
	}
}

// Close tears down the current connection, if any.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.drop()
}

// Do sends one command and returns its reply:
//
//	simple string  -> string
//	error          -> resp.Error (returned as the value, not the error)
//	integer        -> int64
//	bulk string    -> []byte (nil for null bulk)
//	array          -> []any
//	null array     -> nil
//
// A non-nil Go error means the command could not be completed (dial,
// write, read, deadline) — i.e. the store is unreachable or misbehaving.
// That distinction is the signal partition mode is built on.
func (c *Client) Do(args ...string) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureConn(); err != nil {
		return nil, err
	}
	_ = c.conn.SetDeadline(time.Now().Add(c.timeout))
	if err := writeCommand(c.conn, args); err != nil {
		c.drop()
		return nil, err
	}
	v, err := readReply(c.r)
	if err != nil {
		c.drop()
		return nil, err
	}
	return v, nil
}

// Ping reports whether the store answers PING with PONG.
func (c *Client) Ping() bool {
	v, err := c.Do("PING")
	if err != nil {
		return false
	}
	s, ok := v.(string)
	return ok && s == "PONG"
}

func writeCommand(w io.Writer, args []string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func readReply(r *bufio.Reader) (any, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, errors.New("resp: empty reply")
	}
	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return Error(line[1:]), nil
	case ':':
		return strconv.ParseInt(line[1:], 10, 64)
	case '$':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return buf[:n], nil
	case '*':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, nil
		}
		out := make([]any, n)
		for i := 0; i < n; i++ {
			v, err := readReply(r)
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	}
	return nil, fmt.Errorf("resp: unexpected reply prefix %q", line[0])
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), nil
}

// Int64 extracts an int64 from a reply value.
func Int64(v any) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case []byte:
		n, err := strconv.ParseInt(string(t), 10, 64)
		return n, err == nil
	case string:
		n, err := strconv.ParseInt(t, 10, 64)
		return n, err == nil
	}
	return 0, false
}

// String extracts a string from a reply value.
func String(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	}
	return ""
}
