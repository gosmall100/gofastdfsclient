package gofastdfsclient

import (
	"container/list"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	MAXCONNS_LEAST = 5
)

type pConn struct {
	net.Conn
	pool *connPool
}

func (p pConn) Close() error {
	return p.pool.put(p)
}

type connPool struct {
	conns    *list.List // list
	addr     string
	maxConns int
	count    int
	lock     *sync.RWMutex
	finish   chan bool
}

func newConnPool(addr string, maxConns int) (*connPool, error) {
	if maxConns < MAXCONNS_LEAST {
		return nil, fmt.Errorf("too little maxConns < %d", MAXCONNS_LEAST)
	}

	cp := &connPool{
		conns:    list.New(),
		addr:     addr,
		maxConns: maxConns,
		lock:     &sync.RWMutex{},
		finish:   make(chan bool),
	}

	go func() {
		timer := time.NewTimer(time.Second * 20)
		for {
			select {
			case finish := <-cp.finish:
				if finish {
					return
				}
			case <-timer.C:
				_ = cp.CheckConns()
				timer.Reset(time.Second * 20)
			}
		}
	}()

	cp.lock.Lock()
	defer cp.lock.Unlock()
	for i := 0; i < MAXCONNS_LEAST; i++ {
		if err := cp.makeConn(); err != nil {
			return nil, err
		}
	}

	return cp, nil
}

func (c *connPool) Destory() {
	if c == nil {
		return
	}
	c.finish <- true
}

// CheckConns is typically used to check if the connection is valid.
//When the client uses the Connection pool, it can maintain a long
//connection with the server by sending an active test after the
//connection is established
func (c *connPool) CheckConns() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	for e, next := c.conns.Front(), new(list.Element); e != nil; e = next {
		next = e.Next()
		conn := e.Value.(pConn)

		hr := &header{
			cmd: FDFS_PROTO_CMD_ACTIVE_TEST,
		}
		// send req
		if err := hr.SendHeader(conn.Conn); err != nil {
			c.conns.Remove(e)
			c.count--
			continue
		}
		// recv resp
		if err := hr.RecvHeader(conn.Conn); err != nil {
			c.conns.Remove(e)
			c.count--
			continue
		}

		// 判断是否为 tracker 响应码
		if hr.cmd != TRACKER_PROTO_CMD_RESP || hr.status != 0 {
			c.conns.Remove(e)
			c.count--
			continue
		}
	}
	return nil
}

func (c *connPool) makeConn() error {

	conn, err := net.DialTimeout("tcp", c.addr, time.Second*10)
	if err != nil {
		return err
	}

	c.conns.PushBack(pConn{
		Conn: conn,
		pool: c,
	})

	c.count++

	return nil
}

func (c *connPool) get() (net.Conn, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	for {
		e := c.conns.Front()
		if e == nil {
			if c.count >= c.maxConns {
				return nil, fmt.Errorf("reach maxConns %d", c.maxConns)
			}
			_ = c.makeConn()
			continue
		}

		c.conns.Remove(e)
		conn := e.Value.(pConn)
		return conn, nil
	}

	return nil, nil
}

func (c *connPool) put(pConn pConn) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	pConn.pool.conns.PushBack(pConn)
	return nil
}

