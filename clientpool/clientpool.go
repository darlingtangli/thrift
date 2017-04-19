/**
* @file clientpool.go
* @brief thrift client pool
* @author darlingtangli@gmail.com
* @version 1.0
* @date 2017-04-19
 */
package clientpool

import (
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
	"reflect"
	"sync"
	"time"
)

var (
	ErrNoIdleClient = errors.New("No idle client")
)

type Client interface{}

type ClientFactory interface {
	CreateClient(serverAddr string) (Client, error)
}

type ClientPoolItem struct {
	// thrift client
	client Client
	// true to destory client and false to reuse it
	broken bool
	// remote server addr
	serverAddr string
}

func (item *ClientPoolItem) Client() Client {
	return item.client
}

func (item *ClientPoolItem) Broken() {
	item.broken = true
	return
}

func (item *ClientPoolItem) getTransport() thrift.TTransport {
	var transport thrift.TTransport
	var ok bool
	if v := reflect.ValueOf(item.client).Elem().FieldByName("Transport"); !v.IsValid() {
		panic(v)
	} else if v.IsNil() {
		panic(v)
	} else if transport, ok = v.Interface().(thrift.TTransport); !ok {
		panic(v)
	}
	return transport
}

func (item *ClientPoolItem) destory() error {
	return item.getTransport().Close()
}

type ServerAddrNumMap map[string]int

type ClientPool struct {
	// for creating client
	clientFactory ClientFactory
	// store idle clients
	idleItems chan *ClientPoolItem
	// store clients to create
	creates chan string
	// defer to create client for waiting remote server avalible
	createDefer time.Duration
	// join all pool corotine
	waitGroup sync.WaitGroup
	// signal close
	close bool
}

// New ClientPool
func New(m ServerAddrNumMap, d time.Duration, f ClientFactory) (*ClientPool, error) {
	pool := &ClientPool{
		createDefer:   d,
		clientFactory: f,
		waitGroup:     sync.WaitGroup{},
		close:         false,
	}
	// create chan
	sum := 0
	for _, num := range m {
		sum += num
	}
	pool.idleItems = make(chan *ClientPoolItem, sum)
	pool.creates = make(chan string, sum)
	// dispatch all server addr to create list and launch creator corotine
	for serverAddr, num := range m {
		for i := 0; i < num; i++ {
			pool.dispatchCreator(serverAddr, false)
		}
	}
	pool.launchCreator()

	return pool, nil
}

func (pool *ClientPool) Get(timeout time.Duration) (*ClientPoolItem, error) {
	var item *ClientPoolItem
	var err error
	select {
	case item = <-pool.idleItems:
	case <-time.After(timeout):
		err = ErrNoIdleClient
	}
	return item, err
}

func (pool *ClientPool) Recycle(item *ClientPoolItem) {
	if item.broken {
		item.destory()
		pool.dispatchCreator(item.serverAddr, true)
	} else {
		pool.idleItems <- item
	}
	return
}

func (pool *ClientPool) Idles() int {
	return len(pool.idleItems)
}

func (pool *ClientPool) Close() error {
	pool.close = true
	pool.creates <- "" // notify createor corotine to exit
	var err error
	close(pool.idleItems)
	for item := range pool.idleItems {
		e := item.destory()
		if e != nil {
			err = e
		}
	}
	// wail for createor and dispatcher corotine exit
	pool.waitGroup.Wait()
	close(pool.creates)
	return err
}

func (pool *ClientPool) createClient(serverAddr string) (Client, error) {
	return pool.clientFactory.CreateClient(serverAddr)
}

func (pool *ClientPool) launchCreator() {
	pool.waitGroup.Add(1)
	go func() {
		defer pool.waitGroup.Done()
		for {
			select {
			case serverAddr := <-pool.creates:
				if len(serverAddr) == 0 {
					return
				}
				client, err := pool.createClient(serverAddr)
				if err != nil {
					log.Println("create client fail:", err)
					// retry to create after a moment
					pool.dispatchCreator(serverAddr, true)
				} else {
					// push the new create client to idle list
					pool.idleItems <- &ClientPoolItem{client, false, serverAddr}
				}
			}
		}
	}()
}

func (pool *ClientPool) dispatchCreator(serverAddr string, sleep bool) {
	pool.waitGroup.Add(1)
	go func() {
		defer pool.waitGroup.Done()
		if !pool.close {
			if sleep {
				time.Sleep(pool.createDefer)
			}
			pool.creates <- serverAddr
		}
	}()
	return
}
