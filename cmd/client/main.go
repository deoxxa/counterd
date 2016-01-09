package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"fknsrs.biz/p/counterd/client"
)

var (
	addr      = flag.String("addr", "127.0.0.1:2098", "Address of counterd server.")
	key       = flag.Uint64("key", 0, "Key to operate on (required).")
	get       = flag.Bool("get", false, "Get the value at a specific key.")
	monitor   = flag.Bool("monitor", false, "Monitor the value at a specific key.")
	timeout   = flag.Duration("timeout", time.Second, "Specify the timeout for a read operation.")
	increment = flag.Float64("increment", 0, "Increment the value at key by this amount.")
	ttl       = flag.Duration("ttl", time.Minute, "Set the time-to-live of an increment operation.")
)

func main() {
	flag.Parse()

	if *key == 0 {
		log.Fatal("key needs to be not zero")
	}

	c, err := client.Dial(*addr)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	switch {
	case *get:
		v, err := c.ReadOrQuery(*key, *timeout)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%d = %v\n", *key, v)
	case *monitor:
		ch := make(chan client.KV, 1)
		c.Monitor(ch)
		if err := c.Subscribe(*key); err != nil {
			panic(err)
		}
		for kv := range ch {
			fmt.Printf("%d = %v\n", kv.K, kv.V)
		}
	case *increment != 0:
		if err := c.Increment(*key, *increment, *ttl); err != nil {
			panic(err)
		}
	}
}
