// Command counterd provides a persistent, durable, centralised counter
// service with a simple, efficient binary protocol. It exists specifically
// to support the use case of a counter with built in time-to-live logic for
// counter updates.
package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"

	"fknsrs.biz/p/counterd/types"
	"github.com/Sirupsen/logrus"
	"github.com/boltdb/bolt"
)

func remove(s []chan types.Message, c chan types.Message) []chan types.Message {
	for i, v := range s {
		if v == c {
			s[i] = s[len(s)-1]
			s[len(s)-1] = nil
			s = s[:len(s)-1]
			break
		}
	}

	return s
}

func uint32Bytes(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func uint64Bytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func float64Bytes(v float64) []byte {
	b := bytes.NewBuffer(nil)
	binary.Write(b, binary.BigEndian, v)
	return b.Bytes()
}

type kv struct {
	K uint64
	V float64
}

var (
	addr          = flag.String("addr", ":2098", "Address to listen on.")
	logLevel      = flag.String("log_level", "info", "Minimum log level. Options are error, warn, info, debug.")
	dbFile        = flag.String("database_file", "counterd.db", "File to store the persistent database.")
	checkInterval = flag.Duration("check_interval", time.Second, "Interval at which to attempt applying deltas.")
)

func main() {
	flag.Parse()

	level, err := logrus.ParseLevel(*logLevel)
	if err != nil {
		logrus.WithField("error", err.Error()).Fatal("couldn't parse log level")
	}
	logrus.SetLevel(level)

	var dmtx sync.RWMutex
	data := make(map[uint64]float64)

	var smtx sync.RWMutex
	subs := make(map[uint64][]chan types.Message)

	db, err := bolt.Open(*dbFile, 0644, nil)
	if err != nil {
		panic(err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("deltas")); err != nil {
			return err
		}

		return nil
	}); err != nil {
		panic(err)
	}

	if err := db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("deltas")).ForEach(func(kb []byte, vb []byte) error {
			var p kv

			if err := binary.Read(bytes.NewReader(vb), binary.BigEndian, &p); err != nil {
				return err
			}

			data[p.K] = data[p.K] + p.V

			return nil
		})
	}); err != nil {
		panic(err)
	}

	applyChanges := func() error {
		t := uint64(time.Now().Unix())

		delta := make(map[uint64]float64)

		n := 0

		if err := db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("deltas"))

			c := b.Cursor()
			for kb, vb := c.First(); kb != nil; kb, vb = c.Next() {
				k := binary.BigEndian.Uint64(kb)
				if k > t {
					logrus.Debug("found delta for the future; bailing out")

					break
				}

				var p kv

				if err := binary.Read(bytes.NewReader(vb), binary.BigEndian, &p); err != nil {
					return err
				}

				delta[p.K] = delta[p.K] + p.V

				if err := c.Delete(); err != nil {
					return err
				}

				n++
			}

			return nil
		}); err != nil {
			return err
		}

		logrus.WithField("count", n).Debug("applying deltas")

		dmtx.Lock()
		defer dmtx.Unlock()

		for k, v := range delta {
			logrus.WithFields(logrus.Fields{
				"key":   k,
				"value": data[k],
				"delta": v,
			}).Debug("applying delta")

			data[k] = data[k] - v

			for _, c := range subs[k] {
				c <- types.NotifyMessage{
					Key: k,
					Val: data[k],
				}
			}
		}

		return nil
	}

	go func() {
		for {
			if err := applyChanges(); err != nil {
				panic(err)
			}

			time.Sleep(*checkInterval)
		}
	}()

	l, err := net.Listen("tcp4", *addr)
	if err != nil {
		logrus.WithField("error", err.Error()).Fatal("couldn't open listening socket")
	}

	logrus.WithField("addr", l.Addr().String()).Info("listening")

	for {
		s, err := l.Accept()
		if err != nil {
			if err == io.EOF {
				break
			}

			logrus.WithField("error", err.Error()).Warn("couldn't accept connection")

			continue
		}

		logrus.WithFields(logrus.Fields{
			"remote": s.RemoteAddr().String(),
			"local":  s.LocalAddr().String(),
		}).Info("got connection")

		go func() {
			defer func() {
				if e := recover(); e != nil {
					logrus.WithFields(logrus.Fields{
						"remote": s.RemoteAddr().String(),
						"local":  s.LocalAddr().String(),
						"error":  e,
					}).Warn("error occurred with client")
				}
			}()

			ch := make(chan types.Message, 10)
			mysubs := make(map[uint64]bool)

			defer func() {
				logrus.WithFields(logrus.Fields{
					"remote": s.RemoteAddr().String(),
					"local":  s.LocalAddr().String(),
				}).Info("connection closed")
			}()

			defer func() {
				if len(mysubs) == 0 {
					return
				}

				smtx.Lock()
				for k := range mysubs {
					subs[k] = remove(subs[k], ch)
				}
				smtx.Unlock()

				close(ch)
			}()

			go func() {
				for m := range ch {
					logrus.WithFields(logrus.Fields{
						"remote": s.RemoteAddr().String(),
						"local":  s.LocalAddr().String(),
						"type":   m.Type().String(),
					}).Debug("sending message")

					if err := types.WriteMessage(s, m); err != nil {
						panic(err)
					}
				}
			}()

			b := bufio.NewReader(s)

			for {
				m, err := types.ReadMessage(b)
				if err != nil {
					if err == io.EOF {
						break
					}

					panic(err)
				}

				logrus.WithFields(logrus.Fields{
					"remote": s.RemoteAddr().String(),
					"local":  s.LocalAddr().String(),
					"type":   m.Type().String(),
				}).Debug("received message")

				switch m := m.(type) {
				case *types.IncrementMessage:
					if err := db.Update(func(t *bolt.Tx) error {
						k := bytes.NewBuffer(nil)
						if err := binary.Write(k, binary.BigEndian, kv{K: uint64(m.TTE), V: rand.Float64()}); err != nil {
							return err
						}

						v := bytes.NewBuffer(nil)
						if err := binary.Write(v, binary.BigEndian, kv{K: m.Key, V: m.Val}); err != nil {
							return err
						}

						if err := t.Bucket([]byte("deltas")).Put(k.Bytes(), v.Bytes()); err != nil {
							return err
						}

						return nil
					}); err != nil {
						panic(err)
					}

					dmtx.Lock()

					data[m.Key] = data[m.Key] + m.Val

					for _, c := range subs[m.Key] {
						c <- types.NotifyMessage{
							Key: m.Key,
							Val: data[m.Key],
						}
					}

					dmtx.Unlock()
				case *types.SubscribeMessage:
					smtx.Lock()
					subs[m.Key] = append(subs[m.Key], ch)
					mysubs[m.Key] = true
					smtx.Unlock()
				case *types.UnsubscribeMessage:
					smtx.Lock()
					subs[m.Key] = remove(subs[m.Key], ch)
					delete(mysubs, m.Key)
					smtx.Unlock()
				case *types.QueryMessage:
					dmtx.RLock()
					v := data[m.Key]
					dmtx.RUnlock()
					ch <- types.NotifyMessage{
						Key: m.Key,
						Val: v,
					}
				}
			}
		}()
	}
}
