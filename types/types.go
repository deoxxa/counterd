package types

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var (
	ErrUnexepectedEOF = errors.New("unexpected EOF")
)

type MessageType uint8

func (m MessageType) String() string {
	switch m {
	case ZeroType:
		return "Zero"
	case IncrementType:
		return "Increment"
	case SubscribeType:
		return "Subscribe"
	case UnsubscribeType:
		return "Unsubscribe"
	case NotifyType:
		return "Notify"
	case QueryType:
		return "Query"
	default:
		return fmt.Sprintf("Unknown (%02x)", uint8(m))
	}
}

const (
	ZeroType MessageType = iota
	IncrementType
	SubscribeType
	UnsubscribeType
	NotifyType
	QueryType
)

type Message interface {
	Type() MessageType
}

func ReadMessage(r io.Reader) (Message, error) {
	var t MessageType
	if err := binary.Read(r, binary.BigEndian, &t); err != nil {
		return nil, err
	}

	var m Message
	switch t {
	case IncrementType:
		m = &IncrementMessage{}
	case SubscribeType:
		m = &SubscribeMessage{}
	case UnsubscribeType:
		m = &UnsubscribeMessage{}
	case NotifyType:
		m = &NotifyMessage{}
	case QueryType:
		m = &QueryMessage{}
	default:
		return nil, fmt.Errorf("invalid message type %02x", t)
	}

	if err := binary.Read(r, binary.BigEndian, m); err != nil {
		if err == io.EOF {
			return nil, ErrUnexepectedEOF
		}

		return nil, err
	}

	return m, nil
}

func WriteMessage(w io.Writer, m Message) error {
	if err := binary.Write(w, binary.BigEndian, m.Type()); err != nil {
		return err
	}

	return binary.Write(w, binary.BigEndian, m)
}

type IncrementMessage struct {
	Key uint64
	Val float64
	TTE uint32
}

func (m IncrementMessage) Type() MessageType { return IncrementType }

type SubscribeMessage struct {
	Key uint64
}

func (m SubscribeMessage) Type() MessageType { return SubscribeType }

type UnsubscribeMessage struct {
	Key uint64
}

func (m UnsubscribeMessage) Type() MessageType { return UnsubscribeType }

type NotifyMessage struct {
	Key uint64
	Val float64
}

func (m NotifyMessage) Type() MessageType { return NotifyType }

type QueryMessage struct {
	Key uint64
}

func (m QueryMessage) Type() MessageType { return QueryType }
