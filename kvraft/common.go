package kvraft

import (
	"fmt"
	"time"
)

// === ExecuteTimeout

const ExecuteTimeout = 1000 * time.Millisecond

// === Err

type Err uint8

const (
	Ok Err = iota
	ErrNoKey
	ErrWrongLeader
	ErrTimeout
	ErrVersion
)

func (err Err) String() string {
	switch err {
	case Ok:
		return "Ok"
	case ErrNoKey:
		return "ErrNoKey"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrVersion:
		return "ErrVersion"
	case ErrTimeout:
		return "ErrTimeout"
	}
	panic(fmt.Sprintf("unexpected Err %d", err))
}

// === OpType

type OpType uint8

const (
	OpGet OpType = iota
	OpPut
)

func (opType OpType) String() string {
	switch opType {
	case OpGet:
		return "Get"
	case OpPut:
		return "Put"

	}
	panic(fmt.Sprintf("unexpected OpType %d", opType))
}

// === CommandArgs

type CommandArgs struct {
	Key     string
	Value   string
	Version int
	Op      OpType
}

func (args CommandArgs) String() string {
	return fmt.Sprintf("{Key:%v, Value:%v, version:%v, Op:%v}", args.Key, args.Value, args.Version, args.Op)
}

// === CommandReply

type CommandReply struct {
	Value   string
	Version int
	Err     Err
}

func (reply CommandReply) String() string {
	return fmt.Sprintf("{Value:%v, Version:%v, Err:%v}", reply.Value, reply.Version, reply.Err)
}

// === Command

type Command struct {
	*CommandArgs
}
