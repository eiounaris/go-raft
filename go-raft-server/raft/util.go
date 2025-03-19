package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// --- Timeout

// const ElectionTimeout = 150
// const HeartbeatTimeout = 10

const ElectionTimeout = 2000
const HeartbeatTimeout = 1000

type LockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *LockedRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rand.Intn(n)
}

var GlobalRand = &LockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

func RandomElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+GlobalRand.Intn(ElectionTimeout)) * time.Millisecond
}

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}

// --- NodeState

type NodeState uint8

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (state NodeState) String() string {
	switch state {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	panic("unexpected NodeState")
}

// --- LogEntry

type LogEntry struct {
	Index   int
	Term    int
	Command any
}

func (rf *Raft) getLogByIndex(index int) (*LogEntry, error) {
	indexLogBytes, err := rf.logdb.Get([]byte(strconv.Itoa(index)))
	if err != nil {
		return nil, err
	}
	var indexLog LogEntry
	r := bytes.NewBuffer(indexLogBytes)
	d := gob.NewDecoder(r)
	if err := d.Decode(&indexLog); err != nil {
		return nil, err
	}
	return &indexLog, nil
}

func (rf *Raft) storeLogEntry(logentry *LogEntry) error {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	err := e.Encode(logentry)
	if err != nil {
		log.Panicln(err)
	}
	err = rf.logdb.Set([]byte(strconv.Itoa(logentry.Index)), w.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (rf *Raft) getLogsInRange(fromIndex, toIndex int) ([]LogEntry, error) {
	var logs []LogEntry
	if toIndex < fromIndex {
		return logs, nil
	}
	logs = make([]LogEntry, 0, toIndex-fromIndex+1)
	for index := fromIndex; index <= toIndex; index++ {
		log, err := rf.getLogByIndex(index)
		if err != nil {
			return nil, err
		}
		logs = append(logs, *log)
	}
	return logs, nil
}

func (rf *Raft) getLastLog() (*LogEntry, error) {
	lastLogBytes, err := rf.logdb.Get([]byte(strconv.Itoa(rf.lastLogIndex)))
	if err != nil {
		return nil, err
	} else {
		var lastLog LogEntry
		r := bytes.NewBuffer(lastLogBytes)
		d := gob.NewDecoder(r)
		if err = d.Decode(&lastLog); err != nil {
			return nil, err
		}
		return &lastLog, nil
	}
}

// --- ApplyMsg

type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int
	CommandTerm  int
}

func (applymsg ApplyMsg) String() string {
	return fmt.Sprintf("{CommandValid:%v, Command:%v, CommandIndex:%v, CommandTerm:%v}",
		applymsg.CommandValid, applymsg.Command, applymsg.CommandIndex, applymsg.CommandTerm)
}

// --- util

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
