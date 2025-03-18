package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// --- Timeout

// const ElectionTimeout = 150
// const HeartbeatTimeout = 10

const ElectionTimeout = 10000
const HeartbeatTimeout = 5000

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

func (rf *Raft) getLogByIndex(index int) *LogEntry {
	key := fmt.Appendf(nil, "%v", index)
	value, err := rf.logdb.Get(key)
	if err != nil {
		log.Fatalln(err)
	}
	var logentry LogEntry
	r := bytes.NewBuffer(value)
	d := gob.NewDecoder(r)
	if err := d.Decode(&logentry); err != nil {
		log.Fatalln(err)
	}
	return &logentry
}

func (rf *Raft) storeLogEntry(logentry *LogEntry) error {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(logentry)
	err := rf.logdb.Set(fmt.Appendf(nil, "%v", logentry.Index), w.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (rf *Raft) deleteLogEntrysFromIndex(index int) error {
	lastLogIndex := rf.getLastLogIndex()
	for index <= lastLogIndex {
		rf.logdb.Delete(fmt.Appendf(nil, "%v", index))
		index++
	}
	return nil
}

func (rf *Raft) getLastLogIndex() int {
	return rf.lastLogIndex
}

func (rf *Raft) getFirstLogIndex() int {
	return rf.firstLogIndex
}
func (rf *Raft) getFirstLog() *LogEntry {
	firstLogBytes, err := rf.logdb.Get(fmt.Appendf(nil, "%v", rf.getFirstLogIndex()))
	if err != nil {
		return nil
	} else {
		var firstLog LogEntry
		r := bytes.NewBuffer(firstLogBytes)
		d := gob.NewDecoder(r)

		if err = d.Decode(&firstLog); err != nil {
			log.Fatalln(err)
		}
		return &firstLog
	}
}

func (rf *Raft) getLogsInRange(biginIndex, endIndex int) []LogEntry {
	var logs []LogEntry
	for index := biginIndex; index <= endIndex; index++ {
		logs = append(logs, *rf.getLogByIndex(index))
	}
	return logs
}

func (rf *Raft) getLastLog() *LogEntry {
	lastLogBytes, err := rf.logdb.Get([]byte(fmt.Sprintf("%v", rf.getLastLogIndex())))
	if err != nil {
		return nil
	} else {
		var lastLog LogEntry
		r := bytes.NewBuffer(lastLogBytes)
		d := gob.NewDecoder(r)

		if err = d.Decode(&lastLog); err != nil {
			log.Fatalln(err)
		}
		return &lastLog
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
	return fmt.Sprintf("{CommandValid:%v, Command:%v, CommandIndex:%v, CommandTerm:%v}", applymsg.CommandValid, applymsg.Command, applymsg.CommandIndex, applymsg.CommandTerm)
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
