package raft

import (
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
	Command any
	Term    int
	Index   int
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) getFirstLog() LogEntry {
	return rf.logs[0]
}

// --- shrinkEntries

func shrinkEntries(entries []LogEntry) []LogEntry {
	const lenMultiple = 2
	if cap(entries) > len(entries)*lenMultiple {
		newEntries := make([]LogEntry, len(entries))
		copy(newEntries, entries)
		return newEntries
	}
	return entries
}

// --- ApplyMsg

type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
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
