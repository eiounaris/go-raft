package kvraft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"go-raft-server/peer"
	"go-raft-server/persister"
	"go-raft-server/raft"
	"go-raft-server/util"
)

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	lastApplied  int //record the last applied index to avoid duplicate apply

	stateMachine KVStateMachine
	notifyChs    map[int]chan *CommandReply
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) ExecuteCommand(args *CommandArgs, reply *CommandReply) error {
	index, _, isLeader := kv.rf.Start(Command{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return nil
	}
	kv.mu.Lock()
	ch := kv.getNotifyCh(index)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		reply.Value, reply.Err = result.Value, result.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}
	go func() {
		kv.mu.Lock()
		kv.deleteNotifyCh(index)
		kv.mu.Unlock()
	}()
	return nil
}

func (kv *KVServer) getNotifyCh(index int) chan *CommandReply {
	if _, ok := kv.notifyChs[index]; !ok {
		kv.notifyChs[index] = make(chan *CommandReply, 1)
	}
	return kv.notifyChs[index]
}

func (kv *KVServer) deleteNotifyCh(index int) {
	delete(kv.notifyChs, index)
}

func (kv *KVServer) applyLogToStateMachine(command Command) *CommandReply {
	reply := new(CommandReply)
	switch command.Op {
	case OpGet:
		reply.Value, reply.Version, reply.Err = kv.stateMachine.Get(command.Key)
	case OpPut:
		reply.Err = kv.stateMachine.Put(command.Key, command.Value, command.Version)
	}
	return reply
}

func (kv *KVServer) needSnapshot() bool {
	return kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate
}

func (kv *KVServer) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

func (kv *KVServer) restoreStateFromSnapshot(snapshot []byte) {
	if len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := gob.NewDecoder(r)
	var stateMachine MemoryKV
	if d.Decode(&stateMachine) != nil {
		panic("Failed to restore state from snapshot")
	}
	kv.stateMachine = &stateMachine
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		message := <-kv.applyCh
		util.DPrintf("{Node %v} tries to apply message %v", kv.rf.GetId(), message)
		util.DPrintf("{Node %v} 当前数据库为 %v", kv.rf.GetId(), kv.stateMachine)
		if message.CommandValid {
			kv.mu.Lock()
			if message.CommandIndex <= kv.lastApplied {
				util.DPrintf("{Node %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.rf.GetId(), message, kv.lastApplied)
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = message.CommandIndex

			reply := new(CommandReply)
			command := message.Command.(Command) // type assertion

			reply = kv.applyLogToStateMachine(command)

			// just notify related channel for currentTerm's log when node is leader
			if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
				ch := kv.getNotifyCh(message.CommandIndex)
				ch <- reply
			}
			if kv.needSnapshot() {
				kv.takeSnapshot(message.CommandIndex)
			}

			kv.mu.Unlock()
		} else if message.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
				kv.restoreStateFromSnapshot(message.Snapshot)
				kv.lastApplied = message.SnapshotIndex
			}
			kv.mu.Unlock()
		} else {
			panic(fmt.Sprintf("Invalid ApplyMsg %v", message))
		}
	}
}

func StartKVServer(servers []peer.Peer, me int, persister *persister.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Command{})
	applyCh := make(chan raft.ApplyMsg)

	kv := &KVServer{
		mu:           sync.RWMutex{},
		me:           me,
		rf:           raft.Make(servers, me, persister, applyCh),
		applyCh:      applyCh,
		dead:         0,
		maxraftstate: maxraftstate,
		stateMachine: NewMemoryKV(),
		notifyChs:    make(map[int]chan *CommandReply),
	}

	kv.restoreStateFromSnapshot(persister.ReadSnapshot())
	go kv.applier()

	err := util.RegisterRPCService(kv)
	if err != nil {
		log.Fatalf("启动节点Raft rpc服务出错： %v\n", err)
	}
	return kv
}
