package kvraft

import (
	"github.com/eiounaris/go-raft-client/peer"
)

type Clerk struct {
	servers  []peer.Peer
	leaderId int
}

func MakeClerk(servers []peer.Peer) *Clerk {
	return &Clerk{
		servers:  servers,
		leaderId: 0,
	}
}

func (ck *Clerk) Get(key string) *CommandReply {
	return ck.ExecuteCommand(&CommandArgs{Key: key, Op: OpGet})
}

func (ck *Clerk) Put(key string, value string, version int) *CommandReply {
	return ck.ExecuteCommand(&CommandArgs{Key: key, Value: value, Version: version, Op: OpPut})
}

func (ck *Clerk) ExecuteCommand(args *CommandArgs) *CommandReply {
	reply := new(CommandReply)
	for {
		if ok := ck.servers[ck.leaderId].Call("KVServer", "ExecuteCommand", args, reply); !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		return reply
	}
}
