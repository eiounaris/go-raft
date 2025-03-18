package peer

import (
	"encoding/json"
	"fmt"
	"net/rpc"
	"os"
)

// === Peer

type Peer struct {
	Id   int    `json:"id"`
	Ip   string `json:"ip"`
	Port int    `json:"port"`
}

func LoadPeers(filepath string) ([]Peer, error) {
	peersJson, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}
	var peers []Peer
	err = json.Unmarshal([]byte(peersJson), &peers)
	if err != nil {
		return nil, err
	}
	return peers, nil
}

func (peer *Peer) Call(svc string, svcMeth string, args any, reply any) bool {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%v:%v", peer.Ip, peer.Port))
	if err != nil {
		return false
	}
	err = client.Call(svc+"."+svcMeth, args, reply)
	return err == nil
}
