package main

import (
	"bufio"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"os"

	"go-raft-server/kvdb"
	"go-raft-server/kvraft"
	"go-raft-server/peer"
	"go-raft-server/raft"
	"go-raft-server/util"
)

func main() {
	// 加载.env文件环境变量
	env, err := util.LoadEnv()
	if err != nil {
		log.Fatalln(err)
	}
	envMe := env.Id
	util.Debug = env.Debug
	peersFilePath := env.PeersFilePath

	// 加载节点配置信息
	peers, err := peer.LoadPeers(peersFilePath)
	if err != nil {
		log.Fatalln(err)
	}

	// 使用 -me flag 重置环境变量 me
	flagMe := flag.Int("me", envMe, "节点 id")
	flag.Parse()
	me := *flagMe

	// 启动节点 Raft 服务
	logdb, err := kvdb.MakeKVDB(fmt.Sprintf("data/logdb%v", me))
	if err != nil {
		log.Fatalln(err)
	}
	applyCh := make(chan raft.ApplyMsg)
	go func() {
		for msg := range applyCh {
			fmt.Printf("收到 Raft ApplyMsg 回复：%v\n", msg)
		}
	}()
	gob.Register([]kvraft.Command{})
	service := raft.Make(peers, me, logdb, applyCh)

	// 启动 rpc 服务
	if _, err = util.StartRPCServer(fmt.Sprintf(":%v", peers[me].Port)); err != nil {
		log.Fatalf("启动节点 rpc 服务出错：%v\n", err)
	}
	log.Printf("节点 Raft 服务启动，监听地址：%v:%v\n", peers[me].Ip, peers[me].Port)

	// 启动命令行程序
	// 1. 创建通道
	inputCh := make(chan string)

	// 2. 启动协程读取输入
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() { // 循环读取每一行
			input := scanner.Text()
			if input == "" {
				continue
			}
			if input == "exit" { // 输入 Exit 时退出
				inputCh <- input
				return
			}
			blockOfCommands := make([]kvraft.Command, 100)
			for index := range blockOfCommands {
				blockOfCommands[index].CommandArgs = &kvraft.CommandArgs{Key: "key", Value: "", Version: 0, Op: kvraft.OpGet}
			}
			blockOfInputs := make([]int, 100)
			if input == "test Command" { // 输入 Test 时测试 tps
				for {
					// 调用 raft 服务
					fmt.Println(service.Start(blockOfCommands))
				}
			}
			if input == "test string" { // 输入 Test 时测试 tps
				for {
					// 调用 raft 服务
					fmt.Println(service.Start(blockOfInputs))
				}
			}
			// 调用 raft 服务
			fmt.Println(service.Start(input))
		}
	}()

	// 3. 主线程处理输入
	for input := range inputCh {
		if input == "exit" {
			fmt.Println("退出程序...")
			return
		}
	}
}

// package main

// import (
// 	"bufio"
// 	"flag"
// 	"fmt"
// 	"log"
// 	"os"

// 	"go-raft-server/kvraft"
// 	"go-raft-server/peer"
// 	"go-raft-server/persister"
// 	"go-raft-server/util"
// )

// func main() {

// 	// 加载.env文件环境变量
// 	env, err := util.LoadEnv()
// 	if err != nil {
// 		log.Fatalln(err)
// 	}
// 	envMe := env.Id
// 	util.Debug = env.Debug
// 	peersFilePath := env.PeersFilePath

// 	// 加载节点配置信息
// 	peers, err := peer.LoadPeers(peersFilePath)
// 	if err != nil {
// 		log.Fatalln(err)
// 	}

// 	// 使用 -me flag 重置环境变量 me
// 	flagMe := flag.Int("me", envMe, "节点 id")
// 	flag.Parse()
// 	me := *flagMe

// 	// 启动节点 KVRaft 服务
// 	persister := persister.MakePersister()
// 	maxraftstate := -1 // maxraftstate 快照大小，-1 代表不创建快照，
// 	_ = kvraft.StartKVServer(peers, me, persister, maxraftstate)

// 	// 启动 rpc 服务
// 	if _, err = util.StartRPCServer(fmt.Sprintf("%v:%v", peers[me].Ip, peers[me].Port)); err != nil {
// 		log.Fatalln(err)
// 	}
// 	log.Printf("节点 {%v}, 启动 Raft 和 KVRaft 服务，监听地址：%v:%v\n", me, peers[me].Ip, peers[me].Port)

// 	// 启动命令行程序
// 	// 1. 创建通道
// 	inputCh := make(chan string)

// 	// 2. 启动协程读取输入
// 	go func() {
// 		scanner := bufio.NewScanner(os.Stdin)
// 		for scanner.Scan() { // 循环读取每一行
// 			input := scanner.Text()
// 			if input == "" {
// 				continue
// 			}
// 			if input == "Exit" { // 输入 Exit 时退出
// 				inputCh <- input
// 				return
// 			}
// 		}
// 	}()

// 	// 3. 主线程处理输入
// 	for input := range inputCh {
// 		if input == "Exit" {
// 			fmt.Println("退出程序...")
// 			return
// 		}
// 	}
// }
