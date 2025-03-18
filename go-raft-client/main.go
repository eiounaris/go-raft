package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/eiounaris/go-raft-client/kvraft"
	"github.com/eiounaris/go-raft-client/peer"
	"github.com/eiounaris/go-raft-client/util"
)

func main() {

	// 加载.env文件环境变量
	env, err := util.LoadEnv()
	if err != nil {
		log.Fatalln(err)
	}

	// 加载节点配置信息
	peers, err := peer.LoadPeers(env.PeersFilePath)
	if err != nil {
		log.Fatalln(err)
	}

	// 启动节点 Clerk 服务
	ck := kvraft.MakeClerk(peers)

	// 启动命令行程序
	fmt.Println("1. Get <key>                    - 查询键值")
	fmt.Println("2. Put <key> <value> <version>  - 设置键值")
	fmt.Println("3. Exit                         - 退出程序")
	fmt.Println("4. Test                         - 测试 TPS")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}
		if input == "Exit" {
			fmt.Println("退出程序...")
			return
		}
		parts := strings.Fields(input)
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "Get":
			if len(parts) != 2 {
				fmt.Println("错误：参数错误，使用方式: Get <key>")
				continue
			}
			key := parts[1]
			fmt.Println("等待执行 Get 命令")
			reply := ck.Get(key)
			fmt.Printf("查询结果: %v\n", reply)

		case "Put":
			if len(parts) != 4 {
				fmt.Println("错误：参数错误，使用方式: Put <key> <value> <version>")
				continue
			}
			key := parts[1]
			value := parts[2]
			version, err := strconv.Atoi(parts[3])
			if err != nil {
				fmt.Println("错误：version 字段为大于 0 的数字")
			}
			fmt.Println("等待执行 Put 命令")
			reply := ck.Put(key, value, version)
			fmt.Printf("执行结果: %v\n", reply)
		case "Test":
			if len(parts) != 2 {
				fmt.Println("错误：参数错误，使用方式: Test <key>")
				continue
			}
			key := parts[1]
			for {
				fmt.Println("等待执行 Get 命令")
				reply := ck.Get(key)
				fmt.Printf("查询结果: %v\n", reply)
			}

		default:
			fmt.Println("未知命令，支持命令格式如下:")
			fmt.Println("1. Get <key>                    - 查询键值")
			fmt.Println("2. Put <key> <value> <version>  - 设置键值")
			fmt.Println("3. Exit                         - 退出程序")
		}
	}
}
