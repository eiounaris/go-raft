package util

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// === Debugging

const Debug = true

func DPrintf(format string, a ...any) {
	if Debug {
		log.Printf(format, a...)
	}
}

// === Env

type Env struct {
	Id            int
	Debug         bool
	PeersFilePath string
}

func LoadEnv() (*Env, error) {
	err := godotenv.Load(".env")
	if err != nil {
		return nil, err
	}

	idString, ok := os.LookupEnv("id")
	if !ok {
		return nil, errors.New("cann't find id env")
	}
	id, err := strconv.Atoi(idString)
	if err != nil {
		return nil, err
	}

	debugString, ok := os.LookupEnv("debug")
	if !ok {
		return nil, errors.New("cann't find debug env")
	}
	debug, err := strconv.ParseBool(debugString)
	if err != nil {
		return nil, err
	}

	peersFilePath, ok := os.LookupEnv("peersFilePath")
	if !ok {
		return nil, errors.New("cann't find peersFilePath env")
	}
	return &Env{Id: id, Debug: debug, PeersFilePath: peersFilePath}, nil
}

// === RegisterRPCService

func RegisterRPCService(service any) error {
	err := rpc.Register(service)
	if err != nil {
		return err
	}
	return nil
}

// === StartRPCServer

func StartRPCServer(address string) (net.Listener, error) {
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	go http.Serve(listener, nil)

	return listener, nil
}
