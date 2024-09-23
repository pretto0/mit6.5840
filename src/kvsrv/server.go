package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex
	Data map[string]string
	StateMap sync.Map 
 
	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.Data[args.Key]
	// Your code here.
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.State == Report {
		kv.StateMap.Delete(args.Id)
	}
	res, ok := kv.StateMap.Load(args.Id)
	if ok {
		reply.Value = res.(string)
		return
	}

	kv.mu.Lock()
	old := kv.Data[args.Key]
	kv.Data[args.Key] = args.Value
	reply.Value = old
	kv.mu.Unlock()

	kv.StateMap.Store(args.Id,old)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.State == Report {
		kv.StateMap.Delete(args.Id)
	}
	res, ok := kv.StateMap.Load(args.Id)
	if ok {
		reply.Value = res.(string)
		return
	}

	kv.mu.Lock()
	old := kv.Data[args.Key]
	kv.Data[args.Key] = old + args.Value
	reply.Value = old
	kv.mu.Unlock()

	kv.StateMap.Store(args.Id,old)
}

func StartKVServer() *KVServer {
	kv := &KVServer{
		Data: make(map[string]string),
	}

	// You may need initialization code here.

	return kv
}
