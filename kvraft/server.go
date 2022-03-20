package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	Key   string
	Value string
}

// RequestInfo
// indexed by client ID, contains just seq #, and value if already executed
// RPC handler first checks table, only Start()s if seq # > table entry
// each log entry must include client ID, seq #
// when operation appears on applyCh
//   update the seq # and value in the client's table entry
//   wake up the waiting RPC handler (if any)
type RequestInfo struct {
	// unique identifier for each client
	clientID int
	// each request is tagged with a monotonically increasing sequence number
	seq int
	// value if already executed
	value interface{}
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// a simple database of key/value pairs
	db map[string]string
	// contains message from apply ch
	logs map[int]chan bool
	// keeps track of the latest sequence number it has seen for each client
	requests []RequestInfo
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{"Get", args.Key, ""}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	kv.logs[index] = make(chan bool)
	kv.mu.Unlock()

	<-kv.logs[index]

	kv.mu.Lock()
	delete(kv.logs, index)
	if val, ok := kv.db[args.Key]; !ok {
		reply.Err = ErrNoKey
	} else {
		reply.Value = val
		reply.Err = OK
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{args.Op, args.Key, args.Value}
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	kv.logs[index] = make(chan bool)
	kv.mu.Unlock()

	<-kv.logs[index]
	reply.Err = OK

	kv.mu.Lock()
	delete(kv.logs, index)
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.logs = make(map[int]chan bool)

	go kv.applier()

	return kv
}
func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		if m.CommandValid {
			kv.mu.Lock()
			kv.logs[m.CommandIndex] <- true
			kv.mu.Unlock()

			kv.mu.Lock()
			op := m.Command.(Op)
			if op.Type == "Put" {
				kv.db[op.Key] = op.Value
			} else if op.Type == "Append" {
				kv.db[op.Key] += op.Value
			}
			kv.mu.Unlock()
		}
	}
}
