package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

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

	ClientID int
	Seq      int
	Index    int
}

// RequestInfo
// indexed by client ID, contains just seq #, and value if already executed
// RPC handler first checks table, only Start()s if seq # > table entry
// each log entry must include client ID, seq #
type RequestInfo struct {
	// each request is tagged with a monotonically increasing sequence number
	Seq int
	// value if already executed
	Value string
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
	// contains request's indexes from apply ch
	logs map[int]chan bool
	// keeps track of the latest sequence number it has seen for each client
	clients map[int]RequestInfo

	persister *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index := int(nrand())
	ch := make(chan bool, 1)

	kv.mu.Lock()
	if args.Seq <= kv.clients[args.UID].Seq {
		reply.Err = OK
		reply.Value = kv.clients[args.UID].Value
		kv.mu.Unlock()
		return
	}
	kv.logs[index] = ch
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.logs, index)
		kv.mu.Unlock()
	}()

	op := Op{"Get", args.Key, "", args.UID, args.Seq, index}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// return to the client when:
	//  op is sent to ch,
	//  or timeout (if there are failures? ex: when the client initially contacted the leader,
	//   but someone else has since been elected, and the client request you put in the log has been discarded)
	select {
	case <-ch:
		kv.mu.Lock()
		val, ok := kv.db[args.Key]
		kv.mu.Unlock()
		if !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Value = val
			reply.Err = OK
		}
	case <-time.Tick(time.Millisecond * 100):
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	index := int(nrand())
	ch := make(chan bool, 1)

	kv.mu.Lock()
	if args.Seq <= kv.clients[args.UID].Seq {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.logs[index] = ch
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.logs, index)
		kv.mu.Unlock()
	}()
	op := Op{args.Op, args.Key, args.Value, args.UID, args.Seq, index}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	select {
	case <-ch:
		reply.Err = OK
	case <-time.Tick(time.Millisecond * 100):
		reply.Err = ErrWrongLeader
	}
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
	kv.clients = make(map[int]RequestInfo)
	kv.persister = persister

	kv.readPersist(kv.persister.ReadSnapshot())

	go kv.applier()

	return kv
}

func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		if m.SnapshotValid {
			kv.mu.Lock()
			ok := kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot)
			kv.mu.Unlock()
			if ok {
				kv.readPersist(m.Snapshot)
			}
		} else if m.CommandValid {
			op := m.Command.(Op)

			kv.mu.Lock()
			lastOp := kv.clients[op.ClientID]
			if op.Seq <= lastOp.Seq {
				kv.mu.Unlock()
				continue
			}
			if op.Type == "Put" {
				kv.db[op.Key] = op.Value
			} else if op.Type == "Append" {
				kv.db[op.Key] += op.Value
			}
			// update the seq # and value in the client's table entry
			kv.clients[op.ClientID] = RequestInfo{op.Seq, kv.db[op.Key]}

			// save a snapshot when Raft state size is approaching maxraftstate
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				kv.persist(m.CommandIndex)
			}
			// wake up the waiting RPC handler (if any)
			if _, ok := kv.logs[op.Index]; ok {
				kv.logs[op.Index] <- true
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var clients map[int]RequestInfo
	if d.Decode(&db) != nil || d.Decode(&clients) != nil {
		log.Fatalf("decode error")
	}
	kv.mu.Lock()
	kv.clients = clients
	kv.db = db
	kv.mu.Unlock()
}

func (kv *KVServer) persist(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.clients)
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}
