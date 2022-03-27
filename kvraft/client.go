package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	//  unique identifier for each client
	uid int64
	// current sequence number (increase monotonically)
	// If a client re-sends a request, it re-uses the same sequence number.
	currentSeqNum int64
	// remember which server turned out to be the leader for the last RPC,
	// and send the next RPC to that server first
	currentLeader int
	mu            sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.uid = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{key, int(ck.uid), int(atomic.AddInt64(&ck.currentSeqNum, 1))}
	reply := GetReply{}
	for i := ck.currentLeader; ; i = (i + 1) % len(ck.servers) {
		if ck.servers[i].Call("KVServer.Get", &args, &reply) {
			if reply.Err != ErrWrongLeader {
				ck.currentLeader = i
				return reply.Value
			}
		}
		time.Sleep(time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{key, value, op, int(ck.uid), int(atomic.AddInt64(&ck.currentSeqNum, 1))}
	reply := PutAppendReply{}
	for i := ck.currentLeader; ; i = (i + 1) % len(ck.servers) {
		if ck.servers[i].Call("KVServer.PutAppend", &args, &reply) {
			if reply.Err != ErrWrongLeader {
				ck.currentLeader = i
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
