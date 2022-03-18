package kvraft

import (
	"crypto/rand"
	"math/big"
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
	currentSeqNum int
	// remember which server turned out to be the leader for the last RPC, and send the next RPC to that server first
	currentLeader int
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
	args := GetArgs{key}
	reply := GetReply{}
	for i := ck.currentLeader; ; i = (i + 1) % len(ck.servers) {
		if ok := ck.servers[i].Call("KVServer.Get", &args, &reply); ok {
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
	args := PutAppendArgs{key, value, op}
	reply := PutAppendReply{}
	for i := ck.currentLeader; ; i = (i + 1) % len(ck.servers) {
		if ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply); ok {
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
