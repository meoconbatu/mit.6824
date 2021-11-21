package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    *int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	// my state
	state         State
	startTickerCh chan bool
	electionCh    chan chan bool

	startHeatbeatCh chan bool
}

// State type: at any given time each server is in one of three states: leader, follower, or candidate
type State int

const (
	// FOLLOWER is a server's type
	FOLLOWER State = iota
	// CANDIDATE is a server's type
	CANDIDATE
	// LEADER is a server's type
	LEADER
)

// LogEntry log entries; each entry contains command for state machine,
// and term when entry was received by leader (first index is 1)
type LogEntry struct {
	term    int
	command interface{}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == LEADER {
		isleader = true
	}
	term = rf.currentTerm
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// CondInstallSnapshot func
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs type
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply type
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs type
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	entries      []LogEntry
	leaderCommit int
}

// AppendEntriesReply type
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RequestVote func
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.checkRequestVoteArgs(*args) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if rf.votedFor == nil || *rf.votedFor == args.CandidateID {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm

		ID := args.CandidateID
		rf.votedFor = &ID
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start func
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Kill func
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.startTickerCh:
			tick(150, 300)
			rf.endCurrentElection()
		case <-rf.startHeatbeatCh:
			tick(50, 120)
		}

		rf.mu.Lock()
		switch rf.state {
		case FOLLOWER:
			rf.state = CANDIDATE
			// fmt.Println("FOLLOWER")
			go rf.startElection()
		case CANDIDATE:
			// fmt.Println("CANDIDATE")
			go rf.startElection()
		case LEADER:
			rf.sendHeartbeat()
			rf.startHeatbeatCh <- true
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) endCurrentElection() {
	if len(rf.electionCh) > 0 {
		<-rf.electionCh <- true
	}
	for len(rf.electionCh) == 0 {
		return
	}
}
func (rf *Raft) startTicker() {
	if len(rf.startTickerCh) == 0 {
		// fmt.Println("startTicker")
		rf.startTickerCh <- true
	}
}
func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	timeoutCh := make(chan bool)
	rf.electionCh <- timeoutCh
	rf.currentTerm++
	rf.votedFor = &rf.me

	rf.startTicker()

	replyCh := rf.broadcastRequestVote()
	go func() {
		voteCount := int32(1)
		for {
			select {
			case reply := <-replyCh:
				rf.mu.Lock()
				if !rf.checkRequestVoteReply(reply) {
					rf.mu.Unlock()
					return
				}
				if reply.VoteGranted {
					atomic.AddInt32(&voteCount, 1)
				}
				if int(atomic.LoadInt32(&voteCount)) > len(rf.peers)/2 {
					rf.convertToLeader()
					rf.mu.Unlock()
					fmt.Println("reply leader", rf.me)
					return
				}
				rf.mu.Unlock()
			case <-timeoutCh:
				return
			}
		}
	}()
}
func (rf *Raft) checkRequestVoteReply(reply RequestVoteReply) bool {
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.convertToFollower()
		return false
	}
	return true
}

func (rf *Raft) checkRequestVoteArgs(args RequestVoteArgs) bool {
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertToFollower()
		return false
	}
	return true
}
func (rf *Raft) convertToFollower() {
	rf.state = FOLLOWER
	rf.votedFor = nil

	rf.startTicker()
}
func (rf *Raft) convertToLeader() {
	rf.state = LEADER

	rf.sendHeartbeat()
	rf.startHeatbeatCh <- true
}
func (rf *Raft) sendHeartbeat() {
}
func (rf *Raft) broadcastRequestVote() chan RequestVoteReply {
	replyCh := make(chan RequestVoteReply, len(rf.peers))
	currentTime := rf.currentTerm
	for target := range rf.peers {
		if target == rf.me {
			continue
		}
		go func(target, currentTerm int) {
			args := RequestVoteArgs{currentTerm, rf.me, 0, 0}
			reply := RequestVoteReply{}
			if rf.sendRequestVote(target, &args, &reply) {
				replyCh <- reply
			}
		}(target, currentTime)
	}
	return replyCh
}
func tick(min, max int) {
	rand.Seed(time.Now().UnixNano())
	d := time.Duration(rand.Intn(max-min+1) + min)
	time.Sleep(time.Millisecond * d)
}

// Make func
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER

	rf.startTickerCh = make(chan bool, 1)
	rf.startTicker()

	rf.startHeatbeatCh = make(chan bool, 1)

	rf.electionCh = make(chan chan bool, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
