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

	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
	"github.com/sirupsen/logrus"
)

var logger = &logrus.Logger{
	Out:       os.Stderr,
	Formatter: new(logrus.TextFormatter),
	Hooks:     make(logrus.LevelHooks),
	Level:     logrus.InfoLevel,
}

// ApplyMsg type
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

// Raft type
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

	// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	currentTerm int
	// candidateId that received vote in current term (or null if none)
	votedFor *int
	// log entries; first index is 1
	log []LogEntry

	// index of highest log entry known to be committed
	// (initialized to 0, increases monotonically)
	commitIndex int
	// index of highest log entry applied to state machine
	// (initialized to 0, increases monotonically)
	lastApplied int

	// for each server, index of the next log entry to send to that server
	// (initialized to leader last log index + 1)
	// reinitialized after election
	nextIndex []int
	// for each server, index of highest log entry known to be replicated on server
	// (initialized to 0, increases monotonically)
	// reinitialized after election
	matchIndex []int

	// at any given time each server is in one of three states: leader, follower, or candidate
	state State
	// signal to start election timer
	// receives signal when converting to follower and starting an election.
	startElectionTimerCh chan bool
	// contains at most 1 current election
	electionCh chan chan bool
}

// type LogEntries []LogEntry

// State type
type State int

const (
	// FOLLOWER is a server's type
	FOLLOWER State = iota
	// CANDIDATE is a server's type
	CANDIDATE
	// LEADER is a server's type
	LEADER
)

// LogEntry contains command for state machine, and term when entry was received by leader
type LogEntry struct {
	Term    int
	Command interface{}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.isLeader()
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

	// candidate’s term
	Term int
	// candidate requesting vote
	CandidateID int
	// index of candidate’s last log entry
	LastLogIndex int
	// term of candidate’s last log entry
	LastLogTerm int
}

// RequestVoteReply type
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	// currentTerm, for candidate to update itself
	Term int
	// true means candidate received vote
	VoteGranted bool
}

// Set func uses to set values for reply obj.
func (reply *RequestVoteReply) Set(term int, voteGranted bool) {
	reply.Term = term
	reply.VoteGranted = voteGranted
}

// AppendEntriesArgs type
type AppendEntriesArgs struct {
	// leader’s term
	Term int
	// so follower can redirect clients
	LeaderID int
	// index of log entry immediately preceding new ones
	PrevLogIndex int
	// term of prevLogIndex entry
	PrevLogTerm int
	// log entries to store (empty for heartbeat; may send more than one for efficiency)
	Entries []LogEntry
	// leader’s commitIndex
	LeaderCommit int
}

// AppendEntriesReply type
type AppendEntriesReply struct {
	// currentTerm, for leader to update itself
	Term int
	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool
}

// Set func uses to set values for reply obj.
func (reply *AppendEntriesReply) Set(term int, success bool) {
	reply.Term = term
	reply.Success = success
}

// RequestVote func invoked by candidates to gather votes
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Set(rf.currentTerm, false)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertToFollower()
	}

	if rf.votedFor == nil || *rf.votedFor == args.CandidateID {
		// check candidate’s log is at least as up-to-date as receiver’s log:
		// candidate has higher term in last log entry, or
		// candidate has same last term and same length or longer log
		lastLog := rf.getLastLogEntry()
		lastLogIndex := rf.getLastLogIndex()
		if args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLogIndex) {
			// grant vote
			reply.Set(rf.currentTerm, true)
			ID := args.CandidateID
			rf.votedFor = &ID
		} else {
			reply.Set(rf.currentTerm, false)
		}
	} else {
		reply.Set(rf.currentTerm, false)
	}
}

func (rf *Raft) getLastLogEntry() LogEntry {
	return rf.log[rf.getLastLogIndex()]
}
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

// AppendEntries func invoked by leader to replicate log entries; also used as heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Set(rf.currentTerm, false)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	// If AppendEntries RPC received from new leader: convert to follower
	rf.convertToFollower()

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex > rf.getLastLogIndex() || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Set(rf.currentTerm, false)
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	startNewIndex := args.PrevLogIndex + 1
	index := 0
	for ; index+startNewIndex <= rf.getLastLogIndex() && index < len(args.Entries); index++ {
		if rf.log[index+startNewIndex].Term != args.Entries[index].Term {
			rf.log = rf.log[:index+startNewIndex]
			break
		}
	}

	// Append any new entries not already in the log
	rf.log = append(rf.log, args.Entries[index:]...)
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		logger.Debug("commitIndex", rf.commitIndex, rf.me)
	}

	reply.Set(rf.currentTerm, true)
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	// Your code here (2B).
	// The leader appends the command to its log as a new entry,
	// then issues AppendEntries RPCs in parallel to each of the other servers to replicate the entry.
	// When the entry has been safely replicated (as described below),
	// the leader applies the entry to its state machine and returns the result of that execution to the client.
	// If followers crash or run slowly, or if network packets are lost,
	// the leader retries AppendEntries RPCs indefinitely (even after it has responded to the client)
	// until all followers eventually store all log entries.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader() {
		return -1, -1, false
	}

	newEntry := LogEntry{rf.currentTerm, command}
	rf.log = append(rf.log, newEntry)

	logger.Debug("start", rf.me, rf.log)

	return rf.getLastLogIndex(), rf.currentTerm, rf.isLeader()
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

		<-rf.startElectionTimerCh

		tick(200, 300)

		if len(rf.startElectionTimerCh) > 0 {
			continue
		}

		rf.mu.Lock()
		if rf.state == FOLLOWER {
			rf.state = CANDIDATE
		}
		if rf.state == CANDIDATE {
			go rf.startElection()
		}
		rf.mu.Unlock()
	}
}

func tick(min, max int) {
	rand.Seed(time.Now().UnixNano())
	d := time.Duration(rand.Intn(max-min+1) + min)
	time.Sleep(time.Millisecond * d)
}

func (rf *Raft) endCurrentElection() {
	if len(rf.electionCh) > 0 {
		<-rf.electionCh <- true
	}
}

func (rf *Raft) startElectionTimer() {
	if len(rf.startElectionTimerCh) == 0 {
		rf.startElectionTimerCh <- true
	}
}

func (rf *Raft) startElection() {
	// stop current election, if exists, before start a new one.
	rf.endCurrentElection()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	timeoutCh := make(chan bool)
	rf.electionCh <- timeoutCh

	rf.currentTerm++
	rf.votedFor = &rf.me

	logger.Debug("startElection", rf.me)

	rf.startElectionTimer()

	replyCh := rf.broadcastRequestVote()
	go rf.processRequestVoteReply(replyCh, timeoutCh)
}

func (rf *Raft) broadcastRequestVote() chan RequestVoteReply {
	replyCh := make(chan RequestVoteReply, len(rf.peers))

	for target := range rf.peers {
		if target == rf.me {
			continue
		}
		lastLog := rf.getLastLogEntry()
		lastLogIndex := rf.getLastLogIndex()
		go func(target, currentTerm int) {
			args := RequestVoteArgs{currentTerm, rf.me, lastLogIndex, lastLog.Term}
			reply := RequestVoteReply{}
			if rf.sendRequestVote(target, &args, &reply) {
				logger.Debug("sendRequestVote", rf.me, target, args, reply)
				replyCh <- reply
			}
		}(target, rf.currentTerm)
	}
	return replyCh
}

// fromLogIndex: send new log entries to followers, in [fromLogIndex:toLogIntdex] (inclusive)
func (rf *Raft) broadcastAppendEntries() {
	for target := range rf.peers {
		if target == rf.me {
			continue
		}
		// If last log index ≥ nextIndex for a follower:
		// send AppendEntries RPC with log entries starting at nextIndex
		nextIndex := rf.nextIndex[target]
		lastLogIndex := rf.getLastLogIndex()
		args := AppendEntriesArgs{rf.currentTerm, rf.me, nextIndex - 1, rf.log[nextIndex-1].Term,
			rf.log[nextIndex : lastLogIndex+1], rf.commitIndex}
		reply := AppendEntriesReply{}
		go func(target int) {
			if rf.sendAppendEntries(target, &args, &reply) {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.convertToFollower()
				}

				// If successful: update nextIndex and matchIndex for follower
				if reply.Success {
					rf.matchIndex[target] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[target] = rf.matchIndex[target] + 1
				} else {
					// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
					rf.nextIndex[target]--

					rf.mu.Unlock()
					return
				}
				// If there exists an N such that N > commitIndex, a majority
				// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
				logger.Debug("matchIndex", rf.matchIndex, rf.commitIndex, args, rf.currentTerm)
				for N := rf.commitIndex + 1; N < len(rf.log); N++ {
					count := 1
					for _, index := range rf.matchIndex {
						if index >= N {
							count++
						}
					}
					if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
						logger.Debug("commitindex=", N, rf.matchIndex)
						rf.commitIndex = N
						break
					}
				}
				rf.mu.Unlock()
			}
		}(target)
	}
}
func (rf *Raft) isLeader() bool {
	return rf.state == LEADER
}
func (rf *Raft) convertToFollower() {
	rf.state = FOLLOWER
	rf.votedFor = nil
	rf.startElectionTimer()
}

func (rf *Raft) convertToLeader() {
	rf.state = LEADER
	logger.Debug("LEADER", rf.me)
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))

	go rf.startAppendEntriesLoop()
}

func (rf *Raft) startAppendEntriesLoop() {
	for {
		rf.mu.Lock()
		if !rf.isLeader() {
			rf.mu.Unlock()
			return
		}
		rf.broadcastAppendEntries()
		rf.mu.Unlock()

		tick(50, 100)
	}
}

func (rf *Raft) processRequestVoteReply(replyCh chan RequestVoteReply, timeoutCh chan bool) {
	defer func() {
		<-rf.electionCh
	}()
	voteCount := int32(1)
	for {
		select {
		case reply := <-replyCh:
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.convertToFollower()
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted {
				atomic.AddInt32(&voteCount, 1)
			}
			if int(atomic.LoadInt32(&voteCount)) > len(rf.peers)/2 {
				rf.convertToLeader()
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		case <-timeoutCh:
			logger.Debug("timeout", rf.me)
			return
		}
	}
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

	rf.startElectionTimerCh = make(chan bool, 1)
	rf.startElectionTimer()

	rf.electionCh = make(chan chan bool, 1)

	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.startApplyLogLoop(applyCh)
	return rf
}

func (rf *Raft) startApplyLogLoop(applyCh chan ApplyMsg) {
	for {
		rf.mu.Lock()
		logger.Debug(rf.log, rf.me)
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++

			msg := ApplyMsg{}
			msg.Command = rf.log[rf.lastApplied].Command
			msg.CommandIndex = rf.lastApplied
			msg.CommandValid = true

			applyCh <- msg

			logger.Debug("apply", rf.lastApplied, rf.me)
		}
		rf.mu.Unlock()

		tick(10, 20)
	}
}
