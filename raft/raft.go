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
	"bytes"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"github.com/sirupsen/logrus"
)

var logger = &logrus.Logger{
	Out: os.Stderr,
	Formatter: &logrus.TextFormatter{
		// DisableColors: true,
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339Nano,
	},
	Hooks: make(logrus.LevelHooks),
	Level: logrus.InfoLevel,
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
	CurrentTerm int
	// candidateId that received vote in current term (or null if none)
	VotedFor int
	// log entries; first index is 1
	Log []LogEntry

	// the index of the last entry in the log that the snapshot replaces
	// (the last entry the state machine had applied)
	LastIncludedIndex int
	// the term of LastIncludedIndex
	LastIncludedTerm int
	applyCh          chan ApplyMsg

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
	// number of servers
	n                   int
	heardFromLeaderChan chan bool
}

func (rf *Raft) getLastLogEntry() LogEntry {
	return rf.logAt(rf.getLastLogIndex())
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.Log) + rf.LastIncludedIndex - 1
}

func (rf *Raft) logAt(index int) LogEntry {
	return rf.Log[index-rf.LastIncludedIndex]
}

func (rf *Raft) logFrom(index int) []LogEntry {
	return rf.subLog(index, rf.getLastLogIndex())
}

func (rf *Raft) logTo(index int) []LogEntry {
	return rf.subLog(rf.LastIncludedIndex, index)
}

func (rf *Raft) subLog(from, to int) []LogEntry {
	return rf.Log[from-rf.LastIncludedIndex : to-rf.LastIncludedIndex+1]
}

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

// GetState return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.CurrentTerm, rf.isLeader()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.Log)
	e.Encode(rf.VotedFor)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []LogEntry
	var votedFor, currentTerm int
	var lastIncludedIndex, lastIncludedTerm int
	if d.Decode(&log) != nil || d.Decode(&votedFor) != nil || d.Decode(&currentTerm) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		logger.Fatalln("error decode data")
	}
	rf.Log = log
	rf.CurrentTerm = currentTerm
	rf.VotedFor = votedFor
	rf.LastIncludedIndex = lastIncludedIndex
	rf.LastIncludedTerm = lastIncludedTerm
}

// CondInstallSnapshot func
// The service reads from applyCh, and invokes CondInstallSnapshot with the snapshot to tell Raft that
// the service is switching to the passed-in snapshot state, and that Raft should update its log at the same time.
//
// Only switch to snapshot if Raft hasn't have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2D).

	// refuse to install a snapshot if it is an old snapshot:
	// Raft has processed entries after the snapshot's lastIncludedTerm/lastIncludedIndex
	if lastIncludedIndex <= rf.lastApplied {
		return false
	}
	if lastIncludedIndex >= rf.getLastLogIndex() {
		rf.Log = []LogEntry{{}}
	} else {
		rf.Log = append([]LogEntry{rf.Log[0]}, rf.logFrom(lastIncludedIndex+1)...)
	}
	rf.LastIncludedIndex = lastIncludedIndex
	rf.LastIncludedTerm = lastIncludedTerm

	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex

	rf.persistStateAndSnapshot(snapshot)

	return true
}

// Snapshot func
//
// the service says it has created a snapshot that has all info up to and including index.
// this means the service no longer needs the log through (and including) that index.
// Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.LastIncludedTerm = rf.logAt(index).Term
	rf.Log = append([]LogEntry{rf.Log[0]}, rf.logFrom(index+1)...)
	rf.LastIncludedIndex = index

	rf.persistStateAndSnapshot(snapshot)
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.Log)
	e.Encode(rf.VotedFor)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

// RequestVoteArgs type
// RequestVote RPC arguments structure.
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
// RequestVote RPC reply structure.
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
// AppendEntries RPC arguments structure
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
// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	// currentTerm, for leader to update itself
	Term int
	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool

	// additional information to reduce the number of rejected AppendEntries RPCs
	// to back up quickly

	// term in the conflicting entry (0 if not exists)
	XTerm int
	//  index of first entry with that term (0 if not exists)
	XIndex int
	// log length
	XLen int
}

// Set func uses to set values for reply obj.
func (reply *AppendEntriesReply) Set(term int, success bool) {
	reply.Term = term
	reply.Success = success
}

// InstallSnapshotArgs type
// InstallSnapshot RPC arguments structure
type InstallSnapshotArgs struct {
	// leader’s term
	Term int
	// so follower can redirect clients
	LeaderID int
	// the snapshot replaces all entries up through and including this index
	LastIncludedIndex int
	// term of lastIncludedIndex
	LastIncludedTerm int
	// byte Offset where chunk is positioned in the snapshot file
	Offset int
	// raw bytes of the snapshot chunk, starting at offset
	Data []byte
	// if this is the last chunk
	Done bool
}

// InstallSnapshotReply type
// InstallSnapshot RPC reply structure
type InstallSnapshotReply struct {
	// currentTerm, for leader to update itself
	Term int
}

// RequestVote is a RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.Set(rf.CurrentTerm, false)
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.convertToFollower()
		rf.persist()
	}

	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {
		// check candidate’s log is at least as up-to-date as receiver’s log:
		// candidate has higher term in last log entry, or
		// candidate has same last term and same length or longer log
		lastLog := rf.getLastLogEntry()
		lastLogIndex := rf.getLastLogIndex()
		if args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLogIndex) {
			// grant vote
			reply.Set(rf.CurrentTerm, true)
			rf.VotedFor = args.CandidateID
			rf.persist()
			if len(rf.heardFromLeaderChan) == 0 {
				rf.heardFromLeaderChan <- true
			}
			return
		}
	}
	reply.Set(rf.CurrentTerm, false)
}

// AppendEntries is a RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.Set(rf.CurrentTerm, false)
		reply.XLen = 1
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.convertToFollower()
		rf.persist()
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > rf.getLastLogIndex() || args.PrevLogIndex < rf.LastIncludedIndex {
		reply.XLen = rf.getLastLogIndex() + 1
		reply.Set(rf.CurrentTerm, false)
		return
	}
	prevLogTerm := rf.logAt(args.PrevLogIndex).Term
	if args.PrevLogIndex == rf.LastIncludedIndex {
		prevLogTerm = rf.LastIncludedTerm
	}
	if prevLogTerm != args.PrevLogTerm {
		reply.XTerm = prevLogTerm

		// find index of first entry with reply.XTerm
		xIndex := args.PrevLogIndex
		for i := args.PrevLogIndex - 1; i > rf.LastIncludedIndex; i-- {
			if reply.XTerm == rf.logAt(i).Term {
				xIndex = i
			}
			break
		}
		reply.XIndex = xIndex
		reply.Set(rf.CurrentTerm, false)
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	startNewIndex := args.PrevLogIndex + 1
	index := 0
	for ; index+startNewIndex <= rf.getLastLogIndex() && index < len(args.Entries); index++ {
		if rf.logAt(index+startNewIndex).Term != args.Entries[index].Term {
			rf.Log = rf.logTo(index + startNewIndex - 1)
			break
		}
	}

	// Append any new entries not already in the log
	rf.Log = append(rf.Log, args.Entries[index:]...)
	// rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
	}

	// If AppendEntries RPC received from new leader: convert to follower
	rf.convertToFollowerNoResetVote()
	if len(rf.heardFromLeaderChan) == 0 {
		rf.heardFromLeaderChan <- true
	}
	rf.persist()
	reply.Set(rf.CurrentTerm, true)
}

// InstallSnapshot is a RPC handler
// The leader uses a new RPC called InstallSnapshot to send snapshots to followers that are too far behind; see Figure 13.
// When a follower receives a snapshot with this RPC, it must decide what to do with its existing log en- tries.
// Usually the snapshot will contain new information not already in the recipient’s log.
// In this case, the follower discards its entire log; it is all superseded by the snapshot
// and may possibly have uncommitted entries that conflict with the snapshot.
// If instead the follower receives a snap- shot that describes a prefix of its log (due to retransmis- sion or by mistake),
//  then log entries covered by the snap- shot are deleted but entries following the snapshot are still valid and must be retained.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.convertToFollower()
		rf.persist()
	}

	msg := ApplyMsg{}
	msg.CommandValid = false

	msg.SnapshotValid = true
	msg.Snapshot = clone(args.Data)
	msg.SnapshotIndex = args.LastIncludedIndex
	msg.SnapshotTerm = args.LastIncludedTerm

	rf.applyCh <- msg

	rf.convertToFollowerNoResetVote()
	if len(rf.heardFromLeaderChan) == 0 {
		rf.heardFromLeaderChan <- true
	}
	reply.Term = rf.CurrentTerm
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
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
// invoked by candidates to gather votes
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// invoked by leader to replicate log entries; also used as heartbeat
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// invoked by leader to send chunks of a snapshot to a follower. Leaders always send chunks in order
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader() {
		return -1, -1, false
	}

	newEntry := LogEntry{rf.CurrentTerm, command}
	rf.Log = append(rf.Log, newEntry)
	rf.persist()

	return rf.getLastLogIndex(), rf.CurrentTerm, rf.isLeader()
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

// The ticker go routine starts a new election if this peer hasn't received heartsbeats recently.
func (rf *Raft) ticker() {
	for {
		if rf.killed() == false {
			// Your code here to check if a leader election should
			// be started and to randomize sleeping time using
			// time.Sleep().

			<-rf.startElectionTimerCh

			tick(200, 300)

			rf.endCurrentElection()

			if len(rf.heardFromLeaderChan) > 0 {
				<-rf.heardFromLeaderChan
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
		} else {
			time.Sleep(time.Millisecond * 3)
		}
	}
}

func tick(min, max int) {
	rand.Seed(time.Now().UnixNano())
	d := time.Duration(rand.Intn(max-min+1) + min)
	time.Sleep(time.Millisecond * d)
}

func (rf *Raft) endCurrentElection() {
	if len(rf.electionCh) > 0 {
		timeoutCh := <-rf.electionCh
		timeoutCh <- true
		rf.electionCh <- timeoutCh
	}
}

func (rf *Raft) startElectionTimer() {
	if len(rf.startElectionTimerCh) == 0 {
		rf.startElectionTimerCh <- true
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	timeoutCh := make(chan bool)
	rf.electionCh <- timeoutCh

	rf.CurrentTerm++
	// vote for self
	if rf.VotedFor == -1 || rf.VotedFor != rf.me {
		rf.VotedFor = rf.me
		rf.persist()
	}

	// reset election timer
	rf.startElectionTimer()

	replyCh := rf.broadcastRequestVote()
	go rf.processRequestVoteReply(replyCh, timeoutCh)
}

func (rf *Raft) broadcastRequestVote() chan RequestVoteReply {
	replyCh := make(chan RequestVoteReply, rf.n)

	for target := 0; target < rf.n; target++ {
		if target == rf.me {
			continue
		}
		lastLog := rf.getLastLogEntry()
		lastLogIndex := rf.getLastLogIndex()
		go func(target, currentTerm int) {
			args := RequestVoteArgs{currentTerm, rf.me, lastLogIndex, lastLog.Term}
			reply := RequestVoteReply{}
			if rf.sendRequestVote(target, &args, &reply) {
				replyCh <- reply
			}
		}(target, rf.CurrentTerm)
	}
	return replyCh
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
			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.convertToFollower()
				rf.persist()

				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted && int(atomic.AddInt32(&voteCount, 1)) > rf.n/2 {
				rf.convertToLeader()

				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		case <-timeoutCh:
			return
		}
	}
}

func (rf *Raft) isLeader() bool {
	return rf.state == LEADER
}

func (rf *Raft) convertToFollower() {
	rf.state = FOLLOWER
	rf.VotedFor = -1
	rf.startElectionTimer()
}
func (rf *Raft) convertToFollowerNoResetVote() {
	rf.state = FOLLOWER
	rf.startElectionTimer()
}
func (rf *Raft) convertToLeader() {
	rf.state = LEADER

	// reinitialized after election
	if rf.nextIndex == nil {
		rf.nextIndex = make([]int, rf.n)
		for i := 0; i < rf.n; i++ {
			rf.nextIndex[i] = max(rf.getLastLogIndex()+1, 1)
		}
		rf.matchIndex = make([]int, rf.n)
	}
	go rf.startSendAppendEntriesLoop()
}

func (rf *Raft) startSendAppendEntriesLoop() {
	i := 0
	for {
		rf.mu.Lock()
		if !rf.isLeader() {
			rf.mu.Unlock()
			return
		}
		if i == 6 {
			rf.broadcastAppendEntries(true)
			i = 0
		} else {
			rf.broadcastAppendEntries(false)
		}
		rf.mu.Unlock()
		i++

		tick(20, 20)
	}
}

func (rf *Raft) broadcastAppendEntries(isHeartbeat bool) {
	for target := 0; target < rf.n; target++ {
		if target == rf.me {
			continue
		}
		// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
		// else log entries is empty, used as hearbeat.
		nextIndex := rf.nextIndex[target]
		lastLogIndex := rf.getLastLogIndex()

		if nextIndex <= rf.LastIncludedIndex {
			args := InstallSnapshotArgs{rf.CurrentTerm, rf.me, rf.LastIncludedIndex, rf.LastIncludedTerm, 0, rf.persister.ReadSnapshot(), true}
			go func(target int, args InstallSnapshotArgs) {
				reply := InstallSnapshotReply{}
				if rf.sendInstallSnapshot(target, &args, &reply) {
					rf.mu.Lock()
					if reply.Term > rf.CurrentTerm {
						rf.CurrentTerm = reply.Term
						rf.convertToFollower()

						rf.persist()
						rf.mu.Unlock()
						return
					}
					rf.matchIndex[target] = args.LastIncludedIndex
					rf.nextIndex[target] = rf.matchIndex[target] + 1

					rf.mu.Unlock()
				}
			}(target, args)
			continue
		}
		var entries []LogEntry
		var prevLogIndex, prevLogTerm int

		if lastLogIndex >= nextIndex {
			entries = append(entries, rf.subLog(nextIndex, lastLogIndex)...)
			prevLogIndex = nextIndex - 1
		} else {
			prevLogIndex = lastLogIndex
		}
		if prevLogIndex == rf.LastIncludedIndex {
			prevLogTerm = rf.LastIncludedTerm
		} else {
			prevLogTerm = rf.logAt(prevLogIndex).Term
		}
		args := AppendEntriesArgs{rf.CurrentTerm, rf.me, prevLogIndex, prevLogTerm,
			entries, rf.commitIndex}
		if !isHeartbeat && len(entries) == 0 {
			continue
		}
		go func(target int, args AppendEntriesArgs) {
			reply := AppendEntriesReply{}

			if rf.sendAppendEntries(target, &args, &reply) {
				rf.mu.Lock()

				if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					rf.convertToFollower()
					rf.persist()
				}
				if reply.Term < rf.CurrentTerm {
					rf.mu.Unlock()
					return
				}
				if reply.Success {
					rf.matchIndex[target] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[target] = rf.matchIndex[target] + 1
				} else {
					// roll back quickly, instead of
					// rf.nextIndex[target]--

					// follower's log is too short
					if reply.XTerm == 0 {
						rf.nextIndex[target] = reply.XLen
						rf.mu.Unlock()
						return
					}
					// find leader's last entry for XTerm
					xTermIndex := -1
					for i := args.PrevLogIndex; i > rf.LastIncludedIndex; i-- {
						if rf.logAt(i).Term < reply.XTerm {
							break
						}
						if rf.logAt(i).Term == reply.XTerm {
							xTermIndex = i
							break
						}
					}

					if xTermIndex != -1 { // leader has XTerm
						rf.nextIndex[target] = xTermIndex + 1
					} else { // leader doesn't have XTerm
						rf.nextIndex[target] = reply.XIndex
					}

					rf.mu.Unlock()
					return
				}

				// If there exists an N such that N > commitIndex, a majority
				// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
				for N := rf.getLastLogIndex(); N > rf.commitIndex && N >= rf.LastIncludedIndex; N-- {
					count := 1
					for _, index := range rf.matchIndex {
						if index >= N {
							count++
						}
					}
					if count > rf.n/2 && rf.logAt(N).Term == rf.CurrentTerm {
						rf.commitIndex = N
						break
					}
				}
				rf.mu.Unlock()
			}
		}(target, args)
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

	rf.n = len(rf.peers)
	rf.state = FOLLOWER

	rf.startElectionTimerCh = make(chan bool, 1)
	rf.startElectionTimer()

	rf.heardFromLeaderChan = make(chan bool, 1)

	rf.electionCh = make(chan chan bool, 1)

	rf.Log = make([]LogEntry, 1)
	rf.Log[0] = LogEntry{}

	rf.VotedFor = -1

	rf.LastIncludedIndex = 0
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if rf.LastIncludedIndex > 0 {
		rf.lastApplied = rf.LastIncludedIndex
	}
	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.startApplyLogLoop(applyCh)
	return rf
}

func (rf *Raft) startApplyLogLoop(applyCh chan ApplyMsg) {
	for {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied && rf.lastApplied >= rf.LastIncludedIndex {
			rf.lastApplied++

			msg := ApplyMsg{}
			msg.Command = rf.logAt(rf.lastApplied).Command
			msg.CommandIndex = rf.lastApplied
			msg.CommandValid = true

			rf.mu.Unlock()

			applyCh <- msg
			continue
		}
		rf.mu.Unlock()

		tick(1, 2)
	}
}
